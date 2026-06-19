package raft

import (
	"sync"
	"time"
)

// Node is a single participant in the Raft cluster.
type Node struct {
	mu       sync.Mutex
	stopOnce sync.Once

	id    int
	peers []int // voting peers (not self); tracks active config union during joint phase

	transport Transport
	persister Persister
	applyCh   chan ApplyMsg

	// --- Persistent state ---
	currentTerm int
	votedFor    int
	log         []LogEntry

	// --- Volatile state on all servers ---
	state       ServerState
	commitIndex int
	lastApplied int

	// --- Volatile state on leaders only ---
	nextIndex  map[int]int
	matchIndex map[int]int

	// --- Snapshot bookkeeping ---
	lastIncludedIndex int
	lastIncludedTerm  int

	leaderID int

	// --- Election timing ---
	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}

	// -----------------------------------------------------------------------
	// Stage 7 — Dynamic membership (joint consensus, Raft §6)
	// -----------------------------------------------------------------------
	// Three membership states:
	//
	//   Stable  — only configStable active; peers = configStable.Members \ {self}
	//   Joint   — configOld + configNew both active (C_old,new);
	//             majority of BOTH required to commit anything
	//   Final   — C_new-only entry committed; back to Stable with configStable=configNew
	//
	// A node switches to a new config the instant it *appends* the config
	// log entry — not when it commits. This is the §6 rule that prevents
	// two overlapping majorities from simultaneously believing they're leader.

	configStable ClusterConfig // active stable config (C_old during a transition)
	configOld    ClusterConfig // C_old during joint phase (same as configStable entering)
	configNew    ClusterConfig // C_new during joint phase
	configJoint  bool          // true while in C_old,new joint phase

	// pendingConfigIndex is the log index of the in-flight config entry.
	// -1 means no config change in progress.
	pendingConfigIndex int
}

func NewNode(id int, peers []int, transport Transport, persister Persister, applyCh chan ApplyMsg) *Node {
	// Build the initial stable config from the provided peer list + self.
	members := make([]int, len(peers)+1)
	copy(members, peers)
	members[len(peers)] = id

	n := &Node{
		id:          id,
		peers:       peers,
		transport:   transport,
		persister:   persister,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0, Index: 0}},
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		leaderID:    -1,
		stopCh:      make(chan struct{}),

		configStable:       ClusterConfig{Members: dedupSorted(members)},
		configJoint:        false,
		pendingConfigIndex: -1,
	}
	n.restoreLocked()
	return n
}

func (n *Node) State() (ServerState, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

func (n *Node) ID() int { return n.id }

// Peers returns a snapshot of the current peer list (not including self).
func (n *Node) Peers() []int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]int{}, n.peers...)
}

// Config returns the current stable cluster config.
func (n *Node) Config() ClusterConfig {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.configStable
}

// CommitIndex returns the current commitIndex. Used by tests to wait
// for entries to be committed without access to internal state.
func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

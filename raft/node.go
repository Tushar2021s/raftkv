package raft

import (
	"sync"
	"time"
)

// Node is a single participant in the Raft cluster. Every field below
// is grouped to match exactly how the Raft paper's Figure 2 groups
// them, so it's easy to cross-reference.
type Node struct {
	mu       sync.Mutex
	stopOnce sync.Once

	id    int
	peers []int // every other node's ID; does not include this node's own id

	transport Transport
	persister Persister
	applyCh   chan ApplyMsg

	// --- Persistent state on all servers ---
	// Updated on stable storage before responding to RPCs (see Persister).
	currentTerm int
	votedFor    int // -1 means "haven't voted in this term yet"
	log         []LogEntry

	// --- Volatile state on all servers ---
	state       ServerState
	commitIndex int
	lastApplied int

	// --- Volatile state on leaders only, reinitialized after every election ---
	nextIndex  map[int]int // peerID -> index of the next log entry to send that peer
	matchIndex map[int]int // peerID -> highest log entry known to be replicated on that peer

	// --- Snapshot bookkeeping (filled in during the snapshotting stage) ---
	lastIncludedIndex int
	lastIncludedTerm  int

	// --- Election timing ---
	// electionResetAt is bumped every time we see a valid heartbeat or
	// vote for someone; the election timer goroutine checks against it
	// rather than using a single timer.Reset, which is easier to reason
	// about under a mutex.
	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}
}

// NewNode constructs a Raft node in the Follower state with a fresh
// (empty) log. id must be unique within peers+{id}. The dummy entry at
// log index 0 exists so that "PrevLogIndex == 0" is always valid to
// check against, without special-casing an empty log everywhere.
func NewNode(id int, peers []int, transport Transport, persister Persister, applyCh chan ApplyMsg) *Node {
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
		stopCh:      make(chan struct{}),
	}
	n.restoreLocked() // pick up any prior persisted state, if this isn't a fresh start
	return n
}

// State returns the node's current role and term. Safe for concurrent use.
func (n *Node) State() (ServerState, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

// ID returns this node's ID. It's immutable for the node's lifetime,
// so it's safe to call without holding the lock.
func (n *Node) ID() int {
	return n.id
}

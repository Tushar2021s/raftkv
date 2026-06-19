package raft

// ServerState represents the three roles a Raft node can be in at any
// moment. A node moves Follower -> Candidate -> Leader, and falls back
// to Follower the instant it sees a higher term than its own -- this
// is the entire state machine described in Figure 4 of the Raft paper
// (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm", 2014).
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is one entry in the replicated log. Command is intentionally
// opaque ([]byte) -- Raft's only job is to get every node to agree on
// the same sequence of commands in the same order. It has no idea what
// a command *means*; that interpretation belongs entirely to whatever
// state machine sits on top (our kvstore package, later).
type LogEntry struct {
	Term    int          `json:"term"`
	Index   int          `json:"index"`
	Command []byte       `json:"command"`
	Type    LogEntryType `json:"type,omitempty"` // 0 = EntryNormal (backward-compatible)
}

// ApplyMsg is what a Node hands to the application once a log entry
// has been committed by a majority and is safe to execute. Either a
// single committed command is delivered, or (once we reach the
// snapshotting stage) a full snapshot is delivered instead.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// ConfigChangeType distinguishes the two phases of joint consensus.
// A membership change always goes through both phases in order:
//  1. JointConfig  — both old and new majority must agree to commit
//  2. NewConfig    — new majority only; joint phase is complete
type ConfigChangeType int

const (
	JointConfig ConfigChangeType = iota
	NewConfig
)

// ConfigChange is the command payload for a membership change log
// entry. Raft carries it as opaque []byte just like any other command;
// the Node itself decodes it and updates its peer list accordingly.
type ConfigChange struct {
	Type     ConfigChangeType `json:"type"`
	OldPeers []int            `json:"oldPeers"` // meaningful only for JointConfig
	NewPeers []int            `json:"newPeers"`
}

// ClusterConfig holds the peer sets that determine quorum during and
// after a membership transition.
//
// In steady state (no change in progress): NewPeers is the full peer
// set, OldPeers is nil, Joint is false.
//
// During joint consensus: both OldPeers and NewPeers are populated,
// Joint is true. A log entry commits only when it has been replicated
// to a majority of BOTH sets — this is the invariant that prevents two
// simultaneous leaders from forming when the cluster is mid-transition.

// LogEntryType distinguishes normal application commands from cluster
// configuration entries. Config entries are interpreted by Raft itself
// (not by the application state machine) and take effect the moment
// they are appended to the log — not at commit time. That's the rule
// from §6 of the paper: a server adds a new config to its log and
// starts using it immediately, even before it's committed.
type LogEntryType int

const (
	EntryNormal LogEntryType = iota
	EntryConfig              // carries a ConfigChangeArgs payload
)

func init() {
	// Ensure zero-value LogEntry.Type is EntryNormal so existing log
	// entries (which predate stage 7) are treated as normal commands.
	_ = EntryNormal
}

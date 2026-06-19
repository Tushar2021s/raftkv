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
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command []byte `json:"command"`
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

package raft

// RequestVoteArgs is sent by a candidate to every peer when it starts
// an election (Raft paper, Figure 2).
type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntriesArgs serves double duty: when Entries is empty it's a
// heartbeat that just asserts leadership; when it's non-empty it's
// the mechanism for replicating the log.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`

	// ConflictIndex/ConflictTerm let a follower point the leader
	// straight at where the logs diverge instead of the leader
	// backing off one entry per RPC -- the "fast log backtracking"
	// optimization mentioned in the extended Raft paper. Without this,
	// recovering a follower that's far behind is O(n) RPCs; with it,
	// it's O(1).
	ConflictIndex int `json:"conflictIndex"`
	ConflictTerm  int `json:"conflictTerm"`
}

// InstallSnapshotArgs/Reply are wired into the Transport interface now
// so the contract is stable from day one, even though we don't
// implement snapshotting until later.
type InstallSnapshotArgs struct {
	Term              int    `json:"term"`
	LeaderID          int    `json:"leaderId"`
	LastIncludedIndex int    `json:"lastIncludedIndex"`
	LastIncludedTerm  int    `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

type InstallSnapshotReply struct {
	Term int `json:"term"`
}

// AddServerArgs/Reply and RemoveServerArgs/Reply are the client-facing
// RPC types for requesting membership changes. The leader translates
// these into the two-phase joint-consensus log entries internally.
type AddServerArgs struct {
	NewPeerID int `json:"newPeerId"`
}

type AddServerReply struct {
	Success bool   `json:"success"`
	LeaderID int   `json:"leaderId"` // set when Success=false so client can retry
	Err     string `json:"err,omitempty"`
}

type RemoveServerArgs struct {
	PeerID int `json:"peerId"`
}

type RemoveServerReply struct {
	Success  bool   `json:"success"`
	LeaderID int    `json:"leaderId"`
	Err      string `json:"err,omitempty"`
}

// -------------------------------------------------------------------------
// Stage 7 — Dynamic membership (joint consensus, Raft §6)
// -------------------------------------------------------------------------

// ClusterConfig describes a cluster membership configuration. During a
// membership change two configs are active simultaneously: C_old (the
// current members) and C_new (the target members). While the joint
// config C_old,new is active, a majority of BOTH sets must agree for
// anything to commit.
type ClusterConfig struct {
	// Members is the set of node IDs that form this configuration.
	Members []int `json:"members"`
}

// ConfigChangeArgs is the command encoded into a log entry when the
// leader initiates a membership change. It carries both the old and new
// configs so every node in the cluster can reconstruct the joint phase
// from the log alone, without any out-of-band signalling.
type ConfigChangeArgs struct {
	// IsJoint is true for the C_old,new entry and false for the final
	// C_new-only entry that completes the transition.
	IsJoint bool          `json:"isJoint"`
	Old     ClusterConfig `json:"old,omitempty"`
	New     ClusterConfig `json:"new"`
}

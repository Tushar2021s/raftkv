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

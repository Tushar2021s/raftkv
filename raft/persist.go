package raft

import "encoding/json"

// persistentState is the exact set of fields that must survive a
// crash (Raft paper Figure 2): currentTerm, votedFor, the log, and —
// added in stage 6 — the snapshot metadata. Without lastIncludedIndex
// and lastIncludedTerm, a restarted node can't correctly interpret
// the log entries that follow the compacted prefix.
type persistentState struct {
	CurrentTerm       int        `json:"currentTerm"`
	VotedFor          int        `json:"votedFor"`
	Log               []LogEntry `json:"log"`
	LastIncludedIndex int        `json:"lastIncludedIndex"`
	LastIncludedTerm  int        `json:"lastIncludedTerm"`
}

// persistLocked serialises all persistent state and writes it durably
// via the Persister. Must be called — and must complete — before
// responding to any RPC that changed persistent state.
func (n *Node) persistLocked() {
	data, err := json.Marshal(persistentState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Log:               n.log,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
	})
	if err != nil {
		panic(err)
	}
	_ = n.persister.SaveState(data)
}

// restoreLocked loads persisted state on startup. Called once from
// NewNode before the node is exposed to any other goroutine.
func (n *Node) restoreLocked() {
	data, err := n.persister.ReadState()
	if err != nil || len(data) == 0 {
		return
	}
	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log
	n.lastIncludedIndex = ps.LastIncludedIndex
	n.lastIncludedTerm = ps.LastIncludedTerm
	// commitIndex and lastApplied start at lastIncludedIndex after
	// recovery — the snapshot already represents everything up to that
	// point, and the applyLoop will deliver entries above it.
	n.commitIndex = ps.LastIncludedIndex
	n.lastApplied = ps.LastIncludedIndex
}

package raft

import "encoding/json"

// persistentState is the shape of everything that must survive a
// crash: currentTerm, votedFor, and the log itself (Raft paper Figure
// 2 -- these three fields are exactly what's labeled "Persistent
// state on all servers").
type persistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    int        `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// persistLocked must be called -- and complete -- before responding to
// any RPC that changed currentTerm, votedFor, or log. That ordering is
// the entire reason a crashed-and-restarted node can never violate
// safety: it never tells a peer something it hasn't durably committed
// to remembering first.
func (n *Node) persistLocked() {
	data, err := json.Marshal(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
	if err != nil {
		panic(err) // a marshal failure here would be a bug, not a runtime condition
	}
	// Handling a *failed disk write* is deliberately deferred to stage
	// 5, where persister stops being an in-memory stand-in. Right now
	// MemoryPersister.SaveState can't actually fail.
	_ = n.persister.SaveState(data)
}

// restoreLocked loads whatever the persister already had, if
// anything. Called once, at construction, before the node is exposed
// to any other goroutine.
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
}

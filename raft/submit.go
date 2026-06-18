package raft

import "errors"

// ErrNotLeader is returned by Submit when this node isn't the current
// leader. The caller (the KV store layer, later) uses this to redirect
// the client to whoever it thinks the actual leader is.
var ErrNotLeader = errors.New("raft: not the leader")

// Submit is the single entry point through which the application
// proposes a new command to the cluster. The leader appends it to its
// own log and immediately kicks off replication to all peers in
// parallel. The command is NOT safe to execute yet at this point --
// it only becomes safe once it's committed (i.e. a majority of nodes
// hold it and the applyLoop delivers it via applyCh).
//
// Returns the log index the command was assigned, the current term,
// and whether this node is actually the leader. If isLeader is false,
// the caller should redirect to a different node.
func (n *Node) Submit(command []byte) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, n.currentTerm, false
	}

	index = len(n.log)
	term = n.currentTerm
	n.log = append(n.log, LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})
	n.persistLocked()

	// Kick off replication immediately instead of waiting for the next
	// heartbeat tick -- this is what keeps write latency low under a
	// steady workload.
	go n.broadcastAppendEntries(term)

	return index, term, true
}

// LeaderID returns the ID of the node this node currently believes
// to be the leader (-1 if unknown). Followers track the leaderID from
// AppendEntries calls so they can redirect clients correctly.
func (n *Node) LeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

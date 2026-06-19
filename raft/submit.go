package raft

import "errors"

var ErrNotLeader = errors.New("raft: not the leader")

// Submit is the single entry point through which the application proposes
// a command. The leader assigns it a logical log index, appends it locally,
// persists, and immediately kicks off replication.
func (n *Node) Submit(command []byte) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, n.currentTerm, false
	}

	// The new entry's logical index is one past the current last.
	index = n.lastLogIndexLocked() + 1
	term = n.currentTerm
	n.log = append(n.log, LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})
	n.persistLocked()

	go n.broadcastAppendEntries(term)

	return index, term, true
}

// LeaderID returns the ID of the node this node believes is the leader.
// -1 means unknown. Used by followers to redirect clients.
func (n *Node) LeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

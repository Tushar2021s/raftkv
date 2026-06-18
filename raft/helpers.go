package raft

// lastLogIndexLocked returns the index of the last entry currently in
// the log. Caller must hold n.mu.
//
// Note: once snapshotting lands in stage 6, entries before
// lastIncludedIndex get discarded and this will need to account for
// that offset. For now log[0] is always the dummy sentinel at index 0,
// so this is simply len(n.log)-1.
func (n *Node) lastLogIndexLocked() int {
	return len(n.log) - 1
}

// lastLogTermLocked returns the term of the last entry in the log.
func (n *Node) lastLogTermLocked() int {
	return n.log[n.lastLogIndexLocked()].Term
}

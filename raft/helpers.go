package raft

// lastLogIndexLocked returns the index of the last entry currently in
// the log. Caller must hold n.mu.
func (n *Node) lastLogIndexLocked() int {
	return len(n.log) - 1
}

// lastLogTermLocked returns the term of the last entry in the log.
func (n *Node) lastLogTermLocked() int {
	return n.log[n.lastLogIndexLocked()].Term
}

// min returns the smaller of two ints. Defined here because Go 1.22's
// builtin min() only covers ordered types in generic contexts; this is
// simpler and avoids any version confusion.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

package raft

// lastLogIndexLocked returns the logical index of the last entry in
// the log. After log compaction the physical slice is shorter than the
// full logical history; lastIncludedIndex is the logical index of the
// sentinel at physical position 0.
// Caller must hold n.mu.
func (n *Node) lastLogIndexLocked() int {
	return n.lastIncludedIndex + len(n.log) - 1
}

// lastLogTermLocked returns the term of the last entry in the log.
func (n *Node) lastLogTermLocked() int {
	return n.log[len(n.log)-1].Term
}

// logicalToPhysical converts a logical log index to a physical slice
// index. Always call this instead of using a logical index directly
// into n.log; before the first snapshot lastIncludedIndex is 0 and
// the conversion is a no-op, so this is safe to use from day one.
// Caller must hold n.mu.
func (n *Node) logicalToPhysical(logicalIndex int) int {
	return logicalIndex - n.lastIncludedIndex
}

// min returns the smaller of two ints.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

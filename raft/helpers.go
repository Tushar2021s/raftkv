package raft

import "time"

// lastLogIndexLocked returns the logical index of the last log entry.
func (n *Node) lastLogIndexLocked() int {
	return n.lastIncludedIndex + len(n.log) - 1
}

// lastLogTermLocked returns the term of the last log entry.
func (n *Node) lastLogTermLocked() int {
	return n.log[len(n.log)-1].Term
}

// logicalToPhysical converts a logical log index to a physical slice index.
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

// sleep pauses for the given number of milliseconds. Used by
// waitForCommit to poll without spinning.
func sleep(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

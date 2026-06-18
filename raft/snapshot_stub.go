package raft

// HandleInstallSnapshot is the receiver-side handler for the
// InstallSnapshot RPC (Raft paper, Section 7). It replaces a follower's
// log with a compacted snapshot when the leader has already discarded
// the entries that follower would need to catch up normally.
//
// Full implementation lands in stage 6 (snapshotting). For now we
// return a valid reply so the HTTP transport compiles and the RPC
// endpoint exists on the wire -- a leader won't actually send this
// until the snapshotting stage activates it.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.resetElectionTimerLocked()
	return &InstallSnapshotReply{Term: n.currentTerm}
}

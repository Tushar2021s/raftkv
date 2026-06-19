package raft

// TakeSnapshot is called by the application (StateMachine) once its
// log has grown past a threshold. `index` is the last log entry
// included in the snapshot; `data` is the serialised state machine
// image.
//
// What we do here:
//   1. Verify the index is sensible (already committed, not in the future).
//   2. Discard all log entries up to and including index, replacing
//      them with a single sentinel entry that carries the snapshot's
//      term. This sentinel keeps the invariant that n.log[0] is always
//      a valid "previous entry" for the first real entry.
//   3. Persist the updated Raft state (new lastIncludedIndex/Term and
//      the shortened log) and the snapshot bytes.
//   4. Reset nextIndex for every peer to lastIncludedIndex+1 so the
//      next replicateToPeer call immediately knows to send a snapshot
//      instead of trying to find log entries that no longer exist.
//
// Step 4 is the key insight that prevents the timeout bug the previous
// attempt hit: by telling the leader "every peer needs a snapshot",
// we avoid any window where replicateToPeer tries to index into a
// log that's been trimmed and falls through to sendInstallSnapshot
// before the snapshot file has been written.
func (n *Node) TakeSnapshot(index int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Don't double-compact or compact past the committed frontier.
	if index <= n.lastIncludedIndex || index > n.commitIndex {
		return
	}

	phys := n.logicalToPhysical(index)
	if phys < 0 || phys >= len(n.log) {
		return
	}

	snapshotTerm := n.log[phys].Term

	// Build the new compacted log: one sentinel at physical[0]
	// carrying the snapshot's (index, term), plus any entries that
	// came AFTER the snapshot boundary.
	newLog := make([]LogEntry, 1, len(n.log)-phys)
	newLog[0] = LogEntry{Term: snapshotTerm, Index: index}
	newLog = append(newLog, n.log[phys+1:]...)

	n.log = newLog
	n.lastIncludedIndex = index
	n.lastIncludedTerm = snapshotTerm

	// Persist the new Raft state (shortened log + snapshot metadata).
	n.persistLocked()

	// Persist the snapshot bytes so a restarted node and lagged
	// followers can both recover from it.
	_ = n.persister.SaveSnapshot(data)

	// Tell the leader to send a snapshot to any peer whose nextIndex
	// has been compacted away. We do this unconditionally — even for
	// peers that are fully caught up — because it's cheap (nextIndex
	// advances past the snapshot boundary only if the peer already
	// has everything) and it eliminates the race where replicateToPeer
	// tries to slice into a log that was just trimmed.
	if n.state == Leader {
		for _, peerID := range n.peers {
			if n.nextIndex[peerID] <= index {
				n.nextIndex[peerID] = index + 1
			}
		}
	}
}

// HandleInstallSnapshot is the receiver-side handler for the
// InstallSnapshot RPC (Raft paper, Section 7). The leader calls this
// when a follower's nextIndex has fallen behind the leader's snapshot
// boundary, meaning there are no log entries to send — only the
// snapshot itself.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	// Ignore stale snapshots — our log is already ahead of this one.
	if args.LastIncludedIndex <= n.lastIncludedIndex {
		return &InstallSnapshotReply{Term: n.currentTerm}
	}

	// If we have log entries that extend past the snapshot, keep them
	// (we're just installing a baseline, not discarding newer work).
	// Otherwise compact the entire log down to just the sentinel.
	physEnd := n.logicalToPhysical(args.LastIncludedIndex)
	if physEnd >= 0 && physEnd < len(n.log) &&
		n.log[physEnd].Term == args.LastIncludedTerm {
		// Keep everything after the snapshot boundary.
		newLog := make([]LogEntry, 1, len(n.log)-physEnd)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}
		newLog = append(newLog, n.log[physEnd+1:]...)
		n.log = newLog
	} else {
		// Snapshot supersedes our entire log.
		n.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm

	// Advance bookkeeping so the applyLoop doesn't try to re-apply
	// entries the snapshot already covers.
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}

	n.persistLocked()
	_ = n.persister.SaveSnapshot(args.Data)

	// Deliver the snapshot to the state machine via applyCh. This is
	// the same channel the applyLoop uses for normal entries, so
	// ordering is preserved: once the state machine installs this
	// snapshot it will process only entries with index > LastIncludedIndex.
	go func() {
		n.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	return &InstallSnapshotReply{Term: n.currentTerm}
}

// ReadSnapshot returns the raw snapshot bytes from the persister.
// Used by tests and monitoring endpoints to inspect persisted state.
func (n *Node) ReadSnapshot() ([]byte, error) {
	return n.persister.ReadSnapshot()
}

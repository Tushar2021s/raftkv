package raft

import "time"

// HandleAppendEntries implements the receiver side of AppendEntries
// (Raft paper Figure 2). Uses logicalToPhysical() for all log accesses
// so it stays correct after log compaction.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.Term > n.currentTerm || n.state == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	lastIdx := n.lastLogIndexLocked()

	if args.PrevLogIndex > lastIdx {
		return &AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: lastIdx + 1, ConflictTerm: -1,
		}
	}

	prevPhys := n.logicalToPhysical(args.PrevLogIndex)
	if prevPhys < 0 {
		// Our snapshot already covers PrevLogIndex — just accept.
		return &AppendEntriesReply{Term: n.currentTerm, Success: true}
	}

	if n.log[prevPhys].Term != args.PrevLogTerm {
		conflictTerm := n.log[prevPhys].Term
		conflictLogical := args.PrevLogIndex
		for conflictLogical > n.lastIncludedIndex+1 {
			p := n.logicalToPhysical(conflictLogical - 1)
			if p < 0 || n.log[p].Term != conflictTerm {
				break
			}
			conflictLogical--
		}
		return &AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: conflictLogical, ConflictTerm: conflictTerm,
		}
	}

	for i, entry := range args.Entries {
		logical := args.PrevLogIndex + 1 + i
		phys := n.logicalToPhysical(logical)
		if phys <= 0 {
			// Already covered by snapshot.
			continue
		}
		if logical <= n.lastLogIndexLocked() {
			if n.log[phys].Term != entry.Term {
				n.log = append(n.log[:phys], entry)
			}
			continue
		}
		n.log = append(n.log, entry)
	}
	n.persistLocked()

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
	}
	return &AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) startHeartbeatLoop(term int) {
	for {
		n.mu.Lock()
		stillLeader := n.state == Leader && n.currentTerm == term
		n.mu.Unlock()
		if !stillLeader {
			return
		}
		n.broadcastAppendEntries(term)
		select {
		case <-time.After(heartbeatInterval):
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) broadcastAppendEntries(term int) {
	n.mu.Lock()
	peers := append([]int{}, n.peers...)
	n.mu.Unlock()
	for _, peerID := range peers {
		go n.replicateToPeer(peerID, term)
	}
}

// replicateToPeer sends one peer everything it's missing. If the peer
// needs entries that have already been compacted, it sends a snapshot
// instead. All log accesses use logicalToPhysical().
func (n *Node) replicateToPeer(peerID int, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	prevLogical := nextIdx - 1

	// If the peer needs entries before our snapshot boundary, send snapshot.
	if prevLogical < n.lastIncludedIndex {
		snapIndex := n.lastIncludedIndex
		snapTerm := n.lastIncludedTerm
		n.mu.Unlock()

		// Read snapshot from disk outside the lock to avoid holding
		// n.mu during file I/O.
		snapData, _ := n.persister.ReadSnapshot()
		n.sendInstallSnapshot(peerID, term, snapIndex, snapTerm, snapData)
		return
	}

	prevPhys := n.logicalToPhysical(prevLogical)
	if prevPhys < 0 || prevPhys >= len(n.log) {
		n.mu.Unlock()
		return
	}
	prevLogTerm := n.log[prevPhys].Term

	// Collect entries to send.
	var entries []LogEntry
	startPhys := n.logicalToPhysical(nextIdx)
	if startPhys > 0 && startPhys < len(n.log) {
		entries = append([]LogEntry{}, n.log[startPhys:]...)
	}

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogical,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peerID, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := prevLogical + len(entries)
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
			n.updateCommitIndexLocked()
		}
		return
	}

	// Fast backtracking.
	if reply.ConflictTerm == -1 {
		n.nextIndex[peerID] = reply.ConflictIndex
	} else {
		idx := n.lastLogIndexLocked()
		for idx > n.lastIncludedIndex {
			phys := n.logicalToPhysical(idx)
			if phys >= 0 && phys < len(n.log) && n.log[phys].Term == reply.ConflictTerm {
				break
			}
			idx--
		}
		if idx > n.lastIncludedIndex {
			n.nextIndex[peerID] = idx + 1
		} else {
			n.nextIndex[peerID] = reply.ConflictIndex
		}
	}

	go n.replicateToPeer(peerID, term)
}

// sendInstallSnapshot fires the InstallSnapshot RPC and updates
// nextIndex/matchIndex on success.
func (n *Node) sendInstallSnapshot(peerID, term, snapIndex, snapTerm int, data []byte) {
	if len(data) == 0 {
		return // no snapshot to send yet
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}
	reply, err := n.transport.SendInstallSnapshot(peerID, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if snapIndex+1 > n.nextIndex[peerID] {
		n.nextIndex[peerID] = snapIndex + 1
	}
	if snapIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = snapIndex
		n.updateCommitIndexLocked()
	}
}

// updateCommitIndexLocked advances commitIndex when an entry reaches
// majority. Only entries from the current term are directly committed
// (Raft §5.4.2).
func (n *Node) updateCommitIndexLocked() {
	if n.state != Leader {
		return
	}
	for idx := n.lastLogIndexLocked(); idx > n.commitIndex; idx-- {
		phys := n.logicalToPhysical(idx)
		if phys <= 0 || phys >= len(n.log) {
			continue
		}
		if n.log[phys].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, peerID := range n.peers {
			if n.matchIndex[peerID] >= idx {
				count++
			}
		}
		if count*2 > len(n.peers)+1 {
			n.commitIndex = idx
			return
		}
	}
}

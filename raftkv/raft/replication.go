package raft

import "time"

// HandleAppendEntries implements the receiver side of AppendEntries
// (Raft paper Figure 2). This handler hasn't changed at all since
// stage 2 -- it was written to be correct for real entries from day
// one, so the only thing that's different in stage 3 is that Entries
// is no longer always empty.
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

	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		conflictIdx := args.PrevLogIndex
		for conflictIdx > 0 && n.log[conflictIdx-1].Term == conflictTerm {
			conflictIdx--
		}
		return &AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: conflictIdx, ConflictTerm: conflictTerm,
		}
	}

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.lastLogIndexLocked() {
			if n.log[idx].Term != entry.Term {
				n.log = append(n.log[:idx], entry)
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

// startHeartbeatLoop runs for as long as this node remains the leader
// of `term`, firing broadcastAppendEntries on a fixed interval. It
// does double duty as both the heartbeat mechanism (keeping followers
// from timing out) and the retry mechanism for replication -- any
// entry that didn't make it on the first try gets resent here too.
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

// broadcastAppendEntries fires AppendEntries at every peer in
// parallel. Each peer gets exactly the entries it's actually missing
// (everything from nextIndex[peer] onward) -- which is empty, and
// therefore just a heartbeat, for any peer that's already caught up.
func (n *Node) broadcastAppendEntries(term int) {
	n.mu.Lock()
	peers := append([]int{}, n.peers...)
	n.mu.Unlock()

	for _, peerID := range peers {
		go n.replicateToPeer(peerID, term)
	}
}

// replicateToPeer sends one peer everything it's missing and processes
// the result: on success, advances that peer's matchIndex and checks
// whether a new entry just reached a majority. On failure, backs off
// nextIndex using the leader's knowledge of where the logs actually
// diverge (Figure 2's ConflictIndex/ConflictTerm optimization) and
// retries immediately instead of waiting for the next heartbeat tick.
func (n *Node) replicateToPeer(peerID int, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	prevLogIndex := n.nextIndex[peerID] - 1
	prevLogTerm := n.log[prevLogIndex].Term
	// Copy the pending entries out from under the lock -- Submit() can
	// append more to n.log concurrently, and this in-flight RPC
	// shouldn't see those sneak in mid-send.
	entries := append([]LogEntry{}, n.log[prevLogIndex+1:]...)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peerID, args)
	if err != nil {
		return // peer unreachable; the next periodic tick will try again
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return // no longer the leader of this term; this reply is stale
	}

	if reply.Success {
		newMatch := prevLogIndex + len(entries)
		if newMatch > n.matchIndex[peerID] { // guard against an out-of-order stale reply
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
			n.updateCommitIndexLocked()
		}
		return
	}

	// Back off using the fast-backtracking optimization: if the
	// follower had no entry at all at PrevLogIndex, jump straight to
	// where its log actually ends. Otherwise, find the last entry in
	// our own log carrying the conflicting term and resume just after
	// it.
	if reply.ConflictTerm == -1 {
		n.nextIndex[peerID] = reply.ConflictIndex
	} else {
		idx := n.lastLogIndexLocked()
		for idx > 0 && n.log[idx].Term != reply.ConflictTerm {
			idx--
		}
		if idx > 0 {
			n.nextIndex[peerID] = idx + 1
		} else {
			n.nextIndex[peerID] = reply.ConflictIndex
		}
	}

	go n.replicateToPeer(peerID, term) // retry now rather than waiting for the next tick
}

// updateCommitIndexLocked checks whether any not-yet-committed entry
// has now reached a majority of matchIndex values, and advances
// commitIndex if so.
//
// The rule that's easy to get wrong here (Raft paper, Section 5.4.2):
// a leader may only commit an entry *this way* if that entry belongs
// to its own current term. Committing an older-term entry directly,
// just because a majority happens to already hold it, can violate
// safety in a narrow but real scenario -- so older entries only ever
// get committed as a side effect of committing a later current-term
// entry that covers them.
func (n *Node) updateCommitIndexLocked() {
	if n.state != Leader {
		return
	}
	for idx := n.lastLogIndexLocked(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1 // the leader itself
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

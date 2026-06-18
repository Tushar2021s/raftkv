package raft

import "time"

// HandleAppendEntries implements the receiver side of AppendEntries
// (Raft paper Figure 2). With Entries left empty, this is purely a
// heartbeat. Stage 3 adds real log entries to the leader side of this
// RPC -- nothing in this handler needs to change when that happens;
// it's already fully correct for both cases.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// A legitimate leader exists for this term (or a newer one) --
	// recognize it, even if we were ourselves mid-election.
	if args.Term > n.currentTerm || n.state == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.resetElectionTimerLocked()

	lastIdx := n.lastLogIndexLocked()

	// Figure 2, receiver implementation rule 2: we don't even have an
	// entry at PrevLogIndex yet.
	if args.PrevLogIndex > lastIdx {
		return &AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: lastIdx + 1, ConflictTerm: -1,
		}
	}

	// Rule 2/3: the term at PrevLogIndex doesn't match the leader's --
	// our log diverges here. Walk back to the start of that conflicting
	// term so the leader can jump straight to the right spot next time,
	// instead of backing off one entry per round trip.
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

	// Rule 3/4: append any genuinely new entries, truncating from the
	// first point of disagreement and trusting the leader's version of
	// history from there on.
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

	// Rule 5: we can safely advance our commit index to match the
	// leader's, capped at whatever we actually hold in our own log.
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
	}

	return &AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// startHeartbeatLoop runs for as long as this node remains the leader
// of `term`. The moment either condition stops holding -- a newer term
// starts, or we're no longer Leader -- the loop notices on its very
// next iteration and exits on its own. No separate cancellation signal
// is needed; the term comparison does that job.
func (n *Node) startHeartbeatLoop(term int) {
	for {
		n.mu.Lock()
		stillLeader := n.state == Leader && n.currentTerm == term
		n.mu.Unlock()
		if !stillLeader {
			return
		}

		n.sendHeartbeats(term)

		select {
		case <-time.After(heartbeatInterval):
		case <-n.stopCh:
			return
		}
	}
}

// sendHeartbeats fires an empty AppendEntries at every peer in
// parallel. Stage 3 extends this same call site to also carry real
// pending log entries -- the wire format already supports it
// (Entries is a slice); right now we're just always sending an empty
// one.
func (n *Node) sendHeartbeats(term int) {
	n.mu.Lock()
	peers := append([]int{}, n.peers...)
	n.mu.Unlock()

	for _, peerID := range peers {
		go func(peerID int) {
			n.mu.Lock()
			if n.state != Leader || n.currentTerm != term {
				n.mu.Unlock()
				return
			}
			prevLogIndex := n.nextIndex[peerID] - 1
			prevLogTerm := n.log[prevLogIndex].Term
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
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
			}
		}(peerID)
	}
}

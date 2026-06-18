package raft

// startElection turns this node into a Candidate, votes for itself,
// and asks every peer for its vote in parallel (Raft paper, Section
// 5.2). If a strict majority agree, becomeLeaderLocked promotes us to
// Leader.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.persistLocked()
	n.resetElectionTimerLocked()
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	peers := append([]int{}, n.peers...)
	candidateID := n.id
	n.mu.Unlock()

	votes := 1 // we voted for ourselves

	for _, peerID := range peers {
		go func(peerID int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.transport.SendRequestVote(peerID, args)
			if err != nil {
				return // peer unreachable -- its vote simply doesn't count
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			// This reply belongs to an election we've already moved past
			// (we lost candidacy, or a newer term has since started).
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes*2 > len(n.peers)+1 { // strict majority of the whole cluster
					n.becomeLeaderLocked()
				}
			}
		}(peerID)
	}
}

// HandleRequestVote implements the receiver side of RequestVote (Raft
// paper Figure 2, plus the "up-to-date log" rule from Section 5.4.1).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()

	// "Up-to-date" per 5.4.1: a strictly higher term wins outright; on a
	// tied term, the longer log wins. This is the rule that prevents a
	// candidate with a stale log from ever getting elected and silently
	// losing entries the cluster already committed.
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	alreadyVotedForSomeoneElse := n.votedFor != -1 && n.votedFor != args.CandidateID

	if alreadyVotedForSomeoneElse || !logIsUpToDate {
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = args.CandidateID
	n.persistLocked()
	n.resetElectionTimerLocked() // seeing a legitimate candidate also resets our own clock
	return &RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
}

// becomeFollowerLocked resets us to Follower under a new term. Called
// any time we see an RPC carrying a higher term than our own -- the
// rule that guarantees at most one leader is ever legitimate for a
// given term.
func (n *Node) becomeFollowerLocked(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.persistLocked()
}

// becomeLeaderLocked promotes a successful candidate to Leader and
// reinitializes the leader-only volatile state (Figure 2: nextIndex
// optimistically assumes peers are fully caught up; matchIndex starts
// at zero until we hear otherwise).
func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	for _, peerID := range n.peers {
		n.nextIndex[peerID] = len(n.log)
		n.matchIndex[peerID] = 0
	}
	term := n.currentTerm
	go n.startHeartbeatLoop(term)
}

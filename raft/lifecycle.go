package raft

import (
	"math/rand"
	"time"
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

// Run starts the node's background goroutines: the election clock and
// the loop that delivers committed entries to the application. Call it
// once, right after construction.
func (n *Node) Run() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	go n.electionTicker()
	go n.applyLoop()
}

// Stop halts every background goroutine belonging to this node. Safe
// to call more than once -- used both for graceful shutdown and, just
// as importantly, to simulate a crash in tests.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) resetElectionTimerLocked() {
	n.electionResetAt = time.Now()
	span := electionTimeoutMax - electionTimeoutMin
	n.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// electionTicker periodically checks whether too much time has passed
// since we last heard from a legitimate leader (or cast a vote). If
// so, and we're not the leader ourselves, we start an election.
// Randomizing the timeout (see resetElectionTimerLocked) is what makes
// split votes rare in practice: it's vanishingly unlikely for two
// nodes to time out in the same instant and call dueling elections.
func (n *Node) electionTicker() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			expired := time.Since(n.electionResetAt) >= n.electionTimeout
			isLeader := n.state == Leader
			n.mu.Unlock()

			if expired && !isLeader {
				n.startElection()
			}
		}
	}
}

// applyLoop watches commitIndex and hands every newly committed entry
// to the application via applyCh, in order, exactly once. This is the
// *only* path through which a command ever reaches the state machine
// -- which is exactly what guarantees every node in the cluster ends
// up applying the same commands in the same order, no matter how
// leadership changed hands along the way.
func (n *Node) applyLoop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			var pending []ApplyMsg
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				pending = append(pending, ApplyMsg{
					CommandValid: true,
					Command:      n.log[n.lastApplied].Command,
					CommandIndex: n.lastApplied,
				})
			}
			n.mu.Unlock()

			for _, msg := range pending {
				n.applyCh <- msg
			}
		}
	}
}

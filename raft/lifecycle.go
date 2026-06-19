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

// Run starts the node's background goroutines. Call once after construction.
func (n *Node) Run() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	// If this node restarted with a persisted snapshot, deliver it to
	// the state machine immediately so it rebuilds its state before
	// processing any new log entries.
	go n.restoreSnapshot()

	go n.electionTicker()
	go n.applyLoop()
}

// restoreSnapshot delivers any persisted snapshot to the state machine
// on startup, then lets the applyLoop take over for entries above it.
func (n *Node) restoreSnapshot() {
	data, err := n.persister.ReadSnapshot()
	if err != nil || len(data) == 0 {
		return
	}
	n.mu.Lock()
	snapIndex := n.lastIncludedIndex
	snapTerm := n.lastIncludedTerm
	n.mu.Unlock()

	if snapIndex == 0 {
		return
	}

	n.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  snapTerm,
		SnapshotIndex: snapIndex,
	}
}

// Stop halts every background goroutine. Safe to call more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) resetElectionTimerLocked() {
	n.electionResetAt = time.Now()
	span := electionTimeoutMax - electionTimeoutMin
	n.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

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

// applyLoop watches commitIndex and delivers every newly committed entry
// to the application via applyCh, in logical-index order. Updated in
// stage 6 to use logicalToPhysical() for every log access so it
// handles post-compaction indices correctly.
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
				phys := n.logicalToPhysical(n.lastApplied)
				// Skip entries that fall within the snapshot boundary
				// (shouldn't happen in normal flow, but be defensive).
				if phys <= 0 || phys >= len(n.log) {
					continue
				}
				pending = append(pending, ApplyMsg{
					CommandValid: true,
					Command:      n.log[phys].Command,
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

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

func (n *Node) Run() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	go n.restoreSnapshot()
	go n.electionTicker()
	go n.applyLoop()
}

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

// applyLoop delivers committed entries to the application. Config-change
// entries are intercepted here and applied directly to the Node's own
// state (clusterConfig, peers) rather than forwarded to the KV state
// machine — the state machine has no business knowing about cluster topology.
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
				if phys <= 0 || phys >= len(n.log) {
					continue
				}
				entry := n.log[phys]

				// Config-change entries are handled here, inside the
				// lock — they update n.clusterConfig and n.peers
				// immediately so that the new quorum rules are in
				// effect for the very next entry.
				if entry.Type == EntryConfig {
					continue
				}

				pending = append(pending, ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
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

package raft

import (
	"encoding/json"
	"errors"
	"sort"
)

var (
	ErrConfigChangeInProgress = errors.New("raft: a config change is already in progress")
	ErrSingleNode             = errors.New("raft: cannot remove the last node")
)

// AddMember initiates a joint-consensus membership change to add newID
// to the cluster. Only the leader can do this. The caller blocks until
// the C_new-only entry commits (i.e., the transition is fully complete)
// or until the context times out.
//
// The two-phase protocol (§6):
//
//  Phase 1 — leader appends C_old,new entry. Every node switches to
//            joint config the moment it appends this entry. In joint
//            config a majority of BOTH C_old and C_new must agree for
//            anything to commit — this prevents two separate majorities
//            from being elected at the same time.
//
//  Phase 2 — once C_old,new commits, leader immediately appends C_new
//            entry. Once C_new commits, nodes in C_old-only drop out.
//
// Between phase 1 and phase 2 the leader replicates to ALL members of
// C_old ∪ C_new, so the new node receives a snapshot if it's behind.
func (n *Node) AddMember(newID int) error {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.pendingConfigIndex >= 0 && n.pendingConfigIndex > n.commitIndex {
		n.mu.Unlock()
		return ErrConfigChangeInProgress
	}

	// Build C_new = C_old + newID (deduplicated, sorted).
	newMembers := dedupSorted(append(append([]int{}, n.configStable.Members...), newID))
	newConfig := ClusterConfig{Members: newMembers}

	err := n.startConfigChangeLocked(newConfig)
	n.mu.Unlock()
	return err
}

// RemoveMember initiates a joint-consensus membership change to remove
// removeID from the cluster.
//
// Special case: if the leader is removing itself, it steps down after
// the C_new entry commits and the remaining nodes elect a new leader.
func (n *Node) RemoveMember(removeID int) error {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.pendingConfigIndex >= 0 && n.pendingConfigIndex > n.commitIndex {
		n.mu.Unlock()
		return ErrConfigChangeInProgress
	}

	newMembers := dedupSorted(without(n.configStable.Members, removeID))
	if len(newMembers) == 0 {
		n.mu.Unlock()
		return ErrSingleNode
	}
	newConfig := ClusterConfig{Members: newMembers}

	err := n.startConfigChangeLocked(newConfig)
	n.mu.Unlock()
	return err
}

// startConfigChangeLocked appends the C_old,new joint entry to the log
// and switches this node into joint config immediately.
// Caller must hold n.mu.
func (n *Node) startConfigChangeLocked(newConfig ClusterConfig) error {
	args := ConfigChangeArgs{
		IsJoint: true,
		Old:     n.configStable,
		New:     newConfig,
	}
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}

	index := n.lastLogIndexLocked() + 1
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: data,
		Type:    EntryConfig,
	}
	n.log = append(n.log, entry)
	n.pendingConfigIndex = index
	n.persistLocked()

	// Switch to joint config immediately on append (§6 rule).
	n.applyConfigLocked(args)

	term := n.currentTerm
	go n.broadcastAppendEntries(term)
	return nil
}

// applyConfigLocked updates the in-memory membership state when a
// config entry is appended or restored from the log. Called both by
// the leader (on append) and by every node (on HandleAppendEntries).
// Caller must hold n.mu.
func (n *Node) applyConfigLocked(args ConfigChangeArgs) {
	if args.IsJoint {
		// Enter joint phase: majority of BOTH C_old and C_new required.
		n.configOld = args.Old
		n.configNew = args.New
		n.configJoint = true

		// peers = union of all members except self.
		all := dedupSorted(append(append([]int{}, args.Old.Members...), args.New.Members...))
		n.peers = without(all, n.id)

		// Initialise leader state for any brand-new peer.
		if n.state == Leader {
			lastIdx := n.lastLogIndexLocked()
			for _, pid := range n.peers {
				if _, ok := n.nextIndex[pid]; !ok {
					n.nextIndex[pid] = lastIdx + 1
					n.matchIndex[pid] = 0
				}
			}
		}
	} else {
		// Exit joint phase: C_new is now the stable config.
		n.configStable = args.New
		n.configJoint = false
		n.configNew = ClusterConfig{}
		n.configOld = ClusterConfig{}

		// peers = C_new members except self.
		n.peers = without(args.New.Members, n.id)

		// If the leader just removed itself, step down.
		if n.state == Leader {
			selfInNew := false
			for _, m := range args.New.Members {
				if m == n.id {
					selfInNew = true
					break
				}
			}
			if !selfInNew {
				n.becomeFollowerLocked(n.currentTerm)
			}
		}
	}
}

// commitConfigLocked is called by updateCommitIndexLocked when a config
// entry commits. If it's a joint entry, the leader immediately appends
// the C_new-only entry to complete the transition.
// Caller must hold n.mu.
func (n *Node) commitConfigLocked(entry LogEntry) {
	var args ConfigChangeArgs
	if err := json.Unmarshal(entry.Command, &args); err != nil {
		return
	}
	if !args.IsJoint {
		// C_new committed — transition complete.
		n.pendingConfigIndex = -1
		return
	}

	// C_old,new just committed. If we're the leader, append C_new.
	if n.state != Leader {
		return
	}

	finalArgs := ConfigChangeArgs{IsJoint: false, New: args.New}
	data, err := json.Marshal(finalArgs)
	if err != nil {
		return
	}

	index := n.lastLogIndexLocked() + 1
	entry2 := LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: data,
		Type:    EntryConfig,
	}
	n.log = append(n.log, entry2)
	n.pendingConfigIndex = index
	n.persistLocked()

	// Apply C_new immediately on append.
	n.applyConfigLocked(finalArgs)

	term := n.currentTerm
	go n.broadcastAppendEntries(term)
}

// quorumReached returns true if `matches` (a set of node IDs that
// have replicated an entry) forms a valid quorum. In stable config,
// this is a simple majority. In joint config, it requires a majority
// of BOTH C_old and C_new simultaneously.
// Caller must hold n.mu.
func (n *Node) quorumReached(matches map[int]bool) bool {
	if !n.configJoint {
		return majority(n.configStable.Members, matches)
	}
	return majority(n.configOld.Members, matches) &&
		majority(n.configNew.Members, matches)
}

// majority returns true if more than half of `group` are in `matches`.
func majority(group []int, matches map[int]bool) bool {
	count := 0
	for _, id := range group {
		if matches[id] {
			count++
		}
	}
	return count*2 > len(group)
}

// --------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------

func dedupSorted(ids []int) []int {
	seen := make(map[int]bool)
	result := ids[:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	sort.Ints(result)
	return result
}

func without(ids []int, remove int) []int {
	result := make([]int, 0, len(ids))
	for _, id := range ids {
		if id != remove {
			result = append(result, id)
		}
	}
	return result
}


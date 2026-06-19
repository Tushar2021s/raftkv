package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
	"github.com/Tushar2021s/raftkv/transport"
)

// memberCluster is a test cluster that supports dynamic resizing.
type memberCluster struct {
	nodes   []*raft.Node
	lt      *transport.LocalTransport
	nextID  int
	cleanup func()
}

func newMemberCluster(t *testing.T, n int) *memberCluster {
	t.Helper()
	lt := transport.NewLocalTransport()
	nodes := make([]*raft.Node, n)

	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}

	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		ch := make(chan raft.ApplyMsg, 512)
		node := raft.NewNode(i, peers, lt, raft.NewMemoryPersister(), ch)
		nodes[i] = node
		lt.Register(i, node)
	}
	for _, node := range nodes {
		node.Run()
	}

	return &memberCluster{
		nodes:  nodes,
		lt:     lt,
		nextID: n,
		cleanup: func() {
			for _, node := range nodes {
				node.Stop()
			}
		},
	}
}

func (c *memberCluster) leader(t *testing.T) (int, *raft.Node) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, node := range c.nodes {
			if s, _ := node.State(); s == raft.Leader {
				return i, node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader within 3s")
	return -1, nil
}

func (c *memberCluster) waitCommit(t *testing.T, node *raft.Node, minIndex int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, term := node.State(); term > 0 {
			// Use CommitIndex accessor
			if node.CommitIndex() >= minIndex {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d: commitIndex never reached %d", node.ID(), minIndex)
}

// TestAddMember verifies that a new node can join a live 3-node cluster
// via joint consensus, and that the cluster remains healthy (still
// elects leaders and commits entries) after the transition completes.
func TestAddMember(t *testing.T) {
	c := newMemberCluster(t, 3)
	defer c.cleanup()

	leaderIdx, leaderNode := c.leader(t)
	t.Logf("initial leader: node %d", leaderIdx)

	// Spin up the new node (ID=3) with no peers yet — it'll be told
	// about the cluster via the config change entry.
	newID := 3
	newCh := make(chan raft.ApplyMsg, 512)
	newNode := raft.NewNode(newID, []int{0, 1, 2}, c.lt, raft.NewMemoryPersister(), newCh)
	c.lt.Register(newID, newNode)
	newNode.Run()
	c.nodes = append(c.nodes, newNode)

	// Tell the leader to add node 3.
	if err := leaderNode.AddMember(newID); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	t.Logf("AddMember(3) submitted by node %d", leaderIdx)

	// Give the joint entry and C_new entry time to commit.
	time.Sleep(800 * time.Millisecond)

	// The cluster should now have 4 members. Verify by submitting a
	// command and confirming it commits.
	cmd := []byte("probe-after-add")
	idx, _, isLeader := leaderNode.Submit(cmd)
	if !isLeader {
		// Leader may have changed during the config transition; find new one.
		_, leaderNode = c.leader(t)
		idx, _, isLeader = leaderNode.Submit(cmd)
		if !isLeader {
			t.Fatal("no leader after membership change")
		}
	}

	// Wait for all 4 nodes to commit the probe entry.
	for _, node := range c.nodes {
		c.waitCommit(t, node, idx)
	}

	// Confirm the new node's config includes itself.
	cfg := newNode.Config()
	found := false
	for _, m := range cfg.Members {
		if m == newID {
			found = true
		}
	}
	if !found {
		t.Fatalf("new node config doesn't include self: %v", cfg.Members)
	}

	t.Logf("TestAddMember OK: cluster grew to %d members %v; probe committed at index %d",
		len(cfg.Members), cfg.Members, idx)
}

// TestRemoveMember verifies that a follower can be removed from a live
// 3-node cluster. After removal the remaining 2-node cluster (which is
// now a 2-of-2 majority) can still commit entries.
func TestRemoveMember(t *testing.T) {
	c := newMemberCluster(t, 3)
	defer c.cleanup()

	leaderIdx, leaderNode := c.leader(t)

	// Remove a follower (not the leader).
	removeIdx := -1
	for i := range c.nodes {
		if i != leaderIdx {
			removeIdx = i
			break
		}
	}
	removeID := c.nodes[removeIdx].ID()
	t.Logf("removing node %d from 3-node cluster (leader is %d)", removeID, leaderIdx)

	if err := leaderNode.RemoveMember(removeID); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}

	// Give joint + C_new entries time to commit.
	time.Sleep(800 * time.Millisecond)

	// Cluster should now be 2 nodes. Submit a command and confirm it commits.
	cmd := []byte("probe-after-remove")
	idx, _, isLeader := leaderNode.Submit(cmd)
	if !isLeader {
		_, leaderNode = c.leader(t)
		idx, _, isLeader = leaderNode.Submit(cmd)
		if !isLeader {
			t.Fatal("no leader after removal")
		}
	}

	// Wait for both remaining nodes to commit.
	for i, node := range c.nodes {
		if i == removeIdx {
			continue // removed node — skip
		}
		c.waitCommit(t, node, idx)
	}

	cfg := leaderNode.Config()
	for _, m := range cfg.Members {
		if m == removeID {
			t.Fatalf("removed node %d still appears in config: %v", removeID, cfg.Members)
		}
	}
	t.Logf("TestRemoveMember OK: cluster shrank to %v; probe committed at index %d",
		cfg.Members, idx)
}

// TestLeaderRemovesSelf verifies that when the leader removes itself,
// it steps down and the remaining nodes elect a new leader.
func TestLeaderRemovesSelf(t *testing.T) {
	c := newMemberCluster(t, 3)
	defer c.cleanup()

	leaderIdx, leaderNode := c.leader(t)
	leaderID := leaderNode.ID()
	t.Logf("leader node %d removing itself", leaderID)

	if err := leaderNode.RemoveMember(leaderID); err != nil {
		t.Fatalf("RemoveMember(self): %v", err)
	}

	// Give the transition time to complete and a new leader to emerge.
	time.Sleep(1200 * time.Millisecond)

	// Original leader should no longer be Leader.
	if s, _ := leaderNode.State(); s == raft.Leader {
		t.Fatal("original leader still believes it's leader after self-removal")
	}

	// One of the remaining two nodes should be the new leader.
	newLeaderFound := false
	for i, node := range c.nodes {
		if i == leaderIdx {
			continue
		}
		if s, _ := node.State(); s == raft.Leader {
			newLeaderFound = true
			t.Logf("new leader after self-removal: node %d", node.ID())
			break
		}
	}
	if !newLeaderFound {
		t.Fatal("no new leader elected after self-removal")
	}

	t.Logf("TestLeaderRemovesSelf OK: node %d stepped down, new leader elected", leaderID)
}

// TestConcurrentWritesDuringMembership verifies that normal KV writes
// are not interrupted by a membership change — the cluster continues
// committing entries throughout the joint phase.
func TestConcurrentWritesDuringMembership(t *testing.T) {
	c := newMemberCluster(t, 3)
	defer c.cleanup()

	leaderIdx, leaderNode := c.leader(t)
	t.Logf("leader: node %d", leaderIdx)

	// Submit 5 entries, then trigger membership change, then 5 more.
	commitEntry := func(label string) int {
		idx, _, ok := leaderNode.Submit([]byte(label))
		if !ok {
			// Find new leader if needed.
			_, leaderNode = c.leader(t)
			idx, _, ok = leaderNode.Submit([]byte(label))
			if !ok {
				t.Fatalf("submit %s: not leader", label)
			}
		}
		return idx
	}

	var firstBatch []int
	for i := 0; i < 5; i++ {
		firstBatch = append(firstBatch, commitEntry(fmt.Sprintf("pre-%d", i)))
	}

	// Add a new node mid-stream.
	newID := 3
	newCh := make(chan raft.ApplyMsg, 512)
	newNode := raft.NewNode(newID, []int{0, 1, 2}, c.lt, raft.NewMemoryPersister(), newCh)
	c.lt.Register(newID, newNode)
	newNode.Run()
	c.nodes = append(c.nodes, newNode)

	if err := leaderNode.AddMember(newID); err != nil {
		t.Fatalf("AddMember: %v", err)
	}

	var secondBatch []int
	for i := 0; i < 5; i++ {
		secondBatch = append(secondBatch, commitEntry(fmt.Sprintf("post-%d", i)))
	}

	// All entries (both batches) should eventually commit on the original
	// 3 nodes. The new node may lag due to catchup but the others shouldn't.
	time.Sleep(500 * time.Millisecond)
	lastIdx := secondBatch[len(secondBatch)-1]
	for i, node := range c.nodes[:3] {
		_ = i
		c.waitCommit(t, node, lastIdx)
	}

	t.Logf("TestConcurrentWritesDuringMembership OK: %d entries committed across membership change",
		len(firstBatch)+len(secondBatch))
}

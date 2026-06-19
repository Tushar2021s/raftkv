package raft_test

// These tests live in an external _test package (raft_test, not raft)
// specifically to avoid an import cycle: transport imports raft, so a
// test that wants to use transport.LocalTransport can't itself live
// inside package raft. This is also exactly the shape integration
// tests for the later stages (replication, chaos testing) will take.

import (
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
	"github.com/Tushar2021s/raftkv/transport"
)

// newTestCluster wires up n raft.Node instances on a shared
// LocalTransport and starts them. The returned transport is exposed so
// individual tests can simulate failures (Unregister) on top of just
// stopping a node's own goroutines (Stop).
func newTestCluster(t *testing.T, n int) ([]*raft.Node, *transport.LocalTransport, func()) {
	t.Helper()
	lt := transport.NewLocalTransport()
	nodes := make([]*raft.Node, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 256)
		node := raft.NewNode(i, peers, lt, raft.NewMemoryPersister(), applyCh)
		nodes[i] = node
		lt.Register(i, node)
	}

	for _, node := range nodes {
		node.Run()
	}

	cleanup := func() {
		for _, node := range nodes {
			node.Stop()
		}
	}
	return nodes, lt, cleanup
}

// waitForLeader polls until exactly one node in the given set reports
// itself as Leader, or fails the test if that doesn't happen in time.
func waitForLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if state, _ := node.State(); state == raft.Leader {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected among %d nodes within %s", len(nodes), timeout)
	return nil
}

func TestLeaderElection(t *testing.T) {
	nodes, _, cleanup := newTestCluster(t, 5)
	defer cleanup()

	leader := waitForLeader(t, nodes, 2*time.Second)
	_, term := leader.State()
	t.Logf("elected leader: node %d, term %d", leader.ID(), term)

	// Every other node should agree it's a Follower in that same term --
	// if two nodes both think they're Leader in the same term, that's a
	// safety violation, not just a flaky test.
	for _, node := range nodes {
		if node.ID() == leader.ID() {
			continue
		}
		state, nodeTerm := node.State()
		if state == raft.Leader {
			t.Fatalf("node %d also believes it's Leader in term %d -- split brain", node.ID(), nodeTerm)
		}
	}
}

func TestReElectionAfterLeaderCrash(t *testing.T) {
	nodes, lt, cleanup := newTestCluster(t, 5)
	defer cleanup()

	firstLeader := waitForLeader(t, nodes, 2*time.Second)
	_, firstTerm := firstLeader.State()

	// Simulate a real crash: stop the node's own goroutines AND make it
	// unreachable to everyone else.
	firstLeader.Stop()
	lt.Unregister(firstLeader.ID())

	remaining := make([]*raft.Node, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.ID() != firstLeader.ID() {
			remaining = append(remaining, node)
		}
	}

	secondLeader := waitForLeader(t, remaining, 3*time.Second)
	_, secondTerm := secondLeader.State()

	if secondLeader.ID() == firstLeader.ID() {
		t.Fatalf("expected a different node to take over leadership")
	}
	if secondTerm <= firstTerm {
		t.Fatalf("expected the new term (%d) to exceed the old leader's term (%d)", secondTerm, firstTerm)
	}
	t.Logf("crashed leader was node %d (term %d); re-elected node %d (term %d)",
		firstLeader.ID(), firstTerm, secondLeader.ID(), secondTerm)
}

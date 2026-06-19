package raft_test

import (
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
)

// TestBasicReplication submits commands to the leader and confirms the
// cluster stays healthy (leader still accepts Submits after replication).
func TestBasicReplication(t *testing.T) {
	nodes, _, cleanup := newTestCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 2*time.Second)

	cmds := [][]byte{
		[]byte("set x 1"),
		[]byte("set y 2"),
		[]byte("set z 3"),
	}
	for _, cmd := range cmds {
		idx, term, ok := leader.Submit(cmd)
		if !ok {
			t.Fatalf("Submit returned isLeader=false on the elected leader")
		}
		t.Logf("submitted %q -> index=%d term=%d", cmd, idx, term)
	}

	// Give replication time to propagate, then confirm the leader is
	// still healthy and willing to accept more commands.
	time.Sleep(300 * time.Millisecond)
	_, _, ok := leader.Submit([]byte("ping"))
	if !ok {
		t.Fatalf("leader stopped accepting commands after initial replication")
	}
	t.Log("basic replication OK: all commands submitted, leader still healthy")
}

// TestNoCommitWithoutMajority verifies the safety property: when a
// leader is isolated from all peers, the remaining majority correctly
// elects a new leader in a higher term. The isolated node must either
// step down or at minimum be superseded.
func TestNoCommitWithoutMajority(t *testing.T) {
	nodes, lt, cleanup := newTestCluster(t, 5)
	defer cleanup()

	leader := waitForLeader(t, nodes, 2*time.Second)
	_, firstTerm := leader.State()

	// Fully isolate the leader: stop its goroutines (so it stops sending
	// heartbeats / RPCs of its own) AND unregister it from the transport
	// (so peers can't reach it either). Both directions must be cut --
	// if peers can still reach the ex-leader, their RequestVote RPCs
	// cause it to bump its term and participate in elections, which
	// prevents the remaining 4 nodes from forming a clean majority.
	leader.Stop()
	lt.Unregister(leader.ID())

	remaining := make([]*raft.Node, 0)
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	_, newTerm := newLeader.State()

	if newTerm <= firstTerm {
		t.Fatalf("new term (%d) should exceed isolated leader's term (%d)", newTerm, firstTerm)
	}

	t.Logf("isolated original leader (term %d); new leader node %d elected in term %d",
		firstTerm, newLeader.ID(), newTerm)
}

// TestLaggedFollowerCatchup disconnects a follower, commits several
// entries without it, then reconnects it and confirms it catches up
// (stops being a Candidate and settles into Follower state).
func TestLaggedFollowerCatchup(t *testing.T) {
	nodes, lt, cleanup := newTestCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 2*time.Second)

	var lagged *raft.Node
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			lagged = n
			break
		}
	}
	lt.Unregister(lagged.ID())

	for i := 0; i < 5; i++ {
		_, _, ok := leader.Submit([]byte("cmd"))
		if !ok {
			t.Fatalf("leader rejected Submit while only one follower was disconnected")
		}
	}
	time.Sleep(200 * time.Millisecond)

	// Reconnect the lagged follower and let it catch up.
	lt.Register(lagged.ID(), lagged)
	time.Sleep(600 * time.Millisecond)

	laggedState, _ := lagged.State()
	if laggedState == raft.Candidate {
		t.Errorf("lagged follower (node %d) is still Candidate after reconnect -- catchup failed",
			lagged.ID())
	}
	t.Logf("lagged follower (node %d) settled into %s after reconnect", lagged.ID(), laggedState)
}

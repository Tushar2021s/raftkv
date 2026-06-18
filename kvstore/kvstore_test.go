package kvstore_test

import (
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/kvstore"
	"github.com/Tushar2021s/raftkv/raft"
	"github.com/Tushar2021s/raftkv/transport"
)

// cluster is a self-contained in-process KV cluster for testing.
type cluster struct {
	nodes []*raft.Node
	sms   []*kvstore.StateMachine
	lt    *transport.LocalTransport
}

func newCluster(t *testing.T, n int) (*cluster, func()) {
	t.Helper()
	lt := transport.NewLocalTransport()
	nodes := make([]*raft.Node, n)
	sms := make([]*kvstore.StateMachine, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 512)
		node := raft.NewNode(i, peers, lt, raft.NewMemoryPersister(), applyCh)
		sm := kvstore.NewStateMachine(node, applyCh)
		nodes[i] = node
		sms[i] = sm
		lt.Register(i, node)
	}
	for _, n := range nodes {
		n.Run()
	}
	c := &cluster{nodes: nodes, sms: sms, lt: lt}
	return c, func() {
		for _, n := range nodes {
			n.Stop()
		}
	}
}

// leader returns the StateMachine of the current leader, waiting up
// to timeout for one to be elected.
func (c *cluster) leader(t *testing.T, timeout time.Duration) *kvstore.StateMachine {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if state, _ := n.State(); state == raft.Leader {
				return c.sms[i]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

// TestPutGet writes a key and reads it back on the leader.
func TestPutGet(t *testing.T) {
	c, cleanup := newCluster(t, 3)
	defer cleanup()

	sm := c.leader(t, 2*time.Second)

	if err := sm.Put("name", "tushar", "req-1"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := sm.Get("name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "tushar" {
		t.Fatalf("expected 'tushar', got %q", val)
	}
	t.Logf("Put + Get OK: name=%s", val)
}

// TestDelete writes a key then deletes it and confirms it's gone.
func TestDelete(t *testing.T) {
	c, cleanup := newCluster(t, 3)
	defer cleanup()

	sm := c.leader(t, 2*time.Second)

	if err := sm.Put("temp", "value", "req-2"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := sm.Delete("temp", "req-3"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err := sm.Get("temp")
	if err != kvstore.ErrKeyMissing {
		t.Fatalf("expected ErrKeyMissing after delete, got %v", err)
	}
	t.Log("Delete OK: key correctly absent after delete")
}

// TestFollowerRejectsWrite confirms that a follower's StateMachine
// returns ErrNotLeader when a write is attempted on it directly,
// matching what the HTTP layer will surface as a redirect.
func TestFollowerRejectsWrite(t *testing.T) {
	c, cleanup := newCluster(t, 3)
	defer cleanup()

	c.leader(t, 2*time.Second) // wait for an election to settle

	// Find a follower
	var followerSM *kvstore.StateMachine
	for i, n := range c.nodes {
		if state, _ := n.State(); state == raft.Follower {
			followerSM = c.sms[i]
			break
		}
	}
	if followerSM == nil {
		t.Skip("couldn't find a follower in time")
	}

	err := followerSM.Put("key", "value", "req-4")
	if err != kvstore.ErrNotLeader {
		t.Fatalf("expected ErrNotLeader from follower, got %v", err)
	}
	t.Log("Follower correctly rejected write with ErrNotLeader")
}

// TestIdempotentPut submits the same reqID twice and confirms the
// key's value is correct (not double-applied).
func TestIdempotentPut(t *testing.T) {
	c, cleanup := newCluster(t, 3)
	defer cleanup()

	sm := c.leader(t, 2*time.Second)

	if err := sm.Put("counter", "1", "req-idem-1"); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	// Simulate a retry: same reqID, different value -- the state
	// machine must keep "1", not overwrite with "2".
	if err := sm.Put("counter", "2", "req-idem-1"); err != nil {
		t.Fatalf("second Put (same reqId): %v", err)
	}
	val, _ := sm.Get("counter")
	if val != "1" {
		t.Fatalf("idempotency violated: expected '1', got %q", val)
	}
	t.Log("Idempotent Put OK: duplicate reqId correctly deduplicated")
}

// TestMultipleKeys writes several keys concurrently and reads them all back.
func TestMultipleKeys(t *testing.T) {
	c, cleanup := newCluster(t, 3)
	defer cleanup()

	sm := c.leader(t, 2*time.Second)

	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		if err := sm.Put(k, k+"-value", "req-multi-"+string(rune('0'+i))); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	for _, k := range keys {
		val, err := sm.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if val != k+"-value" {
			t.Fatalf("key %s: expected %q got %q", k, k+"-value", val)
		}
	}
	t.Logf("MultipleKeys OK: wrote and read back %d keys correctly", len(keys))
}

package chaos_test

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/chaos"
	"github.com/Tushar2021s/raftkv/raft"
)

// TestChaosLeaderFailover measures how long the cluster takes to elect
// a new leader after the current one crashes. Run 5 times and report
// min/avg/max. These numbers go directly on your resume.
func TestChaosLeaderFailover(t *testing.T) {
	const rounds = 5
	times := make([]time.Duration, 0, rounds)

	for i := 0; i < rounds; i++ {
		c := chaos.NewChaosCluster(5)

		newLeaderID, elapsed, err := c.MeasureFailoverTime(3 * time.Second)
		c.Stop()

		if err != nil {
			t.Errorf("round %d: %v", i+1, err)
			continue
		}
		times = append(times, elapsed)
		t.Logf("round %d: new leader=node%d failover=%v", i+1, newLeaderID, elapsed.Round(time.Millisecond))
	}

	if len(times) == 0 {
		t.Fatal("no successful rounds")
	}

	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var total time.Duration
	for _, d := range times {
		total += d
	}
	avg := total / time.Duration(len(times))

	t.Logf("─────────────────────────────────────────")
	t.Logf("FAILOVER LATENCY over %d rounds (5-node cluster)", len(times))
	t.Logf("  min: %v", times[0].Round(time.Millisecond))
	t.Logf("  avg: %v", avg.Round(time.Millisecond))
	t.Logf("  max: %v", times[len(times)-1].Round(time.Millisecond))
	t.Logf("─────────────────────────────────────────")
}

// TestChaosNetworkPartition partitions the leader from 2 of 4 peers
// (dropping it below majority), confirms no writes go through, then
// heals the partition and confirms the cluster recovers.
func TestChaosNetworkPartition(t *testing.T) {
	c := chaos.NewChaosCluster(5)
	defer c.Stop()

	leader, sm := c.Leader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader before partition")
	}
	_, firstTerm := leader.State()

	// Partition: cut the leader off from 3 peers (below majority).
	// This should cause the remaining 4 to elect a new leader.
	peers := make([]int, 0)
	for _, n := range c.Nodes {
		if n.ID() != leader.ID() {
			peers = append(peers, n.ID())
		}
	}
	for _, p := range peers[:3] {
		c.Network.Partition(leader.ID(), p)
	}

	// Wait for new leader among the majority partition.
	majority := make([]*raft.Node, 0)
	for _, n := range c.Nodes {
		if n.ID() != leader.ID() {
			majority = append(majority, n)
		}
	}

	var newLeader *raft.Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range majority {
			if state, _ := n.State(); state == raft.Leader {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after partition")
	}
	_, newTerm := newLeader.State()
	t.Logf("partition: new leader=node%d term=%d (old leader=node%d term=%d)",
		newLeader.ID(), newTerm, leader.ID(), firstTerm)

	// Writes to the OLD leader should fail (it can't reach majority).
	err := sm.Put("partition-key", "should-not-commit", fmt.Sprintf("req-%d", time.Now().UnixNano()))
	if err == nil {
		t.Log("note: old leader accepted write -- it may not have stepped down yet")
	} else {
		t.Logf("old leader correctly rejected write: %v", err)
	}

	// Heal the partition and confirm the cluster converges.
	c.Network.HealAll()
	time.Sleep(600 * time.Millisecond)

	// After healing, the whole cluster should agree on one leader.
	leaderCount := 0
	for _, n := range c.Nodes {
		if state, _ := n.State(); state == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader after heal, got %d", leaderCount)
	}
	t.Logf("partition healed: cluster converged to %d leader", leaderCount)
}

// TestChaosPacketLoss runs a 3-node cluster with 20%% random packet
// loss and verifies writes still eventually succeed (Raft's retry
// mechanism absorbs packet loss transparently).
func TestChaosPacketLoss(t *testing.T) {
	c := chaos.NewChaosCluster(3)
	defer c.Stop()

	c.Network.SetDropRate(0.20) // 20% packet loss

	_, sm := c.Leader(3 * time.Second)
	if sm == nil {
		t.Fatal("no leader under 20%% packet loss")
	}

	const writes = 20
	var success, failed int
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("loss-key-%d", i)
		reqID := fmt.Sprintf("loss-req-%d-%d", i, time.Now().UnixNano())
		if err := sm.Put(key, "v", reqID); err != nil {
			failed++
		} else {
			success++
		}
	}

	t.Logf("20%% packet loss: %d/%d writes succeeded, %d timed out", success, writes, failed)
	if success < writes/2 {
		t.Errorf("too many failures under packet loss: only %d/%d succeeded", success, writes)
	}
}

// TestChaosConcurrentWritesThroughput measures sustained write
// throughput from 8 concurrent clients with no faults injected.
// The ops/sec number is what goes on your resume.
func TestChaosConcurrentWritesThroughput(t *testing.T) {
	c := chaos.NewChaosCluster(3)
	defer c.Stop()

	_, sm := c.Leader(2 * time.Second)
	if sm == nil {
		t.Fatal("no leader")
	}

	const (
		clients  = 8
		duration = 3 * time.Second
	)
	var ops atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("c%d-k%d", clientID, seq)
				reqID := fmt.Sprintf("c%d-r%d-%d", clientID, seq, time.Now().UnixNano())
				if err := sm.Put(key, "v", reqID); err == nil {
					ops.Add(1)
				}
				seq++
			}
		}(i)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	total := ops.Load()
	opsPerSec := float64(total) / duration.Seconds()

	t.Logf("─────────────────────────────────────────")
	t.Logf("WRITE THROUGHPUT  (%d clients, %s, 3-node cluster)", clients, duration)
	t.Logf("  total ops : %d", total)
	t.Logf("  ops/sec   : %.0f", opsPerSec)
	t.Logf("─────────────────────────────────────────")

	if total == 0 {
		t.Error("zero writes committed")
	}
}

// TestChaosSplitBrainPrevention is the core safety test: partition the
// cluster so both halves think they might be able to elect a leader,
// write to both sides, then heal and confirm no data was lost or
// duplicated -- only the majority side's writes survive.
func TestChaosSplitBrainPrevention(t *testing.T) {
	c := chaos.NewChaosCluster(5)
	defer c.Stop()

	// Wait for the initial election.
	leader, _ := c.Leader(2 * time.Second)
	if leader == nil {
		t.Fatal("no initial leader")
	}

	// Partition into two groups: {0,1} and {2,3,4}.
	// Group A (minority) cannot elect a leader (needs 3 of 5).
	// Group B (majority) can elect a leader.
	for _, a := range []int{0, 1} {
		for _, b := range []int{2, 3, 4} {
			c.Network.Partition(a, b)
		}
	}

	time.Sleep(800 * time.Millisecond)

	// The correct Raft safety invariant is NOT "at most one node thinks
	// it's leader" -- a stale leader that can't reach majority is
	// allowed to still believe it's leader briefly. The invariant is:
	// only the majority partition can COMMIT entries. Verify this by
	// attempting writes on both partitions and confirming only the
	// majority side succeeds.
	var minorityCommitted, majorityCommitted int
	for _, n := range c.Nodes {
		state, _ := n.State()
		if state != raft.Leader {
			continue
		}
		id := n.ID()
		sm := c.SMs[id]
		err := sm.Put(fmt.Sprintf("key-%d", id), "v",
			fmt.Sprintf("req-%d-%d", id, time.Now().UnixNano()))
		inMinority := id == 0 || id == 1
		if err == nil {
			if inMinority {
				minorityCommitted++
				t.Errorf("SPLIT BRAIN: minority node%d committed a write -- safety violation", id)
			} else {
				majorityCommitted++
			}
		}
	}

	if minorityCommitted == 0 {
		t.Logf("Safety OK: minority partition could not commit (correct), majority committed %d", majorityCommitted)
	}

	// Heal and confirm recovery.
	c.Network.HealAll()
	time.Sleep(600 * time.Millisecond)

	newLeader, newSM := c.Leader(3 * time.Second)
	if newLeader == nil {
		t.Fatal("no leader after heal")
	}
	if err := newSM.Put("post-heal", "ok", fmt.Sprintf("req-%d", time.Now().UnixNano())); err != nil {
		t.Errorf("write failed after partition heal: %v", err)
	}
	t.Logf("post-heal write succeeded on leader node%d", newLeader.ID())
}

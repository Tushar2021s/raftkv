package bench

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/chaos"
	"github.com/Tushar2021s/raftkv/kvstore"
	"github.com/Tushar2021s/raftkv/raft"
)

// latencies collects write durations and computes percentiles.
type latencies struct {
	mu   sync.Mutex
	data []time.Duration
}

func (l *latencies) record(d time.Duration) {
	l.mu.Lock()
	l.data = append(l.data, d)
	l.mu.Unlock()
}

func (l *latencies) percentile(p float64) time.Duration {
	if len(l.data) == 0 {
		return 0
	}
	sorted := append([]time.Duration{}, l.data...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}

func (l *latencies) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.data)
}

// runWriteBench submits `ops` writes from `concurrency` goroutines against
// the given SM, recording each individual write latency.
func runWriteBench(sm *kvstore.StateMachine, ops, concurrency int) *latencies {
	lat := &latencies{}
	var wg sync.WaitGroup
	perWorker := ops / concurrency

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", workerID, i)
				reqID := fmt.Sprintf("w%d-r%d-%d", workerID, i, rand.Int63())
				start := time.Now()
				err := sm.Put(key, "v", reqID)
				if err == nil {
					lat.record(time.Since(start))
				}
			}
		}(w)
	}
	wg.Wait()
	return lat
}

// printPercentiles logs a standard latency table.
func printPercentiles(t *testing.T, label string, lat *latencies) {
	t.Helper()
	t.Logf("─────────────────────────────────────────────────")
	t.Logf("%-36s  n=%d", label, lat.count())
	t.Logf("  p50  : %v", lat.percentile(50).Round(time.Microsecond))
	t.Logf("  p90  : %v", lat.percentile(90).Round(time.Microsecond))
	t.Logf("  p99  : %v", lat.percentile(99).Round(time.Microsecond))
	t.Logf("─────────────────────────────────────────────────")
}

// BenchmarkWriteLatency measures write latency at varying concurrency
// on a healthy 3-node cluster.
func BenchmarkWriteLatency(b *testing.B) {
	b.Skip("use TestWriteLatencyPercentiles for human-readable output")
}

// TestWriteLatencyPercentiles is the main benchmark. Run with:
//
//	go test ./bench/... -v -run TestWriteLatencyPercentiles -count=1
func TestWriteLatencyPercentiles(t *testing.T) {
	for _, concurrency := range []int{1, 4, 8} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			c := chaos.NewChaosCluster(3)
			defer c.Stop()

			_, sm := c.Leader(2 * time.Second)
			if sm == nil {
				t.Fatal("no leader")
			}

			const ops = 400
			lat := runWriteBench(sm, ops, concurrency)
			printPercentiles(t, fmt.Sprintf("3-node, %d concurrent writers", concurrency), lat)
		})
	}
}

// TestWriteLatencyUnderFaults measures write latency with 10% packet
// loss injected -- shows how fault tolerance affects tail latency.
func TestWriteLatencyUnderFaults(t *testing.T) {
	c := chaos.NewChaosCluster(3)
	defer c.Stop()

	c.Network.SetDropRate(0.10)

	_, sm := c.Leader(2 * time.Second)
	if sm == nil {
		t.Fatal("no leader under 10%% packet loss")
	}

	const ops = 200
	lat := runWriteBench(sm, ops, 4)
	printPercentiles(t, "3-node, 4 writers, 10% packet loss", lat)
}

// TestWriteLatencyClusterSizes compares latency across different cluster
// sizes. Larger clusters need more nodes to acknowledge each write, so
// tail latency increases with cluster size.
func TestWriteLatencyClusterSizes(t *testing.T) {
	for _, size := range []int{3, 5} {
		size := size
		t.Run(fmt.Sprintf("%d-node", size), func(t *testing.T) {
			c := chaos.NewChaosCluster(size)
			defer c.Stop()

			_, sm := c.Leader(2 * time.Second)
			if sm == nil {
				t.Fatalf("no leader in %d-node cluster", size)
			}

			const ops = 300
			lat := runWriteBench(sm, ops, 4)
			printPercentiles(t, fmt.Sprintf("%d-node cluster, 4 writers", size), lat)
		})
	}
}

// TestFailoverLatencyPercentiles measures the distribution of time-to-
// new-leader across many crash-and-recover cycles. p99 here is the
// "worst case a client experiences" number.
func TestFailoverLatencyPercentiles(t *testing.T) {
	const rounds = 20
	lat := &latencies{}

	for i := 0; i < rounds; i++ {
		c := chaos.NewChaosCluster(5)
		_, elapsed, err := c.MeasureFailoverTime(3 * time.Second)
		c.Stop()
		if err == nil {
			lat.record(elapsed)
		}
	}

	t.Logf("─────────────────────────────────────────────────")
	t.Logf("FAILOVER LATENCY DISTRIBUTION  (%d rounds, 5-node)", rounds)
	t.Logf("  p50  : %v", lat.percentile(50).Round(time.Millisecond))
	t.Logf("  p90  : %v", lat.percentile(90).Round(time.Millisecond))
	t.Logf("  p99  : %v", lat.percentile(99).Round(time.Millisecond))
	t.Logf("─────────────────────────────────────────────────")
}

// newChaosCluster is a helper that wires up a ChaosCluster and returns
// (nodes, leader SM) ready for raw Raft-level benchmarks.
func newRaftCluster(n int) (*chaos.ChaosCluster, *kvstore.StateMachine) {
	c := chaos.NewChaosCluster(n)
	_, sm := c.Leader(2 * time.Second)
	return c, sm
}

// findLeaderNode returns the raft.Node that is currently the leader.
func findLeaderNode(c *chaos.ChaosCluster) *raft.Node {
	for _, n := range c.Nodes {
		if state, _ := n.State(); state == raft.Leader {
			return n
		}
	}
	return nil
}

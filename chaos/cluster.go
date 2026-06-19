package chaos

import (
	"fmt"
	"time"

	"github.com/Tushar2021s/raftkv/kvstore"
	"github.com/Tushar2021s/raftkv/raft"
)

// ChaosCluster is a self-contained in-process cluster wired onto a
// SimNetwork so tests can inject faults at will.
type ChaosCluster struct {
	Nodes   []*raft.Node
	SMs     []*kvstore.StateMachine
	Network *SimNetwork
	size    int
}

func NewChaosCluster(n int) *ChaosCluster {
	net := NewSimNetwork()
	nodes := make([]*raft.Node, n)
	sms := make([]*kvstore.StateMachine, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 1024)
		node := raft.NewNode(i, peers, net.TransportFor(i), raft.NewMemoryPersister(), applyCh)
		sm := kvstore.NewStateMachine(node, applyCh)
		nodes[i] = node
		sms[i] = sm
		net.Register(i, node)
	}
	for _, node := range nodes {
		node.Run()
	}
	return &ChaosCluster{Nodes: nodes, SMs: sms, Network: net, size: n}
}

func (c *ChaosCluster) Stop() {
	for _, n := range c.Nodes {
		n.Stop()
	}
}

// Leader returns the current leader node and its SM, polling until
// timeout. Returns nil if no leader is elected in time.
func (c *ChaosCluster) Leader(timeout time.Duration) (*raft.Node, *kvstore.StateMachine) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range c.Nodes {
			if state, _ := n.State(); state == raft.Leader {
				return n, c.SMs[i]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, nil
}

// Put submits a write to whichever node is currently the leader.
// Returns an error if no leader is found or the write times out.
func (c *ChaosCluster) Put(key, value string) error {
	_, sm := c.Leader(2 * time.Second)
	if sm == nil {
		return fmt.Errorf("no leader available")
	}
	reqID := fmt.Sprintf("%s-%d", key, time.Now().UnixNano())
	return sm.Put(key, value, reqID)
}

// MeasureFailoverTime crashes the current leader and measures how long
// until a new one is elected. This is the key latency number for your resume.
func (c *ChaosCluster) MeasureFailoverTime(timeout time.Duration) (leaderID int, elapsed time.Duration, err error) {
	leader, _ := c.Leader(2 * time.Second)
	if leader == nil {
		return -1, 0, fmt.Errorf("no leader before failover")
	}
	oldID := leader.ID()

	// Kill the leader: stop its goroutines AND isolate it from the network.
	leader.Stop()
	c.Network.IsolateNode(oldID)

	start := time.Now()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n.ID() == oldID {
				continue
			}
			if state, _ := n.State(); state == raft.Leader {
				return n.ID(), time.Since(start), nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1, time.Since(start), fmt.Errorf("no new leader within %s", timeout)
}

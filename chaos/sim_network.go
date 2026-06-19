package chaos

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
)

// SimNetwork is a drop-in replacement for transport.LocalTransport that
// adds fault injection: messages can be dropped, delayed, or entire
// node pairs can be partitioned from each other.
//
// Every raft.Node in a chaos test uses a SimNetwork instead of the
// real HTTP transport, so failure scenarios are fully deterministic,
// instant, and require no actual network or clock tricks.
type SimNetwork struct {
	mu sync.RWMutex

	nodes map[int]*raft.Node

	// dropRate is the probability [0,1] that any single RPC is silently
	// dropped, simulating packet loss or an overloaded node.
	dropRate float64

	// partitioned is a set of (from,to) pairs where RPCs are blocked.
	// partitioned[a][b] = true means a cannot reach b.
	partitioned map[int]map[int]bool

	// delayRange adds artificial latency to every delivered RPC.
	delayMin, delayMax time.Duration
}

func NewSimNetwork() *SimNetwork {
	return &SimNetwork{
		nodes:       make(map[int]*raft.Node),
		partitioned: make(map[int]map[int]bool),
	}
}

func (n *SimNetwork) Register(id int, node *raft.Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

func (n *SimNetwork) Unregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, id)
}

// SetDropRate sets the probability that any RPC is silently dropped.
// 0 = no drops, 1 = all dropped.
func (n *SimNetwork) SetDropRate(rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = rate
}

// Partition blocks all RPCs between nodeA and nodeB in both directions.
func (n *SimNetwork) Partition(a, b int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.partitioned[a] == nil {
		n.partitioned[a] = make(map[int]bool)
	}
	if n.partitioned[b] == nil {
		n.partitioned[b] = make(map[int]bool)
	}
	n.partitioned[a][b] = true
	n.partitioned[b][a] = true
}

// Heal removes the partition between nodeA and nodeB.
func (n *SimNetwork) Heal(a, b int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.partitioned[a] != nil {
		delete(n.partitioned[a], b)
	}
	if n.partitioned[b] != nil {
		delete(n.partitioned[b], a)
	}
}

// HealAll removes every active partition.
func (n *SimNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = make(map[int]map[int]bool)
}

// IsolateNode partitions one node from every other registered node.
func (n *SimNetwork) IsolateNode(id int) {
	n.mu.Lock()
	others := make([]int, 0, len(n.nodes))
	for k := range n.nodes {
		if k != id {
			others = append(others, k)
		}
	}
	n.mu.Unlock()
	for _, other := range others {
		n.Partition(id, other)
	}
}

// SetDelay adds artificial latency [min, max] to every delivered RPC.
func (n *SimNetwork) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delayMin = min
	n.delayMax = max
}

func (n *SimNetwork) isBlocked(from, to int) bool {
	if n.partitioned[from] != nil && n.partitioned[from][to] {
		return true
	}
	if rand.Float64() < n.dropRate {
		return true
	}
	return false
}

func (n *SimNetwork) maybeDelay() {
	if n.delayMax == 0 {
		return
	}
	span := n.delayMax - n.delayMin
	delay := n.delayMin + time.Duration(rand.Int63n(int64(span+1)))
	time.Sleep(delay)
}

func (n *SimNetwork) getPeer(id int) (*raft.Node, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	node, ok := n.nodes[id]
	return node, ok
}

func (n *SimNetwork) SendRequestVote(peerID int, selfID int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	n.mu.RLock()
	blocked := n.isBlocked(selfID, peerID)
	n.mu.RUnlock()
	if blocked {
		return nil, errBlocked
	}
	n.maybeDelay()
	peer, ok := n.getPeer(peerID)
	if !ok {
		return nil, errBlocked
	}
	return peer.HandleRequestVote(args), nil
}

func (n *SimNetwork) SendAppendEntries(peerID int, selfID int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	n.mu.RLock()
	blocked := n.isBlocked(selfID, peerID)
	n.mu.RUnlock()
	if blocked {
		return nil, errBlocked
	}
	n.maybeDelay()
	peer, ok := n.getPeer(peerID)
	if !ok {
		return nil, errBlocked
	}
	return peer.HandleAppendEntries(args), nil
}

func (n *SimNetwork) SendInstallSnapshot(peerID int, selfID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	n.mu.RLock()
	blocked := n.isBlocked(selfID, peerID)
	n.mu.RUnlock()
	if blocked {
		return nil, errBlocked
	}
	n.maybeDelay()
	peer, ok := n.getPeer(peerID)
	if !ok {
		return nil, errBlocked
	}
	return peer.HandleInstallSnapshot(args), nil
}

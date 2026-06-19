package transport

import (
	"errors"
	"sync"

	"github.com/Tushar2021s/raftkv/raft"
)

// LocalTransport routes RPCs by calling a peer's handler methods
// directly, in-process — no sockets, no serialization. It exists so
// raft.Node can be tested without standing up real network
// connections, and it's the direct ancestor of the fault-injecting
// SimNetwork we'll build for chaos testing in stage 8: same underlying
// idea (an in-memory stand-in for the network), just without
// deliberately breaking anything yet.
type LocalTransport struct {
	mu    sync.RWMutex
	peers map[int]*raft.Node
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{peers: make(map[int]*raft.Node)}
}

// Register makes a node reachable by its ID. Every node in a test
// cluster must be registered on the same LocalTransport instance.
func (t *LocalTransport) Register(id int, node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = node
}

// Unregister makes a node unreachable, simulating a crash: any peer
// that tries to call it gets an error back, exactly as if the process
// had actually died. Pair this with node.Stop() (which halts the
// node's own background goroutines) to fully simulate a crash from
// both sides.
func (t *LocalTransport) Unregister(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

func (t *LocalTransport) get(id int) (*raft.Node, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.peers[id]
	if !ok {
		return nil, errors.New("transport: peer unreachable")
	}
	return n, nil
}

func (t *LocalTransport) SendRequestVote(peerID int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.HandleRequestVote(args), nil
}

func (t *LocalTransport) SendAppendEntries(peerID int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.HandleAppendEntries(args), nil
}

func (t *LocalTransport) SendInstallSnapshot(peerID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	if _, err := t.get(peerID); err != nil {
		return nil, err
	}
	return nil, errors.New("transport: InstallSnapshot not implemented until the snapshotting stage")
}

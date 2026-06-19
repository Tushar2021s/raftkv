package transport

import (
	"errors"
	"sync"

	"github.com/Tushar2021s/raftkv/raft"
)

// LocalTransport routes RPCs by calling a peer's handler methods
// directly, in-process — no sockets, no serialisation. Used for all
// tests. InstallSnapshot is now fully wired so snapshot tests work.
type LocalTransport struct {
	mu    sync.RWMutex
	peers map[int]*raft.Node
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{peers: make(map[int]*raft.Node)}
}

func (t *LocalTransport) Register(id int, node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = node
}

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

// SendInstallSnapshot is now fully implemented — it routes the RPC
// directly to the peer's HandleInstallSnapshot handler. This is what
// lets snapshot tests work without real HTTP.
func (t *LocalTransport) SendInstallSnapshot(peerID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.HandleInstallSnapshot(args), nil
}

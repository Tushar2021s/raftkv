package transport

import (
	"errors"
	"sync"

	"github.com/Tushar2021s/raftkv/raft"
)

// LocalTransport routes all RPCs (including membership changes) by
// calling peer handler methods directly, in-process. Implements both
// raft.Transport and raft.MembershipTransport.
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

func (t *LocalTransport) SendInstallSnapshot(peerID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.HandleInstallSnapshot(args), nil
}

func (t *LocalTransport) SendAddServer(peerID int, args *raft.AddServerArgs) (*raft.AddServerReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.AddServer(args.NewPeerID), nil
}

func (t *LocalTransport) SendRemoveServer(peerID int, args *raft.RemoveServerArgs) (*raft.RemoveServerReply, error) {
	peer, err := t.get(peerID)
	if err != nil {
		return nil, err
	}
	return peer.RemoveServer(args.PeerID), nil
}

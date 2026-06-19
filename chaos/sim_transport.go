package chaos

import (
	"errors"

	"github.com/Tushar2021s/raftkv/raft"
)

var errBlocked = errors.New("chaos: RPC blocked by fault injection")

// SimTransport wraps SimNetwork and implements raft.Transport for a
// specific node. Each node gets its own SimTransport so the network
// knows the sender's ID for partition checks.
type SimTransport struct {
	selfID  int
	network *SimNetwork
}

func (n *SimNetwork) TransportFor(selfID int) *SimTransport {
	return &SimTransport{selfID: selfID, network: n}
}

func (t *SimTransport) SendRequestVote(peerID int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return t.network.SendRequestVote(peerID, t.selfID, args)
}

func (t *SimTransport) SendAppendEntries(peerID int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return t.network.SendAppendEntries(peerID, t.selfID, args)
}

func (t *SimTransport) SendInstallSnapshot(peerID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return t.network.SendInstallSnapshot(peerID, t.selfID, args)
}

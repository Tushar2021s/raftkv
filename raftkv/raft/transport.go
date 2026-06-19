package raft

// Transport is the only way a Node talks to its peers. Raft's
// consensus logic is written entirely against this interface and
// never touches a socket directly. That separation is what lets us
// swap in two completely different implementations later without
// changing a single line of consensus code:
//
//   - transport.HTTPTransport: real node-to-node calls over the
//     network, used when actually running a cluster.
//   - transport.SimNetwork: a fully in-process, deterministic network
//     that can drop, delay, or partition messages on command. Used by
//     the chaos-testing harness to reproduce failure scenarios
//     instantly and reliably instead of fighting real timing.
type Transport interface {
	SendRequestVote(peerID int, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peerID int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(peerID int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Persister is how a Node's durable state survives a crash and
// restart. Per Figure 2 of the Raft paper, currentTerm, votedFor, and
// log must be persisted before a node responds to any RPC that
// changed them -- this is the rule that makes the "a node can crash
// and come back without violating safety" guarantee hold.
type Persister interface {
	SaveState(state []byte) error
	ReadState() ([]byte, error)
	SaveSnapshot(snapshot []byte) error
	ReadSnapshot() ([]byte, error)
}

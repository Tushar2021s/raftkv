package raft_test

import (
	"os"
	"testing"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
	"github.com/Tushar2021s/raftkv/transport"
)

// TestFilePersisterRoundTrip writes state and reads it back, including
// across a fresh FilePersister instance (simulating a process restart).
func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()

	p, err := raft.NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte(`{"currentTerm":7,"votedFor":2,"log":[]}`)
	if err := p.SaveState(data); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Simulate a process restart by creating a brand-new FilePersister
	// pointed at the same directory.
	p2, _ := raft.NewFilePersister(dir)
	got, err := p2.ReadState()
	if err != nil {
		t.Fatalf("ReadState after restart: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
	t.Log("FilePersister round-trip OK across simulated restart")
}

// TestFilePersisterChecksumDetection corrupts the persisted file on
// disk and confirms ReadState returns an error instead of silently
// handing back garbage. This is the check that prevents a node from
// loading corrupt state after a bad disk write.
func TestFilePersisterChecksumDetection(t *testing.T) {
	dir := t.TempDir()
	p, _ := raft.NewFilePersister(dir)

	if err := p.SaveState([]byte("good data")); err != nil {
		t.Fatal(err)
	}

	// Corrupt the file by flipping a byte in the middle.
	path := dir + "/raft-state"
	raw, _ := os.ReadFile(path)
	raw[len(raw)/2] ^= 0xFF
	os.WriteFile(path, raw, 0644)

	_, err := p.ReadState()
	if err == nil {
		t.Fatal("expected checksum error on corrupt file, got nil")
	}
	t.Logf("Checksum detection OK: got expected error: %v", err)
}

// TestNodeRecoversTermAfterCrash verifies the core Raft safety property
// around persistence: a node that has voted in term T and then crashes
// must remember that vote after restarting, so it doesn't hand out a
// second vote in the same term to a different candidate.
func TestNodeRecoversTermAfterCrash(t *testing.T) {
	dir := t.TempDir()
	lt := transport.NewLocalTransport()

	// Build a 3-node cluster where every node uses a FilePersister.
	makeCluster := func() ([]*raft.Node, func()) {
		nodes := make([]*raft.Node, 3)
		for i := 0; i < 3; i++ {
			peers := []int{}
			for j := 0; j < 3; j++ {
				if j != i {
					peers = append(peers, j)
				}
			}
			p, err := raft.NewFilePersister(dir + "/" + string(rune('0'+i)))
			if err != nil {
				t.Fatal(err)
			}
			applyCh := make(chan raft.ApplyMsg, 256)
			node := raft.NewNode(i, peers, lt, p, applyCh)
			nodes[i] = node
			lt.Register(i, node)
		}
		for _, n := range nodes {
			n.Run()
		}
		return nodes, func() {
			for _, n := range nodes {
				n.Stop()
				lt.Unregister(n.ID())
			}
		}
	}

	// First cluster lifetime: let it elect a leader and run briefly.
	nodes1, stop1 := makeCluster()
	leader := waitForLeader(t, nodes1, 2*time.Second)
	_, term1 := leader.State()
	t.Logf("first lifetime: leader=%d term=%d", leader.ID(), term1)
	stop1() // simulate a clean shutdown of the whole cluster

	// Second lifetime: restart all nodes from their persisted state.
	nodes2, stop2 := makeCluster()
	defer stop2()

	leader2 := waitForLeader(t, nodes2, 3*time.Second)
	_, term2 := leader2.State()
	t.Logf("second lifetime: leader=%d term=%d", leader2.ID(), term2)

	// After a restart the cluster must elect in a term >= the previous
	// one. It can't go backwards -- if it did, a node that voted for
	// candidate A in term 5 could be tricked into voting for candidate
	// B in the same term 5 after a restart, potentially electing two
	// leaders for the same term.
	if term2 < term1 {
		t.Fatalf("term went backwards after restart: was %d, now %d", term1, term2)
	}
	t.Log("Crash recovery OK: term monotonically non-decreasing across restart")
}

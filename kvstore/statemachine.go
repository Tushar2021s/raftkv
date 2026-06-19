package kvstore

import (
	"errors"
	"sync"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
)

var (
	ErrNotLeader  = errors.New("kvstore: not the leader, redirect to leader")
	ErrKeyMissing = errors.New("kvstore: key not found")
	ErrTimeout    = errors.New("kvstore: timed out waiting for commit")
)

const (
	applyTimeout = 3 * time.Second

	// snapshotThreshold is how many log entries can accumulate above
	// the last snapshot before we compact. Kept at 100 in production;
	// tests use a lower value via TakeSnapshotIfNeeded directly.
	snapshotThreshold = 100
)

type pendingWrite struct {
	done chan struct{}
	err  error
}

// StateMachine is the KV store layer that sits on top of a raft.Node.
// It applies committed commands, deduplicates retried writes, and
// automatically triggers log compaction once the log grows past
// snapshotThreshold entries since the last snapshot.
type StateMachine struct {
	mu   sync.RWMutex
	data map[string]string
	dedup   map[string]string
	pending map[int]*pendingWrite

	node    *raft.Node
	applyCh <-chan raft.ApplyMsg

	// snapshot bookkeeping
	lastSnapshot int // logical index of last snapshot we triggered
}

func NewStateMachine(node *raft.Node, applyCh <-chan raft.ApplyMsg) *StateMachine {
	sm := &StateMachine{
		data:    make(map[string]string),
		dedup:   make(map[string]string),
		pending: make(map[int]*pendingWrite),
		node:    node,
		applyCh: applyCh,
	}
	go sm.applyLoop()
	return sm
}

// applyLoop drains applyCh. It is the *only* goroutine that writes
// sm.data. Snapshot messages (SnapshotValid=true) replace the entire
// state; command messages execute one at a time in order.
//
// Auto-snapshotting: after applying a command, if the distance between
// the current applied index and the last snapshot exceeds the threshold,
// we call node.TakeSnapshot synchronously. TakeSnapshot acquires n.mu
// and trims the log — this is safe to call from outside the Raft
// package because we're not holding any locks ourselves at this point.
func (sm *StateMachine) applyLoop() {
	for msg := range sm.applyCh {
		if msg.SnapshotValid {
			sm.installSnapshot(msg.Snapshot)
			// Update our own lastSnapshot tracking so we don't
			// immediately re-trigger a snapshot for the same index.
			sm.mu.Lock()
			if msg.SnapshotIndex > sm.lastSnapshot {
				sm.lastSnapshot = msg.SnapshotIndex
			}
			sm.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}

		cmd, err := decodeCommand(msg.Command)
		if err != nil {
			sm.signalPending(msg.CommandIndex, err)
			continue
		}

		sm.mu.Lock()
		if _, seen := sm.dedup[cmd.ReqID]; seen {
			sm.mu.Unlock()
			sm.signalPending(msg.CommandIndex, nil)
			continue
		}
		switch cmd.Op {
		case OpPut:
			sm.data[cmd.Key] = cmd.Value
			sm.dedup[cmd.ReqID] = cmd.Value
		case OpDelete:
			delete(sm.data, cmd.Key)
			sm.dedup[cmd.ReqID] = ""
		}
		appliedIndex := msg.CommandIndex
		lastSnap := sm.lastSnapshot
		sm.mu.Unlock()

		// Signal the waiting Put/Delete caller BEFORE taking the
		// snapshot so the client's latency doesn't include compaction.
		sm.signalPending(appliedIndex, nil)

		// Trigger compaction if we've accumulated enough entries.
		// We call this synchronously so the snapshot is fully written
		// before the next replication round — this is what prevents the
		// race where replicateToPeer tries to send a snapshot that isn't
		// on disk yet.
		if appliedIndex-lastSnap >= snapshotThreshold {
			sm.takeSnapshot(appliedIndex)
		}
	}
}

// takeSnapshot serialises the current state and hands it to Raft.
func (sm *StateMachine) takeSnapshot(index int) {
	data, err := sm.Snapshot()
	if err != nil {
		return
	}
	sm.mu.Lock()
	sm.lastSnapshot = index
	sm.mu.Unlock()
	sm.node.TakeSnapshot(index, data)
}

// TakeSnapshotAt is the test-accessible entry point for forcing a
// snapshot at a specific index (used by snapshot tests that want to
// control the threshold).
func (sm *StateMachine) TakeSnapshotAt(index int) {
	sm.takeSnapshot(index)
}

func (sm *StateMachine) signalPending(index int, err error) {
	sm.mu.Lock()
	pw, ok := sm.pending[index]
	if ok {
		delete(sm.pending, index)
	}
	sm.mu.Unlock()
	if ok {
		pw.err = err
		close(pw.done)
	}
}

func (sm *StateMachine) Get(key string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.data[key]
	if !ok {
		return "", ErrKeyMissing
	}
	return v, nil
}

func (sm *StateMachine) Put(key, value, reqID string) error {
	return sm.writeOp(OpPut, key, value, reqID)
}

func (sm *StateMachine) Delete(key, reqID string) error {
	return sm.writeOp(OpDelete, key, "", reqID)
}

func (sm *StateMachine) writeOp(op OpType, key, value, reqID string) error {
	data, err := encodeCommand(Command{Op: op, Key: key, Value: value, ReqID: reqID})
	if err != nil {
		return err
	}

	idx, _, isLeader := sm.node.Submit(data)
	if !isLeader {
		return ErrNotLeader
	}

	pw := &pendingWrite{done: make(chan struct{})}
	sm.mu.Lock()
	sm.pending[idx] = pw
	sm.mu.Unlock()

	select {
	case <-pw.done:
		return pw.err
	case <-time.After(applyTimeout):
		sm.mu.Lock()
		delete(sm.pending, idx)
		sm.mu.Unlock()
		return ErrTimeout
	}
}

// Snapshot serialises the current KV state for Raft log compaction.
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return marshalSnap(sm.data)
}

func (sm *StateMachine) installSnapshot(data []byte) {
	m, err := unmarshalSnap(data)
	if err != nil {
		return
	}
	sm.mu.Lock()
	sm.data = m
	sm.mu.Unlock()
}

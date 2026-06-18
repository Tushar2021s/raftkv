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

const applyTimeout = 3 * time.Second

// pendingWrite is a write that has been submitted to Raft but not yet
// committed. The state machine signals it (by closing done) the moment
// its log index is applied.
type pendingWrite struct {
	done chan struct{}
	err  error
}

// StateMachine is the KV store layer that sits directly on top of a
// raft.Node. It:
//   - receives committed log entries via applyCh and executes them
//     against an in-memory map
//   - deduplicates retried client requests using reqID
//   - tracks pending writes so Put/Delete can block until their entry
//     is committed rather than returning immediately
type StateMachine struct {
	mu   sync.RWMutex
	data map[string]string

	// dedup stores the last result for each client reqID so retried
	// requests don't get applied twice (important once we add crash
	// recovery in stage 5 -- a client that timed out and retried might
	// see its first request committed on restart).
	dedup map[string]string

	// pending maps log index -> a waiting Put/Delete caller.
	pending map[int]*pendingWrite

	node    *raft.Node
	applyCh <-chan raft.ApplyMsg
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

// applyLoop drains applyCh forever. Every message is a committed log
// entry (CommandValid=true) or a snapshot (SnapshotValid=true, wired
// in stage 6). This goroutine is the *only* writer to sm.data, which
// keeps the locking simple: reads lock sm.mu for reading, this loop
// holds a write lock only while mutating the map.
func (sm *StateMachine) applyLoop() {
	for msg := range sm.applyCh {
		if msg.SnapshotValid {
			sm.installSnapshot(msg.Snapshot)
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

		// Deduplication: if we've seen this reqID before, don't apply
		// again -- just signal the waiter with the cached result.
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

		sm.mu.Unlock()
		sm.signalPending(msg.CommandIndex, nil)
	}
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

// Get reads a key directly from the local state machine. Only correct
// on the leader (a follower might be behind); the HTTP handler enforces
// this before calling here.
func (sm *StateMachine) Get(key string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.data[key]
	if !ok {
		return "", ErrKeyMissing
	}
	return v, nil
}

// Put submits a PUT to Raft and blocks until the entry is committed
// and applied, or until applyTimeout.
func (sm *StateMachine) Put(key, value, reqID string) error {
	return sm.writeOp(OpPut, key, value, reqID)
}

// Delete submits a DELETE to Raft and blocks until committed/applied.
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

	// Register the pending waiter *after* Submit returns the index, but
	// before the applyLoop might signal it. There's no race here because
	// signalPending checks for the entry and the channel is buffered.
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

// Snapshot serialises the current state for Raft snapshotting (stage 6).
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

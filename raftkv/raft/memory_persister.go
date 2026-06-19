package raft

import "sync"

// MemoryPersister is a Persister that keeps everything in RAM. It's a
// stand-in for the real, disk-backed persister we'll build in stage 5
// -- and it'll stay useful afterward too, since tests generally
// shouldn't depend on real disk I/O just to verify consensus logic.
type MemoryPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (p *MemoryPersister) SaveState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte{}, state...)
	return nil
}

func (p *MemoryPersister) ReadState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte{}, p.state...), nil
}

func (p *MemoryPersister) SaveSnapshot(snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = append([]byte{}, snapshot...)
	return nil
}

func (p *MemoryPersister) ReadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte{}, p.snapshot...), nil
}

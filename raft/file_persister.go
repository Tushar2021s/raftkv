package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister is the production Persister implementation. It writes
// every state change to disk and calls fsync before returning, which
// is the guarantee Raft needs: a node must never tell a peer something
// it hasn't durably committed to storage first.
//
// File layout under dir/:
//   raft-state        current Raft state (term, votedFor, log)
//   raft-state.tmp    atomic write staging file
//   raft-snapshot     latest snapshot
//   raft-snapshot.tmp atomic write staging file
//
// Writes use a write-to-tmp-then-rename pattern so a crash mid-write
// never leaves a corrupt file: rename() is atomic on POSIX filesystems,
// so the reader always sees either the old complete file or the new
// complete file, never a half-written one.
//
// Each file is prefixed with a CRC32 checksum of the payload so we
// can detect bit-rot or a truncated write on read.
type FilePersister struct {
	mu  sync.Mutex
	dir string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FilePersister{dir: dir}, nil
}

func (p *FilePersister) SaveState(state []byte) error {
	return p.atomicWrite("raft-state", state)
}

func (p *FilePersister) ReadState() ([]byte, error) {
	return p.readChecked("raft-state")
}

func (p *FilePersister) SaveSnapshot(snapshot []byte) error {
	return p.atomicWrite("raft-snapshot", snapshot)
}

func (p *FilePersister) ReadSnapshot() ([]byte, error) {
	return p.readChecked("raft-snapshot")
}

// atomicWrite writes data to a .tmp file, fsyncs it, then renames it
// over the target. This guarantees we never end up with a partial
// write visible to a reader.
func (p *FilePersister) atomicWrite(name string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp := filepath.Join(p.dir, name+".tmp")
	final := filepath.Join(p.dir, name)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// Write a 4-byte CRC32 checksum prefix so we can detect corruption.
	checksum := crc32.ChecksumIEEE(data)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], checksum)

	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	// fsync before rename: ensures the data is on disk, not just in the
	// OS page cache. Without this, a power failure between the rename
	// and the actual disk flush could leave us with a renamed but empty
	// (or zero-length) file on next boot.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, final)
}

func (p *FilePersister) readChecked(name string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	path := filepath.Join(p.dir, name)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // fresh node, no state yet
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < 4 {
		return nil, errors.New("persister: file too short, likely corrupt")
	}

	stored := binary.BigEndian.Uint32(raw[:4])
	data := raw[4:]
	computed := crc32.ChecksumIEEE(data)
	if stored != computed {
		return nil, errors.New("persister: checksum mismatch, file is corrupt")
	}
	return data, nil
}

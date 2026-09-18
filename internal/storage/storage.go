// Package storage provides concrete Storage implementations for Raft nodes.
// Two implementations are provided:
//
//  1. MemoryStorage — in-memory, non-durable. For tests and single-process use.
//  2. FileStorage   — durable, file-backed. Suitable for real deployments.
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
)

// ─────────────────────────────────────────────────────────────────────────────
// MemoryStorage
// ─────────────────────────────────────────────────────────────────────────────

// MemoryStorage is a non-durable, thread-safe Storage implementation.
// Use it in tests and single-process demos where crash recovery is not needed.
type MemoryStorage struct {
	mu       sync.RWMutex
	state    *raft.PersistentState
	snapshot *raft.Snapshot
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (m *MemoryStorage) SaveState(state raft.PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deep-copy the log slice to prevent aliasing.
	cp := raft.PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         make([]raft.LogEntry, len(state.Log)),
	}
	copy(cp.Log, state.Log)
	m.state = &cp
	return nil
}

func (m *MemoryStorage) LoadState() (raft.PersistentState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return raft.PersistentState{}, fmt.Errorf("no state")
	}
	cp := raft.PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         make([]raft.LogEntry, len(m.state.Log)),
	}
	copy(cp.Log, m.state.Log)
	return cp, nil
}

func (m *MemoryStorage) SaveSnapshot(snap raft.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := raft.Snapshot{
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              make([]byte, len(snap.Data)),
	}
	copy(cp.Data, snap.Data)
	m.snapshot = &cp
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (*raft.Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.snapshot == nil {
		return nil, nil
	}
	cp := &raft.Snapshot{
		LastIncludedIndex: m.snapshot.LastIncludedIndex,
		LastIncludedTerm:  m.snapshot.LastIncludedTerm,
		Data:              make([]byte, len(m.snapshot.Data)),
	}
	copy(cp.Data, m.snapshot.Data)
	return cp, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// FileStorage
// ─────────────────────────────────────────────────────────────────────────────

const (
	stateFile    = "raft-state.json"
	snapshotFile = "raft-snapshot.json"
)

// FileStorage is a durable, file-backed Storage implementation.
// It uses atomic write (write to temp file + rename) to prevent corruption
// on partial writes or crashes.
//
// Directory structure under dir/:
//
//	raft-state.json    — PersistentState (term, vote, log)
//	raft-snapshot.json — Latest snapshot (if any)
type FileStorage struct {
	mu  sync.Mutex
	dir string
}

// NewFileStorage creates a FileStorage that persists data in dir.
// The directory is created if it does not exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	return &FileStorage{dir: dir}, nil
}

func (f *FileStorage) SaveState(state raft.PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return writeJSON(filepath.Join(f.dir, stateFile), state)
}

func (f *FileStorage) LoadState() (raft.PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var state raft.PersistentState
	if err := readJSON(filepath.Join(f.dir, stateFile), &state); err != nil {
		return raft.PersistentState{}, err
	}
	return state, nil
}

func (f *FileStorage) SaveSnapshot(snap raft.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return writeJSON(filepath.Join(f.dir, snapshotFile), snap)
}

func (f *FileStorage) LoadSnapshot() (*raft.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var snap raft.Snapshot
	if err := readJSON(filepath.Join(f.dir, snapshotFile), &snap); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return &snap, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// File I/O helpers
// ─────────────────────────────────────────────────────────────────────────────

// writeJSON atomically writes v as JSON to path.
// Uses write-to-temp + rename to guarantee all-or-nothing semantics.
func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// readJSON reads JSON from path into v.
func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// Package kvstore implements a linearizable key-value store built on top of Raft.
//
// Architecture:
//
//	Client → KVServer (Propose) → Raft.Propose → [replicated] → ApplyMsg → KVServer.apply
//
// Linearizability is achieved by:
//  1. Routing all writes through the Raft leader.
//  2. Applying commands only after they are committed by Raft majority.
//  3. Using client session IDs + sequence numbers to deduplicate retried commands.
//
// Supported operations: GET, PUT, DELETE.
package kvstore

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
)

// OpType identifies the kind of KV operation.
type OpType string

const (
	OpGet    OpType = "GET"
	OpPut    OpType = "PUT"
	OpDelete OpType = "DELETE"
)

// Op is a single KV store operation encoded in a Raft log entry.
type Op struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"` // non-empty for PUT
	ClientID string `json:"clientId"`        // for deduplication
	SeqNum   uint64 `json:"seqNum"`          // monotonically increasing per client
}

// Result is returned to the caller of Get/Put/Delete after the operation is applied.
type Result struct {
	Value string // populated for GET
	Err   error
}

// pendingOp tracks a proposal that's in flight through Raft.
type pendingOp struct {
	index  uint64
	term   uint64
	respCh chan Result
}

// KVServer is a single-node KV server backed by Raft consensus.
// Clients interact through the Get/Put/Delete methods.
// Multiple KVServer instances (one per cluster node) form a fault-tolerant store.
type KVServer struct {
	mu sync.Mutex

	node    *raft.Node
	applyCh <-chan raft.ApplyMsg
	logger  *slog.Logger

	// State machine.
	data map[string]string // current KV state

	// Pending client operations waiting for commit.
	pending map[uint64]*pendingOp // log index → pending op

	// Deduplication: last applied sequence number per clientID.
	lastSeq map[string]uint64
	lastRes map[string]Result // last result per clientID (for idempotent replay)

	stopCh chan struct{}
}

// NewKVServer creates a KVServer. Call Start() to begin processing.
func NewKVServer(node *raft.Node, applyCh <-chan raft.ApplyMsg) *KVServer {
	return &KVServer{
		node:    node,
		applyCh: applyCh,
		logger:  slog.Default().With("kvserver", node.ID()),
		data:    make(map[string]string),
		pending: make(map[uint64]*pendingOp),
		lastSeq: make(map[string]uint64),
		lastRes: make(map[string]Result),
		stopCh:  make(chan struct{}),
	}
}

// Start launches the apply goroutine.
func (s *KVServer) Start() {
	go s.applyLoop()
}

// Stop shuts down the server.
func (s *KVServer) Stop() {
	close(s.stopCh)
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

const proposalTimeout = 5 * time.Second

// Get retrieves the value for key. Returns "" if the key doesn't exist.
// Must be called on the leader; redirects are signalled via ErrNotLeader.
func (s *KVServer) Get(clientID string, seqNum uint64, key string) (string, error) {
	op := Op{Type: OpGet, Key: key, ClientID: clientID, SeqNum: seqNum}
	res, err := s.propose(op)
	if err != nil {
		return "", err
	}
	return res.Value, res.Err
}

// Put sets key=value. Idempotent for the same (clientID, seqNum).
func (s *KVServer) Put(clientID string, seqNum uint64, key, value string) error {
	op := Op{Type: OpPut, Key: key, Value: value, ClientID: clientID, SeqNum: seqNum}
	_, err := s.propose(op)
	return err
}

// Delete removes key from the store.
func (s *KVServer) Delete(clientID string, seqNum uint64, key string) error {
	op := Op{Type: OpDelete, Key: key, ClientID: clientID, SeqNum: seqNum}
	_, err := s.propose(op)
	return err
}

// Snapshot returns the current state as opaque bytes (for Raft snapshotting).
func (s *KVServer) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type snap struct {
		Data    map[string]string `json:"data"`
		LastSeq map[string]uint64 `json:"lastSeq"`
		LastRes map[string]Result `json:"lastRes"`
	}
	return json.Marshal(snap{
		Data:    s.data,
		LastSeq: s.lastSeq,
		LastRes: s.lastRes,
	})
}

// RestoreSnapshot loads state from a snapshot (called after InstallSnapshot).
func (s *KVServer) RestoreSnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	type snap struct {
		Data    map[string]string `json:"data"`
		LastSeq map[string]uint64 `json:"lastSeq"`
		LastRes map[string]Result `json:"lastRes"`
	}
	var ss snap
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}
	s.data = ss.Data
	s.lastSeq = ss.LastSeq
	s.lastRes = ss.LastRes
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal
// ─────────────────────────────────────────────────────────────────────────────

// ErrNotLeader is returned when this node is not the Raft leader.
var ErrNotLeader = fmt.Errorf("not leader")

// ErrTimeout is returned when a proposal is not committed within proposalTimeout.
var ErrTimeout = fmt.Errorf("proposal timed out")

// ErrWrongLeader is returned when the log slot was taken by a different term's entry
// (another leader won the election and committed something else at the same index).
var ErrWrongLeader = fmt.Errorf("wrong leader: lost election, retry")

// propose encodes op as a Raft log entry and waits for it to be applied.
func (s *KVServer) propose(op Op) (Result, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return Result{}, fmt.Errorf("marshal op: %w", err)
	}

	index, term, isLeader := s.node.Propose(data)
	if !isLeader {
		return Result{}, ErrNotLeader
	}

	ch := make(chan Result, 1)
	s.mu.Lock()
	s.pending[index] = &pendingOp{index: index, term: term, respCh: ch}
	s.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-time.After(proposalTimeout):
		s.mu.Lock()
		delete(s.pending, index)
		s.mu.Unlock()
		return Result{}, ErrTimeout
	case <-s.stopCh:
		return Result{}, fmt.Errorf("server stopped")
	}
}

// applyLoop reads committed entries from Raft and applies them to the KV store.
func (s *KVServer) applyLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case msg, ok := <-s.applyCh:
			if !ok {
				return
			}
			if msg.SnapshotValid {
				if err := s.RestoreSnapshot(msg.Snapshot); err != nil {
					s.logger.Error("Failed to restore snapshot", "err", err)
				}
				continue
			}
			if msg.CommandValid {
				s.applyCommand(msg)
			}
		}
	}
}

// applyCommand applies a single committed log entry to the state machine.
func (s *KVServer) applyCommand(msg raft.ApplyMsg) {
	var op Op
	if err := json.Unmarshal(msg.Command, &op); err != nil {
		s.logger.Error("Failed to unmarshal command", "err", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Deduplication: if we've already applied this (clientID, seqNum), return
	// the cached result without re-executing (prevents double-write on retry).
	if op.SeqNum > 0 && op.SeqNum <= s.lastSeq[op.ClientID] {
		if p, ok := s.pending[msg.CommandIndex]; ok {
			// Only respond if our pending op matches (same term).
			if p.term == msg.CommandTerm {
				p.respCh <- s.lastRes[op.ClientID]
			}
			delete(s.pending, msg.CommandIndex)
		}
		return
	}

	// Execute the operation.
	var res Result
	switch op.Type {
	case OpGet:
		res.Value = s.data[op.Key]
	case OpPut:
		s.data[op.Key] = op.Value
	case OpDelete:
		delete(s.data, op.Key)
	default:
		res.Err = fmt.Errorf("unknown op type: %s", op.Type)
	}

	// Record for deduplication.
	if op.ClientID != "" {
		s.lastSeq[op.ClientID] = op.SeqNum
		s.lastRes[op.ClientID] = res
	}

	// Wake up the waiting caller if this is our pending op.
	if p, ok := s.pending[msg.CommandIndex]; ok {
		if p.term == msg.CommandTerm {
			p.respCh <- res
		} else {
			// A different term's entry was committed at this index.
			// Our proposal was lost; caller should retry.
			p.respCh <- Result{Err: ErrWrongLeader}
		}
		delete(s.pending, msg.CommandIndex)
	}
}

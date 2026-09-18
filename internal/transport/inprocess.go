// Package transport provides Transport implementations for Raft nodes.
//
// InProcessTransport connects multiple Node instances running in the same
// process. Used in tests to avoid real networking overhead. Supports:
//   - Controllable message delays
//   - Network partition simulation (disconnect/reconnect peers)
//   - Message drop simulation
package transport

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
)

// RaftNode is the subset of raft.Node methods the transport needs to call
// when delivering RPCs to the destination node.
type RaftNode interface {
	HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
	IsStopped() bool
}

// InProcessTransport is a Transport that routes RPCs directly to Node
// instances in the same process. Network delays and partitions are simulated.
type InProcessTransport struct {
	mu sync.RWMutex

	id    string
	nodes map[string]RaftNode // all registered nodes by ID

	// Network simulation controls (protected by mu).
	disconnected   map[string]bool  // src→dst: true = blocked
	dropRate       float64          // [0,1]: probability of dropping any RPC
	delayMin       time.Duration    // min artificial delay
	delayMax       time.Duration    // max artificial delay (0 = no delay)
}

// NewInProcessTransport creates a transport for the node with the given ID.
func NewInProcessTransport(id string) *InProcessTransport {
	return &InProcessTransport{
		id:           id,
		nodes:        make(map[string]RaftNode),
		disconnected: make(map[string]bool),
	}
}

// Register adds a node to the routing table. Call this for all nodes in
// the cluster before calling Start().
func (t *InProcessTransport) Register(id string, node RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

// ─────────────────────────────────────────────────────────────────────────────
// Transport interface
// ─────────────────────────────────────────────────────────────────────────────

func (t *InProcessTransport) RequestVote(target string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	t.maybeDelay()
	return node.HandleRequestVote(args), nil
}

func (t *InProcessTransport) AppendEntries(target string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	t.maybeDelay()
	return node.HandleAppendEntries(args), nil
}

func (t *InProcessTransport) InstallSnapshot(target string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	node, err := t.resolve(target)
	if err != nil {
		return nil, err
	}
	t.maybeDelay()
	return node.HandleInstallSnapshot(args), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Network simulation controls (useful in tests)
// ─────────────────────────────────────────────────────────────────────────────

// Disconnect causes this transport to reject all RPCs to target (simulates
// a one-way network partition: t.id → target is severed).
func (t *InProcessTransport) Disconnect(target string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disconnected[target] = true
}

// Reconnect re-enables RPCs to target.
func (t *InProcessTransport) Reconnect(target string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.disconnected, target)
}

// SetDropRate sets the probability [0,1] that any RPC will be silently dropped.
func (t *InProcessTransport) SetDropRate(rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropRate = rate
}

// SetDelay sets a random artificial delay range for all RPCs.
func (t *InProcessTransport) SetDelay(min, max time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delayMin = min
	t.delayMax = max
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

func (t *InProcessTransport) resolve(target string) (RaftNode, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.disconnected[target] {
		return nil, fmt.Errorf("transport: %s → %s disconnected", t.id, target)
	}
	if t.dropRate > 0 && rand.Float64() < t.dropRate { //nolint:gosec
		return nil, fmt.Errorf("transport: RPC to %s dropped (rate=%.2f)", target, t.dropRate)
	}

	node, ok := t.nodes[target]
	if !ok {
		return nil, fmt.Errorf("transport: unknown node %q", target)
	}
	if node.IsStopped() {
		return nil, fmt.Errorf("transport: node %q is stopped", target)
	}
	return node, nil
}

func (t *InProcessTransport) maybeDelay() {
	t.mu.RLock()
	lo, hi := t.delayMin, t.delayMax
	t.mu.RUnlock()

	if hi <= 0 {
		return
	}
	delay := lo
	if hi > lo {
		delta := hi - lo
		delay += time.Duration(rand.Int63n(int64(delta))) //nolint:gosec
	}
	if delay > 0 {
		time.Sleep(delay)
	}
}

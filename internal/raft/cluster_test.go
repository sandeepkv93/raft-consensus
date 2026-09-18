// Package raft_test contains integration tests for the Raft consensus implementation.
// Tests run against an in-process cluster with simulated network conditions.
package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
	"github.com/sandeepkv93/raft-consensus/internal/storage"
	"github.com/sandeepkv93/raft-consensus/internal/transport"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test cluster harness
// ─────────────────────────────────────────────────────────────────────────────

// clusterNode bundles a Raft node with its transport and apply channel.
type clusterNode struct {
	node      *raft.Node
	transport *transport.InProcessTransport
	applyCh   chan raft.ApplyMsg
	id        string

	// Persistent applied-message cache: all CommandValid messages ever received.
	// Protected by mu. Messages are never discarded, so every waitApplied call
	// works correctly regardless of the order nodes are waited on.
	mu      sync.Mutex
	applied map[uint64]raft.ApplyMsg
}

// drainLoop runs as a background goroutine for the lifetime of the cluster.
// It pulls messages from applyCh and stores them in the persistent cache.
func (cn *clusterNode) drainLoop(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case msg, ok := <-cn.applyCh:
			if !ok {
				return
			}
			if msg.CommandValid {
				cn.mu.Lock()
				cn.applied[msg.CommandIndex] = msg
				cn.mu.Unlock()
			}
		}
	}
}

// cluster is a collection of Raft nodes running in the same process.
type cluster struct {
	t      *testing.T
	nodes  []*clusterNode
	stopCh chan struct{}
}

// newCluster creates and starts an n-node Raft cluster.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, stopCh: make(chan struct{})}

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	// Faster timeouts for tests.
	cfg := raft.Config{
		HeartbeatInterval:   15,
		ElectionTimeoutMin:  50,
		ElectionTimeoutMax:  100,
		MaxLogEntriesPerRPC: 50,
		SnapshotThreshold:   100,
	}

	transports := make([]*transport.InProcessTransport, n)
	for i, id := range ids {
		transports[i] = transport.NewInProcessTransport(id)
	}

	// Create nodes.
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 256)
		store := storage.NewMemoryStorage()
		node := raft.NewNode(id, peers, transports[i], store, applyCh, cfg)
		cn := &clusterNode{
			node:      node,
			transport: transports[i],
			applyCh:   applyCh,
			id:        id,
			applied:   make(map[uint64]raft.ApplyMsg),
		}
		c.nodes = append(c.nodes, cn)
		go cn.drainLoop(c.stopCh)
	}

	// Wire transport routing (all nodes must be registered before start).
	for i := range c.nodes {
		for j := range c.nodes {
			if i != j {
				c.nodes[i].transport.Register(c.nodes[j].id, c.nodes[j].node)
			}
		}
	}

	// Start all nodes.
	for _, cn := range c.nodes {
		cn.node.Start()
	}

	t.Cleanup(c.shutdown)
	return c
}

// shutdown stops all nodes and drain goroutines.
func (c *cluster) shutdown() {
	for _, cn := range c.nodes {
		cn.node.Stop()
	}
	close(c.stopCh)
}

// waitLeader polls until a leader is elected and returns its index in c.nodes.
func (c *cluster) waitLeader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, cn := range c.nodes {
			if cn.node.IsLeader() {
				return i
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %v", timeout)
	return -1
}

// leader returns the current leader node, or nil.
func (c *cluster) leader() *clusterNode {
	for _, cn := range c.nodes {
		if cn.node.IsLeader() {
			return cn
		}
	}
	return nil
}

// disconnect isolates node i from the rest of the cluster (both directions).
func (c *cluster) disconnect(i int) {
	c.t.Logf("Disconnecting %s", c.nodes[i].id)
	for j, cn := range c.nodes {
		if i != j {
			c.nodes[i].transport.Disconnect(cn.id)
			cn.transport.Disconnect(c.nodes[i].id)
		}
	}
}

// reconnect restores connectivity for node i.
func (c *cluster) reconnect(i int) {
	c.t.Logf("Reconnecting %s", c.nodes[i].id)
	for j, cn := range c.nodes {
		if i != j {
			c.nodes[i].transport.Reconnect(cn.id)
			cn.transport.Reconnect(c.nodes[i].id)
		}
	}
}

// propose submits command to the leader and waits for it to be accepted.
func (c *cluster) propose(cmd []byte, timeout time.Duration) (uint64, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cn := range c.nodes {
			idx, term, ok := cn.node.Propose(cmd)
			if ok {
				return idx, term
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("failed to propose within %v", timeout)
	return 0, 0
}

// waitApplied polls until node i has applied index.
// Uses the persistent per-node cache - never loses messages between calls.
func (c *cluster) waitApplied(nodeIdx int, index uint64, timeout time.Duration) raft.ApplyMsg {
	c.t.Helper()
	cn := c.nodes[nodeIdx]
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cn.mu.Lock()
		msg, ok := cn.applied[index]
		cn.mu.Unlock()
		if ok {
			return msg
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("node %s did not apply index %d within %v", cn.id, index, timeout)
	return raft.ApplyMsg{}
}

// waitAllApplied waits for all running nodes to apply index.
func (c *cluster) waitAllApplied(index uint64, timeout time.Duration) {
	c.t.Helper()
	for i := range c.nodes {
		if !c.nodes[i].node.IsStopped() {
			c.waitApplied(i, index, timeout)
		}
	}
}

// leaderCount returns how many nodes currently believe they are leader.
func (c *cluster) leaderCount() int {
	count := 0
	for _, cn := range c.nodes {
		if cn.node.IsLeader() {
			count++
		}
	}
	return count
}

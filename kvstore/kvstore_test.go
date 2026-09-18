package kvstore_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
	"github.com/sandeepkv93/raft-consensus/internal/storage"
	"github.com/sandeepkv93/raft-consensus/internal/transport"
	"github.com/sandeepkv93/raft-consensus/kvstore"
)

// ─────────────────────────────────────────────────────────────────────────────
// KV cluster harness
// ─────────────────────────────────────────────────────────────────────────────

type kvClusterNode struct {
	raftNode  *raft.Node
	kvServer  *kvstore.KVServer
	transport *transport.InProcessTransport
	id        string
}

type kvCluster struct {
	t     *testing.T
	nodes []*kvClusterNode
}

func newKVCluster(t *testing.T, n int) *kvCluster {
	t.Helper()
	c := &kvCluster{t: t}

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("kv%d", i)
	}

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

	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 128)
		store := storage.NewMemoryStorage()
		rNode := raft.NewNode(id, peers, transports[i], store, applyCh, cfg)
		kvSrv := kvstore.NewKVServer(rNode, applyCh)

		c.nodes = append(c.nodes, &kvClusterNode{
			raftNode:  rNode,
			kvServer:  kvSrv,
			transport: transports[i],
			id:        id,
		})
	}

	// Wire transport.
	for i := range c.nodes {
		for j := range c.nodes {
			if i != j {
				c.nodes[i].transport.Register(c.nodes[j].id, c.nodes[j].raftNode)
			}
		}
	}

	// Start all.
	for _, cn := range c.nodes {
		cn.raftNode.Start()
		cn.kvServer.Start()
	}

	t.Cleanup(func() {
		for _, cn := range c.nodes {
			cn.kvServer.Stop()
			cn.raftNode.Stop()
		}
	})
	return c
}

// leader returns the KV server on the current leader node.
func (c *kvCluster) leader(timeout time.Duration) *kvstore.KVServer {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cn := range c.nodes {
			if cn.raftNode.IsLeader() {
				return cn.kvServer
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no KV leader found")
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const leaderWait = 2 * time.Second

// TestKV_PutGet verifies basic put and get operations.
func TestKV_PutGet(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	if err := srv.Put("c1", 1, "foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := srv.Get("c1", 2, "foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "bar" {
		t.Errorf("expected 'bar', got %q", val)
	}
}

// TestKV_Delete verifies delete removes a key.
func TestKV_Delete(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	srv.Put("c1", 1, "key", "value")
	srv.Delete("c1", 2, "key")

	val, err := srv.Get("c1", 3, "key")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty after delete, got %q", val)
	}
}

// TestKV_GetMissing verifies that getting a non-existent key returns "".
func TestKV_GetMissing(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	val, err := srv.Get("c1", 1, "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty for missing key, got %q", val)
	}
}

// TestKV_Overwrite verifies that put overwrites an existing key.
func TestKV_Overwrite(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	srv.Put("c1", 1, "k", "v1")
	srv.Put("c1", 2, "k", "v2")

	val, _ := srv.Get("c1", 3, "k")
	if val != "v2" {
		t.Errorf("expected v2 after overwrite, got %q", val)
	}
}

// TestKV_NotLeader verifies that Put on a non-leader returns ErrNotLeader.
func TestKV_NotLeader(t *testing.T) {
	c := newKVCluster(t, 3)
	c.leader(leaderWait) // ensure a leader exists

	// Find a follower.
	var followerSrv *kvstore.KVServer
	for _, cn := range c.nodes {
		if !cn.raftNode.IsLeader() {
			followerSrv = cn.kvServer
			break
		}
	}
	if followerSrv == nil {
		t.Skip("no follower found")
	}

	err := followerSrv.Put("c1", 1, "key", "val")
	if err != kvstore.ErrNotLeader {
		t.Errorf("expected ErrNotLeader from follower, got %v", err)
	}
}

// TestKV_ConcurrentClients verifies multiple clients can operate concurrently.
func TestKV_ConcurrentClients(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	const clients = 5
	const opsPerClient = 10

	var wg sync.WaitGroup
	errs := make(chan error, clients*opsPerClient)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			cid := fmt.Sprintf("client-%d", clientID)
			for seq := uint64(1); seq <= opsPerClient; seq++ {
				key := fmt.Sprintf("c%d-key%d", clientID, seq)
				val := fmt.Sprintf("v%d", seq)
				if err := srv.Put(cid, seq, key, val); err != nil {
					errs <- fmt.Errorf("client %d put %s: %w", clientID, key, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

// TestKV_Idempotency verifies that duplicate (clientID, seqNum) ops are deduplicated.
func TestKV_Idempotency(t *testing.T) {
	c := newKVCluster(t, 3)
	srv := c.leader(leaderWait)

	// Put "key" = "first" with seqNum=1.
	srv.Put("c1", 1, "key", "first")

	// Retry the same op (same seqNum). The value must NOT change.
	srv.Put("c1", 1, "key", "first-retry")

	// A new op with seqNum=2 updates the value.
	srv.Put("c1", 2, "key", "second")

	val, _ := srv.Get("c1", 3, "key")
	if val != "second" {
		t.Errorf("expected 'second', got %q", val)
	}
}

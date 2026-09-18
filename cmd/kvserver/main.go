// cmd/kvserver is a standalone binary that runs a 3-node Raft-backed KV
// cluster entirely in one process, prints every applied command, and
// demonstrates the Raft consensus in action.
//
// Usage:
//
//	go run ./cmd/kvserver
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/sandeepkv93/raft-consensus/internal/raft"
	"github.com/sandeepkv93/raft-consensus/internal/storage"
	"github.com/sandeepkv93/raft-consensus/internal/transport"
	"github.com/sandeepkv93/raft-consensus/kvstore"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn, // quiet logs so demo output is readable
	})))

	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║   Raft Consensus - Live Demo (3-node KV)     ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	// ── Build a 3-node in-process cluster ───────────────────────────────
	ids := []string{"alpha", "beta", "gamma"}
	cfg := raft.Config{
		HeartbeatInterval:   50,
		ElectionTimeoutMin:  150,
		ElectionTimeoutMax:  300,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
	}

	type node struct {
		id        string
		raftNode  *raft.Node
		kvServer  *kvstore.KVServer
		transport *transport.InProcessTransport
		applyCh   chan raft.ApplyMsg
	}

	nodes := make([]*node, len(ids))
	transports := make([]*transport.InProcessTransport, len(ids))
	for i, id := range ids {
		transports[i] = transport.NewInProcessTransport(id)
	}

	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		applyCh := make(chan raft.ApplyMsg, 64)
		rNode := raft.NewNode(id, peers, transports[i], storage.NewMemoryStorage(), applyCh, cfg)
		kv := kvstore.NewKVServer(rNode, applyCh)
		nodes[i] = &node{id: id, raftNode: rNode, kvServer: kv, transport: transports[i], applyCh: applyCh}
	}

	// Wire transport.
	for i := range nodes {
		for j := range nodes {
			if i != j {
				nodes[i].transport.Register(nodes[j].id, nodes[j].raftNode)
			}
		}
	}

	// Start cluster.
	for _, n := range nodes {
		n.raftNode.Start()
		n.kvServer.Start()
	}

	// ── Wait for leader election ─────────────────────────────────────────
	fmt.Println("⏳ Waiting for leader election...")
	var leaderNode *node
	for i := 0; i < 100; i++ {
		for _, n := range nodes {
			if n.raftNode.IsLeader() {
				leaderNode = n
				break
			}
		}
		if leaderNode != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leaderNode == nil {
		log.Fatal("No leader elected!")
	}
	fmt.Printf("✅ Leader elected: %s (term %d)\n\n", leaderNode.id, leaderNode.raftNode.Term())

	srv := leaderNode.kvServer
	demo := func(label, key, val string) {
		if val == "" {
			v, err := srv.Get("demo-client", nextSeq(), key)
			if err != nil {
				fmt.Printf("   %-12s GET  %-10s → ERROR: %v\n", label, key, err)
			} else if v == "" {
				fmt.Printf("   %-12s GET  %-10s → (not found)\n", label, key)
			} else {
				fmt.Printf("   %-12s GET  %-10s → %q\n", label, key, v)
			}
		} else if val == "DELETE" {
			err := srv.Delete("demo-client", nextSeq(), key)
			fmt.Printf("   %-12s DEL  %-10s → err=%v\n", label, key, err)
		} else {
			err := srv.Put("demo-client", nextSeq(), key, val)
			fmt.Printf("   %-12s PUT  %-10s = %-12s err=%v\n", label, key, val, err)
		}
	}

	// ── Demo 1: Basic KV operations ─────────────────────────────────────
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Demo 1: Basic KV Operations")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	demo("client-1", "user:1", "alice")
	demo("client-1", "user:2", "bob")
	demo("client-1", "user:3", "charlie")
	demo("client-1", "user:1", "")
	demo("client-1", "user:2", "")
	demo("client-1", "user:3", "DELETE")
	demo("client-1", "user:3", "")
	fmt.Println()

	// ── Demo 2: Leader failure & recovery ───────────────────────────────
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Demo 2: Leader Failure & Re-Election")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Current leader: %s\n", leaderNode.id)
	fmt.Println("  ❌ Disconnecting leader from cluster...")

	// Disconnect leader.
	for _, n := range nodes {
		if n.id != leaderNode.id {
			leaderNode.transport.Disconnect(n.id)
			n.transport.Disconnect(leaderNode.id)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Find new leader.
	var newLeader *node
	for i := 0; i < 60; i++ {
		for _, n := range nodes {
			if n.id != leaderNode.id && n.raftNode.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		fmt.Println("  ⚠️  No new leader elected (check election timeouts)")
	} else {
		fmt.Printf("  ✅ New leader elected: %s (term %d)\n", newLeader.id, newLeader.raftNode.Term())
		newSrv := newLeader.kvServer
		_ = newSrv.Put("demo-client", nextSeq(), "recovery-key", "cluster-recovered")
		v, _ := newSrv.Get("demo-client", nextSeq(), "recovery-key")
		fmt.Printf("  📖 recovery-key = %q\n", v)
	}

	// Reconnect old leader.
	for _, n := range nodes {
		if n.id != leaderNode.id {
			leaderNode.transport.Reconnect(n.id)
			n.transport.Reconnect(leaderNode.id)
		}
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("  ♻️  Reconnected %s - now rejoins as follower (term %d)\n",
		leaderNode.id, leaderNode.raftNode.Term())
	fmt.Println()

	// ── Demo 3: Cluster status ───────────────────────────────────────────
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Demo 3: Node Status")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	for _, n := range nodes {
		var s map[string]any
		json.Unmarshal(n.raftNode.StatusJSON(), &s)
		fmt.Printf("  %s  state=%-9s  term=%-3v  leader=%v\n",
			n.id, s["state"], s["term"], s["leader"])
	}
	fmt.Println()
	fmt.Println("✅ Demo complete.")

	// Cleanup.
	for _, n := range nodes {
		n.kvServer.Stop()
		n.raftNode.Stop()
	}
}

var seq uint64

func nextSeq() uint64 {
	seq++
	return seq
}

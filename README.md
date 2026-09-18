# raft-consensus

A **production-quality Raft Consensus implementation in Go**, built from scratch following the original [Raft paper](https://raft.github.io/raft.pdf) by Ongaro & Ousterhout.

Includes a **linearizable distributed KV store** application layer on top of the consensus engine.

---

## What is Raft?

Raft is a distributed consensus algorithm designed for understandability. It solves the problem of making a cluster of servers agree on a shared sequence of values (a log), even in the presence of failures. Once agreed upon, each server applies log entries to its state machine in the same order, producing identical state.

**Three core problems Raft solves:**

```
Leader Election  →  One server is elected leader. It handles all writes.
Log Replication  →  Leader replicates entries to followers. Committed = majority agreed.
Safety          →  No two leaders in the same term. Committed entries never lost.
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        KV Store Layer                        │
│  kvstore.KVServer  ─  GET / PUT / DELETE with deduplication │
└────────────────────────┬────────────────────────────────────┘
                         │ Propose / ApplyCh
┌────────────────────────▼────────────────────────────────────┐
│                      Raft Core (raft.Node)                   │
│                                                              │
│  Leader Election ─ RequestVote RPCs ─ randomised timers      │
│  Log Replication ─ AppendEntries ─ commitIndex advancement   │
│  Snapshotting    ─ log compaction ─ InstallSnapshot RPC      │
└──────────┬─────────────────────────────┬────────────────────┘
           │ Transport                   │ Storage
    ┌──────▼──────┐               ┌──────▼──────┐
    │  In-Process │               │   Memory    │
    │  (tests)    │               │   or File   │
    │  or TCP/gRPC│               │  (durable)  │
    └─────────────┘               └─────────────┘
```

### Package Layout

```
raft-consensus/
├── internal/
│   ├── raft/           # Core Raft node (types.go, node.go)
│   ├── transport/      # In-process transport with network simulation
│   └── storage/        # MemoryStorage + FileStorage (atomic writes)
├── kvstore/            # Linearizable KV store on top of Raft
└── cmd/
    └── kvserver/       # Runnable demo: 3-node KV cluster
```

---

## Key Raft Properties Implemented

| Property | Implementation |
|---|---|
| **Leader Election** | Randomised election timeouts (150–300ms default) prevent split votes |
| **Vote Safety** | Each node votes at most once per term; persisted before responding |
| **Log Matching** | `PrevLogIndex`/`PrevLogTerm` consistency check on AppendEntries |
| **Election Safety** | Candidates must have log at least as up-to-date as majority (§5.4.1) |
| **Leader Completeness** | Only commit entries from current term (§5.4.2) |
| **Fast Log Rollback** | ConflictTerm/ConflictIndex optimisation avoids O(n) retries |
| **Snapshotting** | Log compaction + `InstallSnapshot` for lagging followers (§7) |
| **Linearizability** | KV layer uses (clientID, seqNum) deduplication for idempotent retries |

---

## Quick Start

### Run the demo

```bash
go run ./cmd/kvserver
```

Example output:
```
╔══════════════════════════════════════════════╗
║   Raft Consensus — Live Demo (3-node KV)     ║
╚══════════════════════════════════════════════╝

⏳ Waiting for leader election...
✅ Leader elected: alpha (term 1)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Demo 1: Basic KV Operations
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   client-1     PUT  user:1      = alice        err=<nil>
   client-1     PUT  user:2      = bob          err=<nil>
   client-1     GET  user:1      → "alice"
   client-1     DEL  user:3      → err=<nil>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Demo 2: Leader Failure & Re-Election
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Current leader: alpha
  ❌ Disconnecting leader from cluster...
  ✅ New leader elected: beta (term 3)
  📖 recovery-key = "cluster-recovered"
  ♻️  Reconnected alpha — now rejoins as follower (term 3)
```

### Run all tests

```bash
go test ./... -v -timeout 120s
```

### Run stress test

```bash
go test ./internal/raft/... -run TestStress -v -timeout 120s
```

---

## Test Coverage

| Test | What it verifies |
|------|-----------------|
| `TestElection_SingleLeader` | Exactly one leader elected in a 3-node cluster |
| `TestElection_FiveNode` | Election works in 5-node clusters |
| `TestElection_TermMonotonicity` | New election always uses a higher term |
| `TestReplication_Basic` | Single command applied on all nodes |
| `TestReplication_Sequential` | 20 commands applied in order on all nodes |
| `TestReplication_ConcurrentProposals` | Concurrent proposals all committed |
| `TestFailure_LeaderCrash` | New leader elected after crash; cluster continues |
| `TestFailure_MinorityPartition` | 5-node cluster works with 2 nodes disconnected |
| `TestFailure_NoQuorum` | Cluster doesn't commit without quorum; recovers after reconnect |
| `TestPartition_SplitBrain` | Minority (2 of 5) cannot elect a leader |
| `TestSnapshot_Basic` | Log compaction works; cluster functions after snapshot |
| `TestSnapshot_LaggingFollower` | Follower catches up via `InstallSnapshot` |
| `TestStress_ManyCommands` | 200 commands committed and applied across 5 nodes |
| `TestKV_PutGet` | Basic KV put + get |
| `TestKV_Delete` | Delete removes key |
| `TestKV_NotLeader` | Follower returns `ErrNotLeader` |
| `TestKV_ConcurrentClients` | 5 concurrent clients, 10 ops each |
| `TestKV_Idempotency` | Duplicate (clientID, seqNum) ops are deduplicated |

---

## Raft Log Structure

```
Index:   0    1    2    3    4    5    6
Term:    0    1    1    2    2    2    3
         ↑
         Sentinel (snapshotIndex)

After snapshot at index 4:
Index:   4    5    6
Term:    2    2    3
         ↑
         New sentinel (compacted log)
```

---

## Configuration

```go
cfg := raft.Config{
    HeartbeatInterval:   50,   // ms — how often leader sends heartbeats
    ElectionTimeoutMin:  150,  // ms — minimum election timeout
    ElectionTimeoutMax:  300,  // ms — maximum election timeout (randomised)
    MaxLogEntriesPerRPC: 100,  // max entries per AppendEntries RPC
    SnapshotThreshold:   1000, // compact after this many log entries
}
```

---

## Using the Raft Node

```go
// 1. Create storage and transport.
store := storage.NewMemoryStorage() // or NewFileStorage("/data/node1")
tr := transport.NewInProcessTransport("node1")

// 2. Create the node.
applyCh := make(chan raft.ApplyMsg, 128)
node := raft.NewNode("node1", []string{"node2", "node3"}, tr, store, applyCh, raft.DefaultConfig())

// 3. Register peer nodes with the transport.
tr.Register("node2", node2)
tr.Register("node3", node3)

// 4. Start.
node.Start()
defer node.Stop()

// 5. Propose commands (only works if this node is leader).
if idx, term, ok := node.Propose([]byte(`{"op":"set","key":"x","val":"1"}`)); ok {
    // Wait for ApplyMsg with CommandIndex == idx on applyCh.
}

// 6. Read applied commands.
for msg := range applyCh {
    if msg.CommandValid {
        // Apply msg.Command to your state machine.
    }
    if msg.SnapshotValid {
        // Restore state from msg.Snapshot.
    }
}
```

---

## References

- [Raft Paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout, 2014
- [Raft Visualization](https://raft.github.io/) — Interactive animation
- [Extended Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) — Full version with snapshots
- [MIT 6.824](https://pdos.csail.mit.edu/6.824/) — Distributed Systems course (inspiration for test structure)

---

## License

MIT

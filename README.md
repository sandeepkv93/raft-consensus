<div align="center">

# raft-consensus

**A complete Raft distributed consensus implementation in Go - built from scratch.**

Leader election · Log replication · Log compaction · Linearizable KV store

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev)
[![Tests](https://img.shields.io/badge/Tests-21%20passing-brightgreen?style=flat)](#test-coverage)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat)](LICENSE)

</div>

---

## Table of Contents

- [What is Raft?](#what-is-raft)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
  - [Leader Election](#1-leader-election)
  - [Log Replication](#2-log-replication)
  - [Snapshotting & Log Compaction](#3-snapshotting--log-compaction)
  - [KV Store Layer](#4-kv-store-layer)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Usage](#api-usage)
- [Test Coverage](#test-coverage)
- [Safety Properties](#safety-properties)
- [References](#references)

---

## What is Raft?

Raft is a distributed consensus algorithm designed to be **understandable**. It solves the fundamental problem of making a cluster of machines agree on a shared, ordered log of commands - even when some machines crash or become unreachable.

Every server that applies the same log in the same order reaches the same state, making the cluster behave like a single reliable machine despite individual failures.

```mermaid
flowchart LR
    C1["Client 1"]
    C2["Client 2"]
    C3["Client 3"]

    subgraph cluster["Raft Cluster"]
        direction TB
        L["🟡 Leader\n(handles all writes)"]
        F1["⬜ Follower"]
        F2["⬜ Follower"]
        L -->|"replicate"| F1
        L -->|"replicate"| F2
    end

    C1 -->|"write"| L
    C2 -->|"write"| L
    C3 -->|"redirect"| L

    subgraph sm["State Machine (per node)"]
        direction LR
        SM1["KV Store\ncopy 1"]
        SM2["KV Store\ncopy 2"]
        SM3["KV Store\ncopy 3"]
    end

    F1 -->|"apply"| SM2
    F2 -->|"apply"| SM3
    L -->|"apply"| SM1
```

**Three core guarantees:**

| Guarantee | Meaning |
|---|---|
| **Election Safety** | At most one leader per term |
| **Log Matching** | If two logs have an entry at the same index with the same term, they are identical up to that point |
| **Leader Completeness** | A committed entry will always be present in all future leaders' logs |

---

## Architecture

The implementation is structured in three clean layers, each with a well-defined interface boundary.

```mermaid
flowchart TD
    Client["Client\nGET / PUT / DELETE"]

    subgraph app["Application Layer  -  kvstore/"]
        KV["KVServer\n• Linearizable reads/writes\n• Client deduplication\n  via (clientID, seqNum)\n• Snapshot / restore"]
    end

    subgraph core["Consensus Core  -  internal/raft/"]
        Node["raft.Node\n• Leader election\n• Log replication\n• Commit index\n• Snapshotting"]
    end

    subgraph infra["Infrastructure  -  internal/"]
        T["Transport interface\n────────────────\nInProcessTransport\n• Network partitions\n• Drop rates / delays"]
        S["Storage interface\n────────────────\nMemoryStorage  (tests)\nFileStorage    (prod)\n• Atomic write-rename"]
    end

    Client -->|"Put / Get / Delete"| KV
    KV -->|"Propose(cmd)"| Node
    Node -->|"ApplyMsg"| KV
    Node <-->|"RequestVote\nAppendEntries\nInstallSnapshot"| T
    Node <-->|"SaveState\nLoadState\nSaveSnapshot"| S
```

---

## How It Works

### 1. Leader Election

Every node starts as a **Follower**. If a follower doesn't hear from a leader within its randomised election timeout (150–300 ms), it becomes a **Candidate** and requests votes. A candidate that receives votes from a majority becomes **Leader**.

```mermaid
stateDiagram-v2
    [*] --> Follower : node starts

    Follower --> Candidate : election timeout fires\n(no heartbeat received)
    Candidate --> Follower : discovers higher term\nor another node won
    Candidate --> Leader : receives votes from\n⌈N/2⌉ + 1 nodes
    Leader --> Follower : discovers higher term\n(stale leader steps down)

    Follower --> Follower : receives valid\nheartbeat → reset timer
    Candidate --> Candidate : election timeout fires\nagain → start new election\n(increment term)
    Leader --> Leader : sends heartbeats\nevery 50ms
```

**Election timeline for a 3-node cluster:**

```mermaid
sequenceDiagram
    participant node0 as node0 (Follower)
    participant node1 as node1 (Candidate → Leader)
    participant node2 as node2 (Follower)

    Note over node0,node2: All nodes start as Followers

    Note over node1: Election timeout fires first
    node1->>node1: term++, vote for self, become Candidate

    par RequestVote RPCs (sent in parallel)
        node1->>node0: RequestVote {term:1, lastLogIdx:0, lastLogTerm:0}
        node0-->>node1: {voteGranted: true}
    and
        node1->>node2: RequestVote {term:1, lastLogIdx:0, lastLogTerm:0}
        node2-->>node1: {voteGranted: true}
    end

    Note over node1: Received majority (2/2 peers) → become Leader

    node1->>node0: AppendEntries heartbeat {term:1, entries:[]}
    node1->>node2: AppendEntries heartbeat {term:1, entries:[]}

    Note over node0,node2: Reset election timers on valid heartbeat
```

**Split-vote prevention:** Each node picks a random timeout in `[ElectionTimeoutMin, ElectionTimeoutMax]`. This ensures that in most elections, only one node times out first, avoiding ties. If a tie occurs, nodes increment their term and try again - the randomness ensures convergence.

---

### 2. Log Replication

All client writes go through the leader. The leader appends the command to its log, then replicates it to followers via `AppendEntries`. Once a majority has persisted the entry, it is **committed** and safe to apply to the state machine.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (node1)
    participant F1 as Follower (node0)
    participant F2 as Follower (node2)

    C->>L: PUT("user:1", "alice")

    Note over L: Append to local log\nindex=5, term=2

    par Replicate in parallel
        L->>F1: AppendEntries {prevLogIdx:4, prevLogTerm:2,\n entries:[{idx:5,term:2,cmd:PUT user:1 alice}]}
        F1-->>L: {success: true}
    and
        L->>F2: AppendEntries {prevLogIdx:4, prevLogTerm:2,\n entries:[{idx:5,term:2,cmd:PUT user:1 alice}]}
        F2-->>L: {success: true}
    end

    Note over L: Majority (2/2 followers) ✓\nAdvance commitIndex → 5

    L->>L: Apply index 5 to KV store
    L-->>C: OK

    Note over F1,F2: On next AppendEntries,\nfollowers learn commitIndex=5\nand apply locally
```

**The Raft log** maintains a strict ordering guarantee:

```mermaid
flowchart LR
    subgraph log["Replicated Log (all nodes identical after commit)"]
        direction LR
        e0["idx:0\nterm:0\n(sentinel)"]
        e1["idx:1\nterm:1\nSET x=1"]
        e2["idx:2\nterm:1\nSET y=2"]
        e3["idx:3\nterm:2\nDEL x"]
        e4["idx:4\nterm:2\nSET z=9"]
        e5["idx:5\nterm:2\nSET a=7"]
        e0 --> e1 --> e2 --> e3 --> e4 --> e5
    end

    ci["commitIndex = 4\n(majority confirmed)"]
    la["lastApplied = 4\n(sent to state machine)"]
    next["index 5 = in-flight\n(not yet committed)"]

    e4 -.- ci
    e4 -.- la
    e5 -.- next
```

**Fast log rollback:** When a follower rejects `AppendEntries` due to a log mismatch, it returns `ConflictTerm` and `ConflictIndex` so the leader can jump back multiple entries in a single round-trip instead of decrementing `nextIndex` one-by-one.

---

### 3. Snapshotting & Log Compaction

Logs grow unboundedly. When the log exceeds `SnapshotThreshold` entries, the application takes a snapshot of its state machine. Raft compacts all entries up to `lastIncludedIndex` into the snapshot, freeing memory and disk space.

```mermaid
flowchart TD
    subgraph before["Before Snapshot  (log grows)"]
        direction LR
        b0["idx:0"] --> b1["idx:1\nSET x=1"] --> b2["idx:2\nSET y=2"] --> b3["idx:3\nDEL x"] --> b4["idx:4\nSET y=9"] --> b5["idx:5\nSET z=1"]
    end

    snap["📸 TakeSnapshot(index=4)\ndata = {y:9}"]

    subgraph after["After Compaction  (log shrinks)"]
        direction LR
        a4["idx:4\nterm:2\n(new sentinel)"] --> a5["idx:5\nSET z=1"]
    end

    before --> snap --> after

    subgraph snapfile["Snapshot File"]
        sf["lastIncludedIndex: 4\nlastIncludedTerm:  2\ndata: {y: 9}"]
    end

    snap -.->|"persisted to"| snapfile
```

**Catching up a lagging follower via `InstallSnapshot`:**

```mermaid
sequenceDiagram
    participant L as Leader
    participant LF as Lagging Follower

    Note over LF: Missed entries 1–40\n(leader already compacted them)

    L->>LF: InstallSnapshot {\n  lastIncludedIndex: 40,\n  lastIncludedTerm: 2,\n  data: <state machine bytes>\n}

    Note over LF: Discard old log\nReplace with snapshot sentinel\nApply snapshot to state machine

    LF-->>L: {term: 2}

    Note over L: Update matchIndex[LF] = 40\nnextIndex[LF] = 41

    L->>LF: AppendEntries {entries: [41, 42, 43 ...]}
    LF-->>L: {success: true}

    Note over LF: Now fully caught up ✓
```

---

### 4. KV Store Layer

The KV store sits on top of Raft, turning the raw log replication into a **linearizable** key-value store. Every operation is serialised through the Raft log, ensuring all replicas apply commands in the same order.

```mermaid
sequenceDiagram
    participant C as Client
    participant KV as KVServer (Leader)
    participant R as raft.Node
    participant AC as ApplyCh

    C->>KV: Put(clientID="c1", seqNum=7,\nkey="user:1", val="alice")

    KV->>R: Propose(encoded op)
    Note over R: Append to log, replicate to majority

    R->>AC: ApplyMsg{CommandIndex: 42, Command: ...}
    AC->>KV: applyCommand(msg)

    Note over KV: Deduplicate: seqNum=7 > lastSeq[c1]=6\n→ execute, record result

    KV->>KV: data["user:1"] = "alice"\nlastSeq["c1"] = 7

    KV-->>C: OK
```

**Client deduplication** prevents double-writes when a client retries after a timeout:

```mermaid
flowchart TD
    req["Client Request\n(clientID, seqNum, op)"]
    check{seqNum <=\nlastSeq\nclientID?}
    cached["Return cached result\n(idempotent replay)"]
    execute["Execute operation\nUpdate state machine\nRecord lastSeq + result"]

    req --> check
    check -->|"Yes - duplicate"| cached
    check -->|"No - new op"| execute
```

---

## Project Structure

```
raft-consensus/
│
├── internal/
│   ├── raft/
│   │   ├── types.go          # LogEntry, RPC structs, Config, ApplyMsg
│   │   ├── node.go           # Core Raft node - election, replication, snapshots
│   │   ├── cluster_test.go   # Reusable test cluster harness
│   │   └── raft_test.go      # 13 integration tests
│   │
│   ├── transport/
│   │   └── inprocess.go      # In-process transport with partition simulation
│   │
│   └── storage/
│       └── storage.go        # MemoryStorage + FileStorage (atomic write-rename)
│
├── kvstore/
│   ├── kvserver.go           # Linearizable KV store on top of Raft
│   └── kvstore_test.go       # 8 KV integration tests
│
├── cmd/
│   └── kvserver/
│       └── main.go           # Runnable demo: 3-node cluster
│
├── go.mod
└── README.md
```

---

## Quick Start

### Prerequisites

```bash
go version  # requires Go 1.21+
```

### Run the interactive demo

```bash
git clone https://github.com/sandeepkv93/raft-consensus.git
cd raft-consensus
go run ./cmd/kvserver
```

**Example output:**

```
╔══════════════════════════════════════════════╗
║   Raft Consensus - Live Demo (3-node KV)     ║
╚══════════════════════════════════════════════╝

⏳ Waiting for leader election...
✅ Leader elected: beta (term 1)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Demo 1: Basic KV Operations
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   client-1     PUT  user:1     = alice        err=<nil>
   client-1     PUT  user:2     = bob          err=<nil>
   client-1     PUT  user:3     = charlie      err=<nil>
   client-1     GET  user:1     → "alice"
   client-1     GET  user:2     → "bob"
   client-1     DEL  user:3     → err=<nil>
   client-1     GET  user:3     → (not found)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Demo 2: Leader Failure & Re-Election
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Current leader: beta
  ❌ Disconnecting leader from cluster...
  ✅ New leader elected: gamma (term 3)
  📖 recovery-key = "cluster-recovered"
  ♻️  Reconnected beta - now rejoins as follower (term 3)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Demo 3: Node Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  alpha  state=Follower   term=3    leader=gamma
  beta   state=Follower   term=3    leader=gamma
  gamma  state=Leader     term=3    leader=gamma

✅ Demo complete.
```

### Run all tests

```bash
go test ./... -v -timeout 120s
```

### Run the stress test

```bash
go test ./internal/raft/... -run TestStress -v -timeout 120s
```

---

## Configuration

```go
cfg := raft.Config{
    // How often the leader broadcasts heartbeats.
    // Must be << ElectionTimeoutMin (broadcast time << election timeout).
    HeartbeatInterval: 50, // ms

    // Randomised election timeout range.
    // Randomisation prevents split votes.
    ElectionTimeoutMin: 150, // ms
    ElectionTimeoutMax: 300, // ms

    // Max log entries per AppendEntries RPC.
    // Prevents single RPCs from becoming too large.
    MaxLogEntriesPerRPC: 100,

    // Compact the log after this many entries.
    SnapshotThreshold: 1000,
}
```

**Timing relationships (Raft paper §5.6):**

```
broadcastTime  <<  electionTimeout  <<  MTBF
    ~1ms               150–300ms       hours/days
```

| Parameter | Development | Production |
|---|---|---|
| `HeartbeatInterval` | 15 ms | 50–150 ms |
| `ElectionTimeoutMin` | 50 ms | 150–500 ms |
| `ElectionTimeoutMax` | 100 ms | 300–1000 ms |

---

## API Usage

### Creating a Raft node

```go
import (
    "github.com/sandeepkv93/raft-consensus/internal/raft"
    "github.com/sandeepkv93/raft-consensus/internal/storage"
    "github.com/sandeepkv93/raft-consensus/internal/transport"
)

// 1. Create durable storage (survives restarts).
store, _ := storage.NewFileStorage("/data/node1")

// 2. Create the RPC transport.
tr := transport.NewInProcessTransport("node1")

// 3. Create the node.
applyCh := make(chan raft.ApplyMsg, 128)
node := raft.NewNode(
    "node1",
    []string{"node2", "node3"}, // peer IDs
    tr,
    store,
    applyCh,
    raft.DefaultConfig(),
)

// 4. Register peer nodes with the transport layer.
tr.Register("node2", node2)
tr.Register("node3", node3)

// 5. Start.
node.Start()
defer node.Stop()
```

### Proposing commands

```go
// Propose only succeeds on the leader.
idx, term, isLeader := node.Propose([]byte(`{"op":"set","key":"x","val":"1"}`))
if !isLeader {
    // Redirect client to node.LeaderID()
    return
}
// idx is the expected log index. Wait for ApplyMsg.CommandIndex == idx on applyCh.
```

### Consuming applied commands

```go
for msg := range applyCh {
    switch {
    case msg.CommandValid:
        // Safe to apply to your state machine.
        // msg.CommandIndex is monotonically increasing.
        applyToStateMachine(msg.Command)

        // Optionally trigger a snapshot.
        if msg.CommandIndex % snapshotInterval == 0 {
            snapData := stateMachine.Serialize()
            node.TakeSnapshot(msg.CommandIndex, snapData)
        }

    case msg.SnapshotValid:
        // Leader installed a snapshot (you were lagging).
        // Reset your state machine to the snapshot state.
        stateMachine.Restore(msg.Snapshot)
    }
}
```

### Using the KV store

```go
import "github.com/sandeepkv93/raft-consensus/kvstore"

// Create and start the KV server.
kv := kvstore.NewKVServer(node, applyCh)
kv.Start()
defer kv.Stop()

// Operations (only succeed on leader).
kv.Put("client-1", seqNum, "user:1", "alice")
val, err := kv.Get("client-1", seqNum, "user:1") // → "alice"
kv.Delete("client-1", seqNum, "user:1")
```

---

## Test Coverage

**21 tests · 0 failures** across Raft core and KV store.

### Raft Core (`internal/raft/`)

| Test | Scenario | Verifies |
|------|----------|----------|
| `TestElection_SingleLeader` | 3-node startup | Exactly one leader elected |
| `TestElection_FiveNode` | 5-node startup | Election scales correctly |
| `TestElection_TermMonotonicity` | Leader disconnected | New election uses higher term |
| `TestReplication_Basic` | Single command | Applied on all 3 nodes |
| `TestReplication_Sequential` | 20 commands | Applied in order on all nodes |
| `TestReplication_ConcurrentProposals` | 10 parallel proposals | All committed and applied |
| `TestFailure_LeaderCrash` | Leader disconnected | New leader elected; cluster continues; old leader rejoins |
| `TestFailure_MinorityPartition` | 2 of 5 nodes disconnected | Majority (3 nodes) keeps working |
| `TestFailure_NoQuorum` | 2 of 3 followers disconnected | No commits during isolation; recovers after reconnect |
| `TestPartition_SplitBrain` | Asymmetric 2/3 partition | Minority cannot commit (no quorum); entries converge after heal |
| `TestSnapshot_Basic` | 30 commands then snapshot | Log compacted; cluster continues working |
| `TestSnapshot_LaggingFollower` | Follower misses 40 entries | Catches up via `InstallSnapshot` |
| `TestStress_ManyCommands` | 200 commands on 5 nodes | All applied; no data loss |

### KV Store (`kvstore/`)

| Test | Verifies |
|------|----------|
| `TestKV_PutGet` | Basic put and get |
| `TestKV_GetMissing` | Missing key returns `""` |
| `TestKV_Delete` | Delete removes key |
| `TestKV_Overwrite` | Put overwrites existing key |
| `TestKV_NotLeader` | Follower returns `ErrNotLeader` |
| `TestKV_ConcurrentClients` | 5 concurrent clients, 10 ops each |
| `TestKV_Idempotency` | Duplicate `(clientID, seqNum)` deduplicated |

---

## Safety Properties

Raft guarantees five fundamental safety properties. All are verified by the test suite:

```mermaid
flowchart TD
    subgraph safety["Raft Safety Properties"]
        ES["Election Safety\nAt most one leader per term\n→ TestElection_SingleLeader\n   TestElection_TermMonotonicity"]
        LM["Log Matching\nIf entries share index+term,\nall preceding entries are identical\n→ TestReplication_Sequential"]
        LC["Leader Completeness\nCommitted entries appear in\nall future leaders' logs\n→ TestFailure_LeaderCrash"]
        SM["State Machine Safety\nAll servers apply the same\ncommand at each index\n→ TestStress_ManyCommands"]
        SB["Split-Brain Prevention\nMinority partition cannot commit\n→ TestPartition_SplitBrain\n   TestFailure_NoQuorum"]
    end

    ES --> LC
    LM --> SM
    LC --> SM
    SB --> ES
```

**Key implementation decisions:**

| Decision | Why |
|---|---|
| Persist `currentTerm` and `votedFor` **before** responding to RPCs | Prevents voting twice if the node crashes and restarts mid-election |
| Only commit entries from the **current term** (§5.4.2) | Prevents the "Figure 8" scenario where a previous term's entry is incorrectly committed |
| Randomise election timeouts | Prevents persistent split votes in even-sized clusters |
| `ConflictTerm` / `ConflictIndex` in AppendEntries reply | Reduces log sync from O(n) round-trips to O(1) per inconsistent segment |
| Atomic write-rename in `FileStorage` | Prevents partial writes corrupting durable state on crash |

---

## References

| Resource | Description |
|---|---|
| [Raft Paper](https://raft.github.io/raft.pdf) | Original paper - Ongaro & Ousterhout, 2014 |
| [Extended Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) | Full version with membership changes and snapshots |
| [Raft Visualization](https://raft.github.io/) | Interactive step-by-step animation |
| [MIT 6.824](https://pdos.csail.mit.edu/6.824/) | Distributed Systems course (test structure inspiration) |
| [TLA+ Spec](https://github.com/ongardie/raft.tla) | Formal specification used to verify the algorithm |

---

## License

MIT - see [LICENSE](LICENSE) for details.

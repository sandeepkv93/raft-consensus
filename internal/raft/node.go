package raft

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Transport is the interface a Raft node uses to communicate with peers.
// Implementations can be in-process (for tests) or network-based (TCP/gRPC).
type Transport interface {
	// RequestVote sends a RequestVote RPC to the target node.
	RequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	// AppendEntries sends an AppendEntries RPC to the target node.
	AppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	// InstallSnapshot sends an InstallSnapshot RPC to the target node.
	InstallSnapshot(target string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Storage is the interface for persisting durable Raft state.
// Must survive crashes and restarts. Raft requires at minimum:
//   - currentTerm (must be persisted before responding to RPCs)
//   - votedFor    (must be persisted before responding to RPCs)
//   - log         (all entries, including index and term)
type Storage interface {
	// SaveState persists the hard state (term, vote, log).
	SaveState(state PersistentState) error
	// LoadState restores the hard state on restart.
	LoadState() (PersistentState, error)
	// SaveSnapshot persists a snapshot.
	SaveSnapshot(snap Snapshot) error
	// LoadSnapshot loads the latest snapshot (returns nil if none).
	LoadSnapshot() (*Snapshot, error)
}

// PersistentState is the durable portion of Raft state (§5.4).
type PersistentState struct {
	CurrentTerm uint64     `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// Snapshot bundles a state machine snapshot with Raft metadata.
type Snapshot struct {
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

// Node is a single member of a Raft cluster.
//
// Lifecycle:
//
//	NewNode → Start() → [running] → Stop()
//
// The node communicates with peers via Transport and persists state via Storage.
// Applied log entries are delivered to the caller through the ApplyCh channel.
type Node struct {
	mu sync.Mutex

	// Identity
	id    string
	peers []string // IDs of all other nodes in the cluster

	// Config
	cfg Config

	// External interfaces
	transport Transport
	storage   Storage
	applyCh   chan<- ApplyMsg
	logger    *slog.Logger

	// ── Persistent state (must survive restarts) ──────────────────────────
	currentTerm uint64
	votedFor    string
	log         []LogEntry // 1-indexed; log[0] is a sentinel (index=0, term=0)

	// ── Volatile state on all servers ─────────────────────────────────────
	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied to state machine

	// ── Volatile state on leaders (re-initialised after election) ─────────
	nextIndex  map[string]uint64 // for each peer: next log index to send
	matchIndex map[string]uint64 // for each peer: highest index known replicated

	// ── Role ──────────────────────────────────────────────────────────────
	state    NodeState
	leaderID string

	// ── Timers ────────────────────────────────────────────────────────────
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// ── Snapshot state ────────────────────────────────────────────────────
	snapshotIndex uint64 // last index included in a snapshot
	snapshotTerm  uint64 // term of snapshotIndex

	// ── Lifecycle ─────────────────────────────────────────────────────────
	stopped     int32      // atomic flag; 1 = stopped
	stopCh      chan struct{}
	applySignal chan struct{} // buffered; signals applier goroutine

	// ── Metrics (simple counters, exported via GetStats) ──────────────────
	stats Stats
}

// Stats holds simple diagnostic counters.
type Stats struct {
	ElectionsStarted  uint64
	VotesGranted      uint64
	AppendEntriesSent uint64
	SnapshotsTaken    uint64
}

// NewNode creates a Raft node but does not start it.
//
//   - id: this node's identifier (must be unique in the cluster)
//   - peers: IDs of all OTHER nodes (excludes self)
//   - transport: RPC transport layer
//   - storage: durable state storage
//   - applyCh: caller receives applied commands here; must be drained promptly
//   - cfg: optional config overrides (pass zero value for defaults)
func NewNode(
	id string,
	peers []string,
	transport Transport,
	storage Storage,
	applyCh chan<- ApplyMsg,
	cfg Config,
) *Node {
	if cfg.HeartbeatInterval == 0 {
		cfg = DefaultConfig()
	}

	n := &Node{
		id:          id,
		peers:       peers,
		cfg:         cfg,
		transport:   transport,
		storage:     storage,
		applyCh:     applyCh,
		logger:      slog.Default().With("node", id),
		log:         []LogEntry{{Index: 0, Term: 0}}, // sentinel at index 0
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		state:       Follower,
		stopCh:      make(chan struct{}),
		applySignal: make(chan struct{}, 1),
	}

	// Restore durable state if available.
	if ps, err := storage.LoadState(); err == nil {
		n.currentTerm = ps.CurrentTerm
		n.votedFor = ps.VotedFor
		if len(ps.Log) > 0 {
			n.log = ps.Log
		}
		n.logger.Info("Restored persistent state",
			"term", n.currentTerm,
			"votedFor", n.votedFor,
			"logLen", len(n.log))
	}

	// Restore snapshot if available.
	if snap, err := storage.LoadSnapshot(); err == nil && snap != nil {
		n.snapshotIndex = snap.LastIncludedIndex
		n.snapshotTerm = snap.LastIncludedTerm
		n.commitIndex = snap.LastIncludedIndex
		n.lastApplied = snap.LastIncludedIndex
		n.logger.Info("Restored snapshot",
			"lastIndex", snap.LastIncludedIndex,
			"lastTerm", snap.LastIncludedTerm)
	}

	return n
}

// Start launches the node's background goroutines. Safe to call once.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.applyLoop()
	n.logger.Info("Node started", "peers", n.peers)
}

// Stop shuts down the node. Idempotent.
func (n *Node) Stop() {
	if atomic.CompareAndSwapInt32(&n.stopped, 0, 1) {
		close(n.stopCh)
		n.mu.Lock()
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}
		n.mu.Unlock()
		n.logger.Info("Node stopped")
	}
}

// IsStopped returns true after Stop() has been called.
func (n *Node) IsStopped() bool {
	return atomic.LoadInt32(&n.stopped) == 1
}

// ID returns the node's identifier.
func (n *Node) ID() string { return n.id }

// State returns the current role (Follower/Candidate/Leader).
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID returns the ID of the known leader (empty if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// IsLeader is a convenience wrapper.
func (n *Node) IsLeader() bool {
	return n.State() == Leader
}

// GetStats returns a snapshot of internal counters.
func (n *Node) GetStats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// Propose submits a command to the Raft log. Returns the expected
// (index, term) and whether this node is the leader. If not leader,
// the caller should redirect to LeaderID().
//
// The command is NOT yet committed when Propose returns. The caller
// must watch ApplyCh for ApplyMsg.CommandIndex == returned index.
func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, 0, false
	}

	term = n.currentTerm
	index = n.lastLogIndex() + 1

	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = index
	n.persistState()

	n.logger.Debug("New proposal", "index", index, "term", term)

	// Trigger immediate replication.
	go n.broadcastAppendEntries()

	return index, term, true
}

// ─────────────────────────────────────────────────────────────────────────────
// RPC Handlers (called by the transport layer on receipt of RPCs)
// ─────────────────────────────────────────────────────────────────────────────

// HandleRequestVote processes an incoming RequestVote RPC (§5.2, §5.4).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	// Rule 1: Reject if candidate's term < ours.
	if args.Term < n.currentTerm {
		return reply
	}

	// Rule 2: If we see a higher term, convert to follower.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, "")
	}
	reply.Term = n.currentTerm

	// Rule 3: Vote if (a) we haven't voted or voted for this candidate, AND
	//         (b) candidate's log is at least as up-to-date as ours (§5.4.1).
	alreadyVoted := n.votedFor != "" && n.votedFor != args.CandidateID
	if alreadyVoted {
		return reply
	}

	if !n.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return reply
	}

	// Grant the vote.
	n.votedFor = args.CandidateID
	n.persistState()
	reply.VoteGranted = true
	atomic.AddUint64(&n.stats.VotesGranted, 1)

	// Granting a vote resets the election timer (we know there's a viable leader).
	n.resetElectionTimer()

	n.logger.Info("Granted vote", "to", args.CandidateID, "term", args.Term)
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC (§5.3).
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	// Rule 1: Reject if leader's term < ours.
	if args.Term < n.currentTerm {
		return reply
	}

	// Rule 2: Higher term → become follower.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, args.LeaderID)
	} else {
		// Same term heartbeat/replication — refresh our knowledge of the leader.
		n.leaderID = args.LeaderID
		if n.state == Candidate {
			n.becomeFollower(args.Term, args.LeaderID)
		}
	}
	reply.Term = n.currentTerm

	// Valid AppendEntries resets the election timer.
	n.resetElectionTimer()

	// Rule 3: Log consistency check.
	// Reject if we don't have an entry at PrevLogIndex with PrevLogTerm.
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex < n.snapshotIndex {
			// PrevLogIndex is before our snapshot; we need a snapshot install.
			reply.ConflictIndex = n.snapshotIndex + 1
			return reply
		}
		localIdx := n.logIndexToSlice(args.PrevLogIndex)
		if localIdx < 0 || localIdx >= len(n.log) {
			// We don't have PrevLogIndex at all.
			reply.ConflictIndex = n.lastLogIndex() + 1
			return reply
		}
		if n.log[localIdx].Term != args.PrevLogTerm {
			// Conflict: find the first index of the conflicting term for fast rollback.
			conflictTerm := n.log[localIdx].Term
			conflictIdx := args.PrevLogIndex
			for conflictIdx > n.snapshotIndex+1 {
				si := n.logIndexToSlice(conflictIdx - 1)
				if si < 0 || n.log[si].Term != conflictTerm {
					break
				}
				conflictIdx--
			}
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = conflictIdx
			return reply
		}
	}

	// Rule 4: Append new entries, replacing any conflicting ones.
	insertIdx := n.logIndexToSlice(args.PrevLogIndex + 1)
	for i, entry := range args.Entries {
		si := insertIdx + i
		if si < len(n.log) {
			if n.log[si].Term != entry.Term {
				// Conflict: truncate and append.
				n.log = n.log[:si]
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
			// Entry already exists and matches — skip (idempotent).
		} else {
			// Append remaining new entries.
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	if len(args.Entries) > 0 {
		n.persistState()
	}

	// Rule 5: Update commitIndex.
	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		n.commitIndex = min64(args.LeaderCommit, lastNew)
		n.signalApplier()
	}

	reply.Success = true
	return reply
}

// HandleInstallSnapshot processes an InstallSnapshot RPC (§7).
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, args.LeaderID)
	}
	reply.Term = n.currentTerm
	n.resetElectionTimer()

	// Ignore stale snapshots.
	if args.LastIncludedIndex <= n.snapshotIndex {
		return reply
	}

	// Persist snapshot.
	snap := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		n.logger.Error("Failed to save snapshot", "err", err)
		return reply
	}

	// Compact the log: keep any entries past LastIncludedIndex, or reset.
	newLog := []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	if si := n.logIndexToSlice(args.LastIncludedIndex + 1); si > 0 && si < len(n.log) {
		newLog = append(newLog, n.log[si:]...)
	}
	n.log = newLog

	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}

	n.persistState()

	// Notify the state machine to load the new snapshot.
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
		go func() {
			n.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()
	}

	n.logger.Info("Installed snapshot",
		"lastIndex", args.LastIncludedIndex,
		"lastTerm", args.LastIncludedTerm)
	return reply
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot (initiated by the application layer)
// ─────────────────────────────────────────────────────────────────────────────

// TakeSnapshot is called by the application when it has checkpointed its state.
// The node compacts all log entries up to and including index.
//
//   - index: the last applied index included in the snapshot
//   - data:  the opaque snapshot bytes from the application
func (n *Node) TakeSnapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.snapshotIndex {
		return nil // already compacted past this point
	}

	si := n.logIndexToSlice(index)
	if si < 0 || si >= len(n.log) {
		return fmt.Errorf("snapshot index %d out of range", index)
	}

	snapTerm := n.log[si].Term

	snap := Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	// Compact log: keep sentinel + entries after index.
	newLog := []LogEntry{{Index: index, Term: snapTerm}}
	if si+1 < len(n.log) {
		newLog = append(newLog, n.log[si+1:]...)
	}
	n.log = newLog
	n.snapshotIndex = index
	n.snapshotTerm = snapTerm

	n.persistState()
	atomic.AddUint64(&n.stats.SnapshotsTaken, 1)

	n.logger.Info("Took snapshot",
		"lastIndex", index,
		"lastTerm", snapTerm,
		"newLogLen", len(n.log))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Leader Election (§5.2)
// ─────────────────────────────────────────────────────────────────────────────

// startElection transitions this node to Candidate and requests votes from peers.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistState()
	n.resetElectionTimer()

	atomic.AddUint64(&n.stats.ElectionsStarted, 1)

	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	n.logger.Info("Starting election", "term", term)

	votes := int32(1) // vote for self
	majority := int32(n.quorum())

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	for _, peer := range n.peers {
		go func(peer string) {
			reply, err := n.transport.RequestVote(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If we're no longer a candidate for this term, discard.
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term, "")
				return
			}

			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) >= majority {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// State transitions
// ─────────────────────────────────────────────────────────────────────────────

// becomeFollower transitions to Follower state. Must be called with lock held.
func (n *Node) becomeFollower(term uint64, leaderID string) {
	n.logger.Info("Becoming follower", "term", term, "leader", leaderID)
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = leaderID
	n.persistState()
	n.resetElectionTimer()
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}

// becomeLeader transitions to Leader state. Must be called with lock held.
func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	n.logger.Info("Becoming leader", "term", n.currentTerm)
	n.state = Leader
	n.leaderID = n.id

	// Initialise leader volatile state (§5.3).
	nextIdx := n.lastLogIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = n.lastLogIndex()

	// Stop election timer; start heartbeat.
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.scheduleHeartbeat()

	// Send an immediate no-op heartbeat to establish authority.
	go n.broadcastAppendEntries()
}

// ─────────────────────────────────────────────────────────────────────────────
// Log replication (§5.3)
// ─────────────────────────────────────────────────────────────────────────────

// broadcastAppendEntries sends AppendEntries to all peers concurrently.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.sendAppendEntries(peer)
	}
}

// sendAppendEntries sends AppendEntries (or InstallSnapshot if needed) to one peer.
func (n *Node) sendAppendEntries(peer string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]

	// If the peer is too far behind (before our snapshot), send a snapshot.
	if nextIdx <= n.snapshotIndex {
		n.mu.Unlock()
		n.sendInstallSnapshot(peer)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		si := n.logIndexToSlice(prevLogIndex)
		if si >= 0 && si < len(n.log) {
			prevLogTerm = n.log[si].Term
		}
	}

	// Collect entries to send (up to MaxLogEntriesPerRPC).
	var entries []LogEntry
	startSI := n.logIndexToSlice(nextIdx)
	if startSI >= 0 && startSI < len(n.log) {
		end := startSI + n.cfg.MaxLogEntriesPerRPC
		if end > len(n.log) {
			end = len(n.log)
		}
		entries = make([]LogEntry, end-startSI)
		copy(entries, n.log[startSI:end])
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	atomic.AddUint64(&n.stats.AppendEntriesSent, 1)

	reply, err := n.transport.AppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, "")
		return
	}

	if reply.Success {
		// Update matchIndex and nextIndex on success.
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommitIndex()
	} else {
		// Fast log rollback (§5.3 optimization).
		if reply.ConflictTerm > 0 {
			// Find the last entry in our log with ConflictTerm.
			found := uint64(0)
			for i := len(n.log) - 1; i >= 0; i-- {
				if n.log[i].Term == reply.ConflictTerm {
					found = n.log[i].Index
					break
				}
			}
			if found > 0 {
				n.nextIndex[peer] = found + 1
			} else {
				n.nextIndex[peer] = reply.ConflictIndex
			}
		} else if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else {
			if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}
	}
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a lagging peer.
func (n *Node) sendInstallSnapshot(peer string) {
	snap, err := n.storage.LoadSnapshot()
	if err != nil || snap == nil {
		return
	}

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	}
	term := n.currentTerm
	n.mu.Unlock()

	reply, err := n.transport.InstallSnapshot(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, "")
		return
	}

	n.matchIndex[peer] = snap.LastIncludedIndex
	n.nextIndex[peer] = snap.LastIncludedIndex + 1
}

// maybeAdvanceCommitIndex advances commitIndex when a majority has replicated
// an entry from the current term (§5.4.2 — only commit current-term entries).
func (n *Node) maybeAdvanceCommitIndex() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		si := n.logIndexToSlice(idx)
		if si < 0 || si >= len(n.log) {
			continue
		}
		// Only commit entries from the current term (Raft §5.4.2).
		if n.log[si].Term != n.currentTerm {
			break
		}
		// Count replicas (including self).
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.signalApplier()
			break
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Apply loop (sends committed entries to the state machine)
// ─────────────────────────────────────────────────────────────────────────────

// applyLoop runs in a dedicated goroutine, delivering committed entries to applyCh.
// It is the only writer to applyCh (except snapshot installs, which are one-shot).
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySignal:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		idx := n.lastApplied
		si := n.logIndexToSlice(idx)
		if si < 0 || si >= len(n.log) {
			n.mu.Unlock()
			continue
		}
		entry := n.log[si]
		n.mu.Unlock()

		n.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Timer management
// ─────────────────────────────────────────────────────────────────────────────

// resetElectionTimer sets (or resets) a randomised election timeout.
// Must be called with the lock held.
func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := n.randomElectionTimeout()
	n.electionTimer = time.AfterFunc(timeout, func() {
		if n.IsStopped() {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.state != Leader {
			n.startElection()
		}
	})
}

// scheduleHeartbeat starts a periodic heartbeat timer. Only called by the leader.
// Must be called with the lock held.
func (n *Node) scheduleHeartbeat() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	interval := time.Duration(n.cfg.HeartbeatInterval) * time.Millisecond
	n.heartbeatTimer = time.AfterFunc(interval, func() {
		if n.IsStopped() {
			return
		}
		n.mu.Lock()
		isLeader := n.state == Leader
		n.mu.Unlock()
		if isLeader {
			n.broadcastAppendEntries()
			n.mu.Lock()
			if n.state == Leader {
				n.scheduleHeartbeat()
			}
			n.mu.Unlock()
		}
	})
}

// randomElectionTimeout returns a random duration in [min, max].
func (n *Node) randomElectionTimeout() time.Duration {
	lo := n.cfg.ElectionTimeoutMin
	hi := n.cfg.ElectionTimeoutMax
	ms := lo + rand.Intn(hi-lo+1)
	return time.Duration(ms) * time.Millisecond
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// quorum returns the minimum number of nodes required for a majority.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

// lastLogIndex returns the index of the last log entry.
func (n *Node) lastLogIndex() uint64 {
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the term of the last log entry.
func (n *Node) lastLogTerm() uint64 {
	return n.log[len(n.log)-1].Term
}

// logIndexToSlice converts an absolute log index to a slice index.
// Returns -1 if the index is before the snapshot (compacted away).
func (n *Node) logIndexToSlice(logIdx uint64) int {
	// log[0] is the sentinel with index == snapshotIndex.
	// log[i].Index == snapshotIndex + i
	offset := int(logIdx) - int(n.log[0].Index)
	if offset < 0 {
		return -1
	}
	return offset
}

// isCandidateLogUpToDate returns true if the candidate's log is at least
// as up-to-date as ours (§5.4.1 election restriction).
func (n *Node) isCandidateLogUpToDate(candidateLastIdx, candidateLastTerm uint64) bool {
	myLastTerm := n.lastLogTerm()
	myLastIdx := n.lastLogIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIdx >= myLastIdx
}

// persistState saves durable state to storage. Must be called with lock held.
func (n *Node) persistState() {
	ps := PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	if err := n.storage.SaveState(ps); err != nil {
		n.logger.Error("Failed to persist state", "err", err)
	}
}

// signalApplier wakes up the apply loop (non-blocking).
func (n *Node) signalApplier() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// ─────────────────────────────────────────────────────────────────────────────
// Debug helpers
// ─────────────────────────────────────────────────────────────────────────────

// StatusJSON returns a JSON-encoded snapshot of the node's current state.
// Useful for debugging and health endpoints.
func (n *Node) StatusJSON() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()

	status := map[string]any{
		"id":            n.id,
		"state":         n.state.String(),
		"term":          n.currentTerm,
		"leader":        n.leaderID,
		"commitIndex":   n.commitIndex,
		"lastApplied":   n.lastApplied,
		"logLen":        len(n.log),
		"snapshotIndex": n.snapshotIndex,
	}
	b, _ := json.Marshal(status)
	return b
}

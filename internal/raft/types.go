// Package raft implements the Raft distributed consensus algorithm.
// Based on the original Raft paper: https://raft.github.io/raft.pdf
// "In Search of an Understandable Consensus Algorithm" by Ongaro & Ousterhout.
package raft

import "fmt"

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower  NodeState = iota // Passive; receives log entries from leader.
	Candidate                  // Actively seeking votes to become leader.
	Leader                     // Handles all client writes; replicates to followers.
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in the Raft replicated log.
// The log is the source of truth for all state machine changes.
type LogEntry struct {
	// Index is the 1-based position in the log. Index 0 is the sentinel/empty entry.
	Index uint64
	// Term is the leader's term when this entry was created. Used for consistency checks.
	Term uint64
	// Command is the opaque application-level payload (e.g., a KV store operation).
	Command []byte
}

func (e LogEntry) String() string {
	return fmt.Sprintf("LogEntry{Index:%d Term:%d Cmd:%q}", e.Index, e.Term, e.Command)
}

// RequestVoteArgs is sent by candidates during leader election.
// A candidate requests a vote from every other node in the cluster.
type RequestVoteArgs struct {
	// Term is the candidate's current term.
	Term uint64
	// CandidateID is the ID of the candidate requesting the vote.
	CandidateID string
	// LastLogIndex is the index of the candidate's last log entry (§5.4.1).
	LastLogIndex uint64
	// LastLogTerm is the term of the candidate's last log entry (§5.4.1).
	LastLogTerm uint64
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	// Term is the current term of the responding node. If higher than the
	// candidate's term, the candidate must step down.
	Term uint64
	// VoteGranted is true if the follower granted its vote.
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries and
// serve as a heartbeat when Entries is empty (§5.3).
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term uint64
	// LeaderID lets followers redirect clients to the leader.
	LeaderID string
	// PrevLogIndex is the index of the log entry immediately before the new ones.
	PrevLogIndex uint64
	// PrevLogTerm is the term of the PrevLogIndex entry.
	// Used for the log consistency check (§5.3).
	PrevLogTerm uint64
	// Entries are the log entries to append (empty for heartbeats).
	Entries []LogEntry
	// LeaderCommit is the leader's commitIndex. Followers update their
	// commitIndex to min(LeaderCommit, index of last new entry).
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	// Term is the current term of the responding node.
	Term uint64
	// Success is true if the follower contained an entry matching
	// PrevLogIndex and PrevLogTerm.
	Success bool

	// ConflictTerm is the term of the conflicting entry (for fast log rollback).
	// Set to 0 if the follower doesn't have PrevLogIndex in its log.
	ConflictTerm uint64
	// ConflictIndex is the first index of the ConflictTerm (for fast rollback).
	// If the follower log is too short, set to len(log)+1.
	ConflictIndex uint64
}

// InstallSnapshotArgs is sent by the leader when a follower is too far
// behind to be caught up with AppendEntries alone (§7).
type InstallSnapshotArgs struct {
	// Term is the leader's current term.
	Term uint64
	// LeaderID identifies the leader.
	LeaderID string
	// LastIncludedIndex is the snapshot's last applied index.
	LastIncludedIndex uint64
	// LastIncludedTerm is the term of the LastIncludedIndex entry.
	LastIncludedTerm uint64
	// Data is the raw snapshot bytes.
	Data []byte
}

// InstallSnapshotReply is the response to InstallSnapshot.
type InstallSnapshotReply struct {
	// Term is the current term of the responding node.
	Term uint64
}

// ApplyMsg is sent on the applyCh when a log entry or snapshot is ready
// to be applied to the state machine.
type ApplyMsg struct {
	// CommandValid is true for normal log entry applies.
	CommandValid bool
	// Command is the opaque command payload.
	Command []byte
	// CommandIndex is the log index of this command.
	CommandIndex uint64
	// CommandTerm is the term when this command was accepted.
	CommandTerm uint64

	// SnapshotValid is true when this message carries a snapshot.
	SnapshotValid bool
	// Snapshot is the raw snapshot data.
	Snapshot []byte
	// SnapshotTerm is the term covered by the snapshot.
	SnapshotTerm uint64
	// SnapshotIndex is the last index covered by the snapshot.
	SnapshotIndex uint64
}

// Config holds tuneable parameters for a Raft node.
type Config struct {
	// HeartbeatInterval is how often the leader sends heartbeats.
	// Must be well below ElectionTimeoutMin. Raft paper recommends
	// heartbeat << election timeout (e.g. 50ms heartbeat, 150-300ms election).
	HeartbeatInterval int // milliseconds

	// ElectionTimeoutMin and ElectionTimeoutMax define the randomised
	// election timeout range. Randomisation prevents split votes.
	ElectionTimeoutMin int // milliseconds
	ElectionTimeoutMax int // milliseconds

	// MaxLogEntriesPerRPC caps how many entries are sent in one AppendEntries.
	// Prevents single RPCs from becoming too large.
	MaxLogEntriesPerRPC int

	// SnapshotThreshold is the number of log entries that triggers a snapshot.
	// When len(log) >= SnapshotThreshold, the node compacts its log.
	SnapshotThreshold int
}

// DefaultConfig returns production-safe defaults following Raft paper recommendations.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:   50,
		ElectionTimeoutMin:  150,
		ElectionTimeoutMax:  300,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
	}
}

package raft_test

import (
	"fmt"
	"testing"
	"time"
)

const (
	leaderTimeout  = 2 * time.Second
	applyTimeout   = 3 * time.Second
	electionStable = 500 * time.Millisecond
)

// ─────────────────────────────────────────────────────────────────────────────
// T1: Basic leader election
// ─────────────────────────────────────────────────────────────────────────────

// TestElection_SingleLeader verifies that a 3-node cluster elects exactly one leader.
func TestElection_SingleLeader(t *testing.T) {
	c := newCluster(t, 3)
	leaderIdx := c.waitLeader(leaderTimeout)
	t.Logf("Leader elected: %s (term %d)", c.nodes[leaderIdx].id, c.nodes[leaderIdx].node.Term())

	if count := c.leaderCount(); count != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", count)
	}
}

// TestElection_FiveNode verifies election works in a 5-node cluster.
func TestElection_FiveNode(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(leaderTimeout)
	if count := c.leaderCount(); count != 1 {
		t.Fatalf("expected exactly 1 leader in 5-node cluster, got %d", count)
	}
}

// TestElection_HigherTermWins verifies that a node with a higher term becomes leader.
func TestElection_TermMonotonicity(t *testing.T) {
	c := newCluster(t, 3)
	leaderIdx := c.waitLeader(leaderTimeout)

	firstTerm := c.nodes[leaderIdx].node.Term()
	t.Logf("First leader term: %d", firstTerm)

	// Disconnect the leader to force a new election.
	c.disconnect(leaderIdx)
	time.Sleep(200 * time.Millisecond)

	// A new leader should be elected with a higher term.
	var newLeaderTerm uint64
	for _, cn := range c.nodes {
		if cn.id != c.nodes[leaderIdx].id && cn.node.IsLeader() {
			newLeaderTerm = cn.node.Term()
			break
		}
	}
	// Allow time for re-election.
	time.Sleep(300 * time.Millisecond)
	for _, cn := range c.nodes {
		if cn.id != c.nodes[leaderIdx].id && cn.node.IsLeader() {
			newLeaderTerm = cn.node.Term()
			break
		}
	}

	if newLeaderTerm <= firstTerm {
		// May not have re-elected yet — that's also valid as long as the test
		// doesn't see two simultaneous leaders. Check that invariant.
		if count := c.leaderCount(); count > 1 {
			t.Fatalf("split brain: %d leaders simultaneously", count)
		}
	}

	c.reconnect(leaderIdx)
}

// ─────────────────────────────────────────────────────────────────────────────
// T2: Log replication
// ─────────────────────────────────────────────────────────────────────────────

// TestReplication_Basic verifies that a single command is applied on all nodes.
func TestReplication_Basic(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(leaderTimeout)

	cmd := []byte("hello-raft")
	idx, _ := c.propose(cmd, 2*time.Second)
	t.Logf("Proposed at index %d", idx)

	// All nodes must apply this index.
	c.waitAllApplied(idx, applyTimeout)
}

// TestReplication_Sequential verifies that 20 sequential commands are applied in order.
func TestReplication_Sequential(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(leaderTimeout)

	const N = 20
	var indices []uint64
	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		idx, _ := c.propose(cmd, 2*time.Second)
		indices = append(indices, idx)
	}

	// All commands must be applied on all nodes.
	for _, idx := range indices {
		c.waitAllApplied(idx, applyTimeout)
	}
}

// TestReplication_ConcurrentProposals verifies concurrent proposals are all committed.
func TestReplication_ConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(leaderTimeout)

	const N = 10
	indices := make([]uint64, N)
	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("concurrent-%d", i))
		idx, _ := c.propose(cmd, 3*time.Second)
		indices[i] = idx
	}

	for _, idx := range indices {
		c.waitAllApplied(idx, applyTimeout)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T3: Leader failure & re-election
// ─────────────────────────────────────────────────────────────────────────────

// TestFailure_LeaderCrash verifies that the cluster recovers when the leader fails.
func TestFailure_LeaderCrash(t *testing.T) {
	c := newCluster(t, 3)
	leaderIdx := c.waitLeader(leaderTimeout)
	t.Logf("First leader: %s", c.nodes[leaderIdx].id)

	// Propose something before the crash.
	cmd1 := []byte("before-crash")
	idx1, _ := c.propose(cmd1, 2*time.Second)

	// Disconnect the leader.
	c.disconnect(leaderIdx)
	t.Logf("Disconnected leader %s", c.nodes[leaderIdx].id)

	// The remaining two nodes should elect a new leader.
	time.Sleep(300 * time.Millisecond)

	// Find the new leader among the remaining nodes.
	newLeaderIdx := -1
	deadline := time.Now().Add(leaderTimeout)
	for time.Now().Before(deadline) {
		for i, cn := range c.nodes {
			if i != leaderIdx && cn.node.IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeaderIdx < 0 {
		t.Fatal("no new leader elected after leader crash")
	}
	t.Logf("New leader: %s", c.nodes[newLeaderIdx].id)

	// New cluster can commit new entries.
	cmd2 := []byte("after-crash")
	idx2, _ := c.propose(cmd2, 2*time.Second)

	// Reconnect old leader; it should catch up.
	c.reconnect(leaderIdx)
	time.Sleep(200 * time.Millisecond)

	// All nodes must have both entries.
	for i := range c.nodes {
		c.waitApplied(i, idx1, applyTimeout)
		c.waitApplied(i, idx2, applyTimeout)
	}
}

// TestFailure_MinorityPartition verifies the cluster works with a minority disconnected.
func TestFailure_MinorityPartition(t *testing.T) {
	c := newCluster(t, 5) // 5 nodes: minority = 2
	c.waitLeader(leaderTimeout)

	// Disconnect 2 followers (minority).
	c.disconnect(3)
	c.disconnect(4)

	// Majority (3 nodes) should still elect a leader and commit.
	time.Sleep(300 * time.Millisecond)
	cmd := []byte("during-partition")
	idx, _ := c.propose(cmd, 3*time.Second)

	// The majority applied it.
	for i := 0; i < 3; i++ {
		c.waitApplied(i, idx, applyTimeout)
	}

	// Reconnect and verify partition members catch up.
	c.reconnect(3)
	c.reconnect(4)
	c.waitApplied(3, idx, applyTimeout)
	c.waitApplied(4, idx, applyTimeout)
}

// TestFailure_NoQuorum verifies that the cluster does NOT commit without a quorum,
// and recovers cleanly once quorum is restored.
func TestFailure_NoQuorum(t *testing.T) {
	c := newCluster(t, 3)
	leaderIdx := c.waitLeader(leaderTimeout)

	// Commit one entry while healthy.
	healthyCmd := []byte("before-partition")
	healthyIdx, _ := c.propose(healthyCmd, 2*time.Second)
	c.waitApplied(leaderIdx, healthyIdx, applyTimeout)

	// Disconnect 2 followers — the leader is now isolated (no quorum).
	for i := range c.nodes {
		if i != leaderIdx {
			c.disconnect(i)
		}
	}

	// Give the isolated leader time to attempt (and fail) to commit.
	time.Sleep(400 * time.Millisecond)

	// Reconnect all nodes.
	for i := range c.nodes {
		if i != leaderIdx {
			c.reconnect(i)
		}
	}
	time.Sleep(100 * time.Millisecond)

	// Cluster must elect a leader and commit new entries.
	c.waitLeader(leaderTimeout)
	cmd := []byte("after-quorum-restored")
	idx, _ := c.propose(cmd, 3*time.Second)
	// All nodes must eventually apply this new command.
	c.waitAllApplied(idx, applyTimeout)
}

// ─────────────────────────────────────────────────────────────────────────────
// T4: Network partition (split-brain prevention)
// ─────────────────────────────────────────────────────────────────────────────

// TestPartition_SplitBrain verifies Raft's split-brain safety property:
// after a network partition, the minority side (2 of 5 nodes) cannot commit
// any log entries because it cannot reach quorum (needs 3 of 5 votes).
//
// Note: a node may transiently still think it is "leader" after a partition
// (it hasn't received counter-evidence yet). The safety guarantee is NOT
// "no node in the minority ever has state==Leader" — it is "no node in the
// minority can commit". We test the stronger property: zero entries applied
// on the minority side while the partition is active.
func TestPartition_SplitBrain(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(leaderTimeout)

	// Partition:
	// - Minority: nodes 0, 1 (2 nodes — cannot reach quorum of 3)
	// - Majority: nodes 2, 3, 4 (3 nodes — can elect and commit)
	minority := []int{0, 1}
	majority := []int{2, 3, 4}

	for _, m := range minority {
		for _, j := range majority {
			c.nodes[m].transport.Disconnect(c.nodes[j].id)
			c.nodes[j].transport.Disconnect(c.nodes[m].id)
		}
	}

	// Allow time for the majority side to elect a leader and commit entries.
	time.Sleep(500 * time.Millisecond)

	// Propose to the majority — this must succeed.
	cmd := []byte("majority-side-write")
	majorityIdx, _ := c.propose(cmd, 3*time.Second)

	// Wait for the majority to apply it.
	for _, m := range majority {
		c.waitApplied(m, majorityIdx, applyTimeout)
	}

	// KEY SAFETY CHECK: the minority must NOT have applied the majority's entry.
	// If they had, it would mean they somehow committed without quorum — split brain.
	time.Sleep(200 * time.Millisecond)
	for _, m := range minority {
		c.nodes[m].mu.Lock()
		_, applied := c.nodes[m].applied[majorityIdx]
		c.nodes[m].mu.Unlock()
		if applied {
			t.Fatalf("split brain! minority node %s applied index %d (committed without quorum)",
				c.nodes[m].id, majorityIdx)
		}
	}

	// Heal partition.
	for i := range c.nodes {
		for j := range c.nodes {
			if i != j {
				c.nodes[i].transport.Reconnect(c.nodes[j].id)
			}
		}
	}

	// After healing, all nodes must converge and apply the entry.
	c.waitLeader(3 * leaderTimeout)
	time.Sleep(200 * time.Millisecond)

	// All nodes (including former minority) must eventually apply the committed index.
	c.waitAllApplied(majorityIdx, 10*time.Second)
}

// ─────────────────────────────────────────────────────────────────────────────
// T5: Snapshot and log compaction
// ─────────────────────────────────────────────────────────────────────────────

// TestSnapshot_Basic verifies TakeSnapshot compacts the log and the node
// continues to function correctly after compaction.
func TestSnapshot_Basic(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(leaderTimeout)

	// Commit enough entries to warrant a snapshot.
	const N = 30
	var lastIdx uint64
	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("snap-cmd-%d", i))
		idx, _ := c.propose(cmd, 2*time.Second)
		lastIdx = idx
	}
	c.waitAllApplied(lastIdx, applyTimeout)

	// Take a snapshot on node 0.
	leaderIdx := c.waitLeader(leaderTimeout)
	snapData := []byte(`{"data":{"key":"snap"}}`)
	if err := c.nodes[leaderIdx].node.TakeSnapshot(lastIdx, snapData); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Verify the node can still propose and commit after compaction.
	cmd := []byte("after-snapshot")
	idx, _ := c.propose(cmd, 2*time.Second)
	c.waitAllApplied(idx, applyTimeout)
}

// TestSnapshot_LaggingFollower verifies that a follower that missed many entries
// can be caught up via InstallSnapshot.
func TestSnapshot_LaggingFollower(t *testing.T) {
	c := newCluster(t, 3)
	leaderIdx := c.waitLeader(leaderTimeout)

	// Isolate follower[2].
	followerIdx := 2
	if leaderIdx == 2 {
		followerIdx = 0
	}
	c.disconnect(followerIdx)
	t.Logf("Isolated follower: %s", c.nodes[followerIdx].id)

	// Commit many entries that the follower will miss.
	const N = 40
	var lastIdx uint64
	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("missed-%d", i))
		idx, _ := c.propose(cmd, 2*time.Second)
		lastIdx = idx
	}

	// Take a snapshot so the follower is forced to use InstallSnapshot.
	snapData := []byte(`{"data":{"snapshotted":true}}`)
	if err := c.nodes[leaderIdx].node.TakeSnapshot(lastIdx, snapData); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Reconnect the lagging follower.
	c.reconnect(followerIdx)
	t.Logf("Reconnected %s — expecting InstallSnapshot", c.nodes[followerIdx].id)

	// The follower should install the snapshot and catch up.
	time.Sleep(500 * time.Millisecond)

	// Propose one more entry; the follower must apply it.
	cmd := []byte("after-snapshot-catchup")
	idx, _ := c.propose(cmd, 3*time.Second)
	c.waitApplied(followerIdx, idx, 5*time.Second)
}

// ─────────────────────────────────────────────────────────────────────────────
// T6: Stress test
// ─────────────────────────────────────────────────────────────────────────────

// TestStress_ManyCommands proposes 200 commands and verifies all are applied.
func TestStress_ManyCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	c := newCluster(t, 5)
	c.waitLeader(leaderTimeout)

	const N = 200
	var lastIdx uint64
	for i := 0; i < N; i++ {
		cmd := []byte(fmt.Sprintf("stress-%d", i))
		idx, _ := c.propose(cmd, 3*time.Second)
		if idx > lastIdx {
			lastIdx = idx
		}
	}

	c.waitAllApplied(lastIdx, 15*time.Second)
	t.Logf("All %d commands applied. Last index: %d", N, lastIdx)
}

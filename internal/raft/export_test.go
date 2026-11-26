package raft

// Test-only state injection. These helpers let the Figure 8 and replication
// tests construct specific leader/follower histories without driving a full
// election, and they never run in production builds.

import (
	"testing"

	"raftkv/internal/sim"
)

func setFollowerTerm(n *Node, term uint64, vote sim.NodeID) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = vote
	n.durableTerm = term
	n.durableVotedFor = vote
	n.selfVoted = false
}

func setLeaderState(t *testing.T, n *Node, term uint64, entries []Entry, durableLast uint64, match, next []uint64, commit, applied uint64) {
	t.Helper()
	n.role = Leader
	n.currentTerm = term
	n.durableTerm = term
	n.votedFor = n.id
	n.durableVotedFor = n.id
	n.selfVoted = true
	n.leaderID = n.id
	n.log = append([]Entry{{Index: 0, Term: 0, Kind: EntrySentinel}}, cloneEntries(entries)...)
	for i, e := range n.log {
		if e.Index != uint64(i) {
			t.Fatalf("injected log entry %d has index %d", i, e.Index)
		}
	}
	n.durableLastIndex = durableLast
	n.matchIndex = cloneU64(match)
	n.nextIndex = cloneU64(next)
	n.commitIndex = commit
	n.lastApplied = applied
	n.pendingClients = make(map[uint64]clientRef)
	n.cancelElectionTimer()
	n.cancelHeartbeatTimer()
}

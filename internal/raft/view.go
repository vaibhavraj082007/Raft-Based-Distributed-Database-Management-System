package raft

import (
	"fmt"
	"sort"
	"strings"

	"raftkv/internal/sim"
)

// View is a read-only snapshot of a live node for invariants. Log aliases the
// node's internal slice and is valid only during the View call.
type View struct {
	ID               sim.NodeID
	Incarnation      uint64
	Recovering       bool
	Role             Role
	Term             uint64
	DurableTerm      uint64
	VotedFor         sim.NodeID
	DurableVotedFor  sim.NodeID
	LeaderID         sim.NodeID
	CommitIndex      uint64
	LastApplied      uint64
	LastLogIndex     uint64
	LastLogTerm      uint64
	DurableLastIndex uint64
	Busy             bool
	Log              []Entry
}

// View returns the current node state without copying the log or consuming
// randomness.
func (n *Node) View() View {
	return View{
		ID:               n.id,
		Incarnation:      n.incarnation,
		Recovering:       n.recovering,
		Role:             n.role,
		Term:             n.currentTerm,
		DurableTerm:      n.durableTerm,
		VotedFor:         n.votedFor,
		DurableVotedFor:  n.durableVotedFor,
		LeaderID:         n.leaderID,
		CommitIndex:      n.commitIndex,
		LastApplied:      n.lastApplied,
		LastLogIndex:     n.lastLogIndex(),
		LastLogTerm:      n.lastLogTerm(),
		DurableLastIndex: n.durableLastIndex,
		Busy:             n.busy,
		Log:              n.log,
	}
}

// Snapshot is a canonical encoding of all behavior-affecting state. It never
// iterates a map and never includes pointers or wall-clock data.
func (n *Node) Snapshot() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "raft id=%d inc=%d role=%d term=%d vote=%d dterm=%d dvote=%d leader=%d commit=%d applied=%d dlast=%d logbytes=%d recovering=%t mrec=%t lrec=%t busy=%t next=%d et=%d ht=%d self=%t\n",
		n.id, n.incarnation, n.role, n.currentTerm, n.votedFor,
		n.durableTerm, n.durableVotedFor, n.leaderID, n.commitIndex,
		n.lastApplied, n.durableLastIndex, n.logBytes, n.recovering,
		n.metaRecovered, n.logRecovered, n.busy, n.nextIO,
		n.electionTimer, n.heartbeatTimer, n.selfVoted)
	fmt.Fprintf(&b, "cfg hb=%d emin=%d emax=%d maxappend=%d\n",
		n.cfg.HeartbeatInterval, n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax,
		n.cfg.MaxAppendEntries)
	fmt.Fprintf(&b, "peers %d", len(n.peers))
	for _, peer := range n.peers {
		fmt.Fprintf(&b, " %d", peer)
	}
	b.WriteByte('\n')
	fmt.Fprintf(&b, "log %d\n", len(n.log))
	writeSnapshotEntries(&b, n.log)
	fmt.Fprintf(&b, "votes %d\n", len(n.votesGranted))
	for i, granted := range n.votesGranted {
		fmt.Fprintf(&b, "%d %t\n", i, granted)
	}
	fmt.Fprintf(&b, "next %d\n", len(n.nextIndex))
	for i, index := range n.nextIndex {
		fmt.Fprintf(&b, "%d %d\n", i, index)
	}
	fmt.Fprintf(&b, "match %d\n", len(n.matchIndex))
	for i, index := range n.matchIndex {
		fmt.Fprintf(&b, "%d %d\n", i, index)
	}

	keys := make([]uint64, 0, len(n.pendingClients))
	for index := range n.pendingClients {
		keys = append(keys, index)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	fmt.Fprintf(&b, "clients %d\n", len(keys))
	for _, index := range keys {
		ref := n.pendingClients[index]
		fmt.Fprintf(&b, "%d %d %d\n", index, ref.ClientID, ref.Seq)
	}

	if n.recoveredMeta != nil || n.recoveredLog != nil {
		fmt.Fprintf(&b, "recovered meta=%x log=%x\n", n.recoveredMeta, n.recoveredLog)
	} else {
		b.WriteString("recovered none\n")
	}

	if p := n.pending; p != nil {
		fmt.Fprintf(&b, "pending kind=%d action=%d phase=%d write=%d sync=%d mterm=%d mvote=%d grant=%t reset=%t\n",
			p.kind, p.action, p.phase, p.tagWrite, p.tagSync,
			p.metaTerm, p.metaVote, p.grant, p.resetElection)
		fmt.Fprintf(&b, "request %d %d %d %d %d %d %d %d\n",
			p.request.peer, p.request.term, p.request.prevIndex,
			p.request.prevTerm, p.request.leaderCommit,
			p.request.clientID, p.request.clientSeq, len(p.request.entries))
		writeSnapshotEntries(&b, p.request.entries)
		fmt.Fprintf(&b, "batch %d %d\n", p.batch.baseIndex, len(p.batch.entries))
		writeSnapshotEntries(&b, p.batch.entries)
	} else {
		b.WriteString("pending none\n")
	}

	fmt.Fprintf(&b, "inbox %d\n", len(n.inbox))
	for _, in := range n.inbox {
		if in.kind == inboundMessage {
			fmt.Fprintf(&b, "msg %d %d %d %s %x\n",
				in.kind, in.msg.From, in.msg.To, in.msg.Kind, in.msg.Payload)
			continue
		}
		fmt.Fprintf(&b, "timer %d %d %d\n", in.kind, in.timer.ID, in.timer.Tag)
	}
	return []byte(b.String())
}

func writeSnapshotEntries(b *strings.Builder, entries []Entry) {
	for _, entry := range entries {
		fmt.Fprintf(b, "%d %d %d %x\n", entry.Index, entry.Term, entry.Kind, entry.Data)
	}
}

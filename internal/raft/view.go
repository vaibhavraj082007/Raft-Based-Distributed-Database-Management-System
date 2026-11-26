package raft

import (
	"fmt"
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
	fmt.Fprintf(&b, "raft id=%d inc=%d role=%d term=%d vote=%d dterm=%d dvote=%d leader=%d commit=%d applied=%d dlast=%d recovering=%t busy=%t next=%d et=%d ht=%d self=%t\n",
		n.id, n.incarnation, n.role, n.currentTerm, n.votedFor,
		n.durableTerm, n.durableVotedFor, n.leaderID, n.commitIndex,
		n.lastApplied, n.durableLastIndex, n.recovering, n.busy, n.nextIO,
		n.electionTimer, n.heartbeatTimer, n.selfVoted)
	fmt.Fprintf(&b, "cfg hb=%d emin=%d emax=%d\n",
		n.cfg.HeartbeatInterval, n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax)
	fmt.Fprintf(&b, "peers %d", len(n.peers))
	for _, peer := range n.peers {
		fmt.Fprintf(&b, " %d", peer)
	}
	b.WriteByte('\n')
	fmt.Fprintf(&b, "log %d\n", len(n.log))
	for _, entry := range n.log {
		fmt.Fprintf(&b, "%d %d %d %x\n", entry.Index, entry.Term, entry.Kind, entry.Data)
	}
	fmt.Fprintf(&b, "votes %d\n", len(n.votesGranted))
	for i, granted := range n.votesGranted {
		fmt.Fprintf(&b, "%d %t\n", i, granted)
	}
	if p := n.pending; p != nil {
		fmt.Fprintf(&b, "pending action=%d phase=%d write=%d sync=%d term=%d vote=%d peer=%d grant=%t success=%t restart=%t\n",
			p.action, p.phase, p.tagWrite, p.tagSync, p.term, p.vote,
			p.peer, p.grant, p.success, p.restartTimer)
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

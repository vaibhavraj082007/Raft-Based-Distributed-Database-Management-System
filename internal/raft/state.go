// Package raft implements the Stage 2 Raft node as a deterministic actor.
//
// Stage 2A covers leader election, empty AppendEntries heartbeats, crash-safe
// currentTerm/votedFor persistence, and the per-step Election Safety
// invariant. Log replication, commit, apply, client commands, and the
// Troublemaker scenario are deliberately absent.
package raft

import (
	"fmt"

	"raftkv/internal/sim"
)

// Role is the Raft server state from Figure 2.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// EntryKind identifies a log entry. Stage 2A keeps only the sentinel; Stage 2B
// adds no-op and command entries.
type EntryKind uint8

const (
	EntrySentinel EntryKind = iota
	EntryNoop
	EntryCommand
)

func (k EntryKind) String() string {
	switch k {
	case EntrySentinel:
		return "sentinel"
	case EntryNoop:
		return "noop"
	case EntryCommand:
		return "command"
	default:
		return fmt.Sprintf("entry-kind(%d)", uint8(k))
	}
}

// Entry is one Raft log entry. Index 0 is always the sentinel.
type Entry struct {
	Index uint64
	Term  uint64
	Kind  EntryKind
	Data  []byte
}

// Config is fixed per node for the lifetime of a simulation. Peers excludes
// ID, is sorted ascending, and contains no duplicates.
type Config struct {
	ID                 sim.NodeID
	Peers              []sim.NodeID
	HeartbeatInterval  sim.Tick
	ElectionTimeoutMin sim.Tick
	ElectionTimeoutMax sim.Tick
}

func normalizeConfig(cfg Config) Config {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 3
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 12
	}
	if cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMax = 24
	}
	return cfg
}

func validateConfig(cfg Config) {
	if cfg.ID == 0 {
		panic("raft: node ID must be non-zero")
	}
	if cfg.HeartbeatInterval < 1 {
		panic("raft: HeartbeatInterval must be >= 1")
	}
	if cfg.ElectionTimeoutMin < 3*cfg.HeartbeatInterval {
		panic("raft: ElectionTimeoutMin must be at least 3*HeartbeatInterval")
	}
	if cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		panic("raft: ElectionTimeoutMax must be >= ElectionTimeoutMin")
	}
	var prev sim.NodeID
	for _, peer := range cfg.Peers {
		if peer == 0 {
			panic("raft: peer ID must be non-zero")
		}
		if peer == cfg.ID {
			panic("raft: peer list must not contain the node itself")
		}
		if prev != 0 && peer <= prev {
			panic("raft: peers must be sorted ascending and unique")
		}
		prev = peer
	}
}

const (
	timerTagElection  uint64 = 1
	timerTagHeartbeat uint64 = 2
)

type persistAction uint8

const (
	actionNone persistAction = iota
	actionElectionSelfVote
	actionVoteReply
	actionAppendReply
	actionStepDown
)

type pendingPersist struct {
	action       persistAction
	phase        uint8 // 1 = Replace issued, 2 = Sync issued
	tagWrite     uint64
	tagSync      uint64
	term         uint64
	vote         sim.NodeID
	peer         sim.NodeID
	grant        bool
	success      bool
	restartTimer bool
}

type inboundKind uint8

const (
	inboundMessage inboundKind = iota
	inboundTimer
)

type inbound struct {
	kind  inboundKind
	msg   sim.Message
	timer sim.TimerEvent
}

// Node is a passive Raft actor. All callbacks run on the simulator goroutine.
type Node struct {
	env         sim.Env
	cfg         Config
	id          sim.NodeID
	peers       []sim.NodeID
	incarnation uint64

	// Persistent state. durableTerm/durableVotedFor are the copies known to
	// have survived a Sync.
	currentTerm     uint64
	votedFor        sim.NodeID
	durableTerm     uint64
	durableVotedFor sim.NodeID

	// Log state. Stage 2A has only the sentinel and no log persistence.
	log              []Entry
	durableLastIndex uint64
	commitIndex      uint64
	lastApplied      uint64

	// Volatile state.
	role         Role
	leaderID     sim.NodeID
	votesGranted []bool
	selfVoted    bool

	electionTimer  sim.TimerID
	heartbeatTimer sim.TimerID

	// Boot recovery.
	recovering bool

	// Serialized persistence: at most one meta batch is in flight.
	busy    bool
	pending *pendingPersist
	inbox   []inbound
	nextIO  uint64
}

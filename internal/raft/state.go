// Package raft implements the Stage 2 Raft node as a deterministic actor.
//
// Stage 2B covers leader no-op entries, append-only log persistence,
// AppendEntries replication with conflict backtracking, commit advancement
// under the Figure 8 current-term rule, and in-order apply. Client command
// handling is included because the 2B protocol depends on it. The Troublemaker
// scenario and scenario-level invariants remain Stage 2C.
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

// EntryKind identifies a log entry.
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
	MaxAppendEntries   int
	Apply              func(Entry)
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
	if cfg.MaxAppendEntries == 0 {
		cfg.MaxAppendEntries = 64
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
	if cfg.MaxAppendEntries < 1 {
		panic("raft: MaxAppendEntries must be >= 1")
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

type persistKind uint8

const (
	persistMeta persistKind = iota
	persistLog
)

type pendingAction uint8

const (
	actionNone pendingAction = iota
	actionElectionSelfVote
	actionVoteReply
	actionStepDown
	actionAppendAfterTerm
	actionLeaderNoop
	actionLeaderClientAppend
	actionFollowerAppendReply
)

type requestSummary struct {
	peer         sim.NodeID
	term         uint64
	prevIndex    uint64
	prevTerm     uint64
	leaderCommit uint64
	entries      []Entry
	clientID     sim.NodeID
	clientSeq    uint64
}

type logBatch struct {
	baseIndex uint64
	entries   []Entry
}

type pendingPersist struct {
	kind          persistKind
	action        pendingAction
	phase         uint8 // 1 = append/replace issued, 2 = sync issued
	tagWrite      uint64
	tagSync       uint64
	metaTerm      uint64
	metaVote      sim.NodeID
	grant         bool // captured vote decision for actionVoteReply
	resetElection bool // captured timer decision
	request       requestSummary
	batch         logBatch
	encodedLen    uint64
}

type clientRef struct {
	ClientID sim.NodeID
	Seq      uint64
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

	// Log state. durableLastIndex is the last index covered by a synced log
	// batch; logBytes counts the append-only file including obsolete batches.
	log              []Entry
	logBytes         uint64
	durableLastIndex uint64
	commitIndex      uint64
	lastApplied      uint64

	// Volatile state.
	role         Role
	leaderID     sim.NodeID
	votesGranted []bool
	selfVoted    bool

	// Leader replication state, aligned with peers.
	nextIndex  []uint64
	matchIndex []uint64

	electionTimer  sim.TimerID
	heartbeatTimer sim.TimerID

	// Boot recovery.
	recovering    bool
	metaRecovered bool
	logRecovered  bool
	recoveredMeta []byte
	recoveredLog  []byte

	// Serialized persistence: at most one batch in flight.
	busy    bool
	pending *pendingPersist
	inbox   []inbound

	// Leader-side client notifications; volatile by design.
	pendingClients map[uint64]clientRef

	nextIO uint64
}

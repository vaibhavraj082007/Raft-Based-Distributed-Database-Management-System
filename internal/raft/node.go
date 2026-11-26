package raft

import (
	"fmt"

	"raftkv/internal/sim"
)

// NewFactory returns a sim.NodeFactory. Each boot creates a fresh Node and
// increments the incarnation counter; only the simulator disk survives.
func NewFactory(cfg Config) sim.NodeFactory {
	cfg = normalizeConfig(cfg)
	validateConfig(cfg)
	peers := append([]sim.NodeID(nil), cfg.Peers...)
	cfg.Peers = peers
	var incarnation uint64
	return func(env sim.Env) sim.Node {
		incarnation++
		return &Node{
			env:            env,
			cfg:            cfg,
			id:             cfg.ID,
			peers:          append([]sim.NodeID(nil), peers...),
			incarnation:    incarnation,
			log:            []Entry{{Index: 0, Term: 0, Kind: EntrySentinel}},
			votesGranted:   make([]bool, len(peers)),
			pendingClients: make(map[uint64]clientRef),
			nextIO:         firstRuntimeTag - 1,
		}
	}
}

func (n *Node) Start() {
	n.recovering = true
	n.metaRecovered = false
	n.logRecovered = false
	n.recoveredMeta = nil
	n.recoveredLog = nil
	n.env.Read(tagRecoverMeta, metaFile, 0, maxMetaFileSize+1)
	n.env.Read(tagRecoverLog, logFile, 0, maxLogFileSize+1)
}

func (n *Node) HandleMessage(msg sim.Message) {
	n.inbox = append(n.inbox, inbound{kind: inboundMessage, msg: msg})
	n.pump()
}

func (n *Node) HandleTimer(ev sim.TimerEvent) {
	n.inbox = append(n.inbox, inbound{kind: inboundTimer, timer: ev})
	n.pump()
}

func (n *Node) HandleIO(ev sim.IOEvent) {
	if ev.Err != nil {
		panic(fmt.Sprintf("raft: node %d IO %s tag %d: %v", n.id, ev.Op, ev.Tag, ev.Err))
	}
	if n.recovering {
		n.handleRecoveryIO(ev)
		return
	}

	p := n.pending
	if !n.busy || p == nil {
		panic(fmt.Sprintf("raft: node %d IO completion with no pending batch", n.id))
	}
	switch p.phase {
	case 1:
		switch p.kind {
		case persistMeta:
			if ev.Tag != p.tagWrite || ev.Op != sim.IOReplace {
				panic(fmt.Sprintf("raft: node %d expected replace tag=%d, got tag=%d op=%s",
					n.id, p.tagWrite, ev.Tag, ev.Op))
			}
		case persistLog:
			if ev.Tag != p.tagWrite || ev.Op != sim.IOAppend {
				panic(fmt.Sprintf("raft: node %d expected append tag=%d, got tag=%d op=%s",
					n.id, p.tagWrite, ev.Tag, ev.Op))
			}
		default:
			panic(fmt.Sprintf("raft: node %d invalid persist kind %d", n.id, p.kind))
		}
		p.phase = 2
		n.env.Sync(p.tagSync)
	case 2:
		if ev.Tag != p.tagSync || ev.Op != sim.IOSync {
			panic(fmt.Sprintf("raft: node %d expected sync tag=%d, got tag=%d op=%s",
				n.id, p.tagSync, ev.Tag, ev.Op))
		}
		switch p.kind {
		case persistMeta:
			n.durableTerm = p.metaTerm
			n.durableVotedFor = p.metaVote
		case persistLog:
			n.durableLastIndex = p.batch.entries[len(p.batch.entries)-1].Index
			n.logBytes += p.encodedLen
		default:
			panic(fmt.Sprintf("raft: node %d invalid persist kind %d", n.id, p.kind))
		}
		n.busy = false
		n.pending = nil
		n.continuePersist(p)
		n.pump()
	default:
		panic(fmt.Sprintf("raft: node %d invalid persist phase %d", n.id, p.phase))
	}
}

func (n *Node) handleRecoveryIO(ev sim.IOEvent) {
	if ev.Op != sim.IORead {
		panic(fmt.Sprintf("raft: node %d unexpected recovery IO tag=%d op=%s", n.id, ev.Tag, ev.Op))
	}
	switch ev.Tag {
	case tagRecoverMeta:
		if n.metaRecovered {
			panic(fmt.Sprintf("raft: node %d duplicate meta recovery read", n.id))
		}
		if len(ev.Data) > maxMetaFileSize {
			panic(fmt.Sprintf("raft: node %d raft.meta read %d bytes, cap %d",
				n.id, len(ev.Data), maxMetaFileSize))
		}
		n.recoveredMeta = cloneEntryData(ev.Data)
		n.metaRecovered = true
	case tagRecoverLog:
		if n.logRecovered {
			panic(fmt.Sprintf("raft: node %d duplicate log recovery read", n.id))
		}
		if len(ev.Data) > maxLogFileSize {
			panic(fmt.Sprintf("raft: node %d raft.log read %d bytes, cap %d",
				n.id, len(ev.Data), maxLogFileSize))
		}
		n.recoveredLog = cloneEntryData(ev.Data)
		n.logRecovered = true
	default:
		panic(fmt.Sprintf("raft: node %d unexpected recovery IO tag=%d", n.id, ev.Tag))
	}
	if n.metaRecovered && n.logRecovered {
		n.finishRecovery()
	}
}

func (n *Node) pump() {
	if n.recovering || n.busy {
		return
	}
	for len(n.inbox) > 0 {
		in := n.inbox[0]
		n.inbox = n.inbox[1:]
		switch in.kind {
		case inboundMessage:
			n.handleMessage(in.msg)
		case inboundTimer:
			n.handleTimer(in.timer)
		default:
			panic(fmt.Sprintf("raft: node %d invalid inbox kind %d", n.id, in.kind))
		}
		if n.recovering || n.busy {
			return
		}
	}
}

func (n *Node) handleMessage(msg sim.Message) {
	switch msg.Kind {
	case KindVoteRequest:
		n.handleVoteRequest(msg)
	case KindVoteResponse:
		n.handleVoteResponse(msg)
	case KindAppendRequest:
		n.handleAppendRequest(msg)
	case KindAppendResponse:
		n.handleAppendResponse(msg)
	case KindClientRequest:
		n.handleClientRequest(msg)
	case KindClientResponse:
		// Nodes never initiate client responses to other nodes.
	default:
		// Unknown kinds are ignored so later stages can add messages without
		// breaking an older node.
	}
}

func (n *Node) handleTimer(ev sim.TimerEvent) {
	if ev.Tag == timerTagElection && ev.ID == n.electionTimer {
		n.electionTimer = 0
		if n.role == Follower || n.role == Candidate {
			n.startElection()
		}
		return
	}
	if ev.Tag == timerTagHeartbeat && ev.ID == n.heartbeatTimer {
		n.heartbeatTimer = 0
		if n.role == Leader {
			n.broadcastAppendEntries()
			n.scheduleHeartbeatTimer()
		}
	}
}

func (n *Node) scheduleElectionTimer() {
	n.cancelElectionTimer()
	delay := sim.Tick(n.env.IntRange(int(n.cfg.ElectionTimeoutMin), int(n.cfg.ElectionTimeoutMax)))
	n.electionTimer = n.env.After(delay, timerTagElection)
}

func (n *Node) resetElectionTimer() {
	n.scheduleElectionTimer()
}

func (n *Node) cancelElectionTimer() {
	if n.electionTimer != 0 {
		n.env.CancelTimer(n.electionTimer)
		n.electionTimer = 0
	}
}

func (n *Node) scheduleHeartbeatTimer() {
	n.cancelHeartbeatTimer()
	n.heartbeatTimer = n.env.After(n.cfg.HeartbeatInterval, timerTagHeartbeat)
}

func (n *Node) cancelHeartbeatTimer() {
	if n.heartbeatTimer != 0 {
		n.env.CancelTimer(n.heartbeatTimer)
		n.heartbeatTimer = 0
	}
}

// becomeFollower is the shared transition. A higher term clears the vote; a
// same-term demotion preserves it. It never touches the election timer: the
// caller decides whether the node keeps its existing timer or starts one
// because it used to be a leader.
func (n *Node) becomeFollower(term uint64, leader sim.NodeID) {
	if term < n.currentTerm {
		panic(fmt.Sprintf("raft: node %d cannot step down from term %d to %d", n.id, n.currentTerm, term))
	}
	if term > n.currentTerm {
		n.votedFor = 0
	}
	n.role = Follower
	n.currentTerm = term
	n.leaderID = leader
	n.selfVoted = false
	for i := range n.votesGranted {
		n.votesGranted[i] = false
	}
	n.pendingClients = make(map[uint64]clientRef)
	n.cancelHeartbeatTimer()
}

func (n *Node) isPeer(id sim.NodeID) bool {
	for _, peer := range n.peers {
		if peer == id {
			return true
		}
	}
	return false
}

func (n *Node) peerIndex(id sim.NodeID) int {
	for i, peer := range n.peers {
		if peer == id {
			return i
		}
	}
	return -1
}

func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) countVotes() int {
	count := 0
	for _, granted := range n.votesGranted {
		if granted {
			count++
		}
	}
	return count
}

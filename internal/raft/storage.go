package raft

import (
	"fmt"

	"raftkv/internal/sim"
)

func cloneBatch(batch logBatch) logBatch {
	return logBatch{baseIndex: batch.baseIndex, entries: cloneEntries(batch.entries)}
}

func (n *Node) beginMetaPersist(term uint64, vote sim.NodeID, action pendingAction, req requestSummary, grant, resetElection bool) {
	if n.busy {
		panic(fmt.Sprintf("raft: node %d started a second persistence batch", n.id))
	}
	if n.currentTerm != term || n.votedFor != vote {
		panic(fmt.Sprintf("raft: node %d meta persist term/vote %d/%d, current %d/%d",
			n.id, term, vote, n.currentTerm, n.votedFor))
	}
	n.nextIO++
	tagWrite := n.nextIO
	n.nextIO++
	tagSync := n.nextIO

	n.busy = true
	n.pending = &pendingPersist{
		kind:          persistMeta,
		action:        action,
		phase:         1,
		tagWrite:      tagWrite,
		tagSync:       tagSync,
		metaTerm:      term,
		metaVote:      vote,
		grant:         grant,
		resetElection: resetElection,
		request:       cloneRequest(req),
	}
	n.env.Replace(tagWrite, metaFile, encodeMeta(term, vote))
}

func (n *Node) beginLogPersist(batch logBatch, action pendingAction, req requestSummary) {
	if n.busy {
		panic(fmt.Sprintf("raft: node %d started a second persistence batch", n.id))
	}
	if len(batch.entries) == 0 {
		panic(fmt.Sprintf("raft: node %d started an empty log batch", n.id))
	}
	if batch.baseIndex != batch.entries[0].Index {
		panic(fmt.Sprintf("raft: node %d log batch base %d, first entry %d",
			n.id, batch.baseIndex, batch.entries[0].Index))
	}
	for i, e := range batch.entries {
		if e.Index != batch.baseIndex+uint64(i) {
			panic(fmt.Sprintf("raft: node %d log batch entry %d index %d, want %d",
				n.id, i, e.Index, batch.baseIndex+uint64(i)))
		}
	}

	encoded := encodeLogBatch(batch)
	if n.logBytes+uint64(len(encoded)) > maxLogFileSize {
		panic(fmt.Sprintf("raft: node %d raft.log would reach %d bytes, cap %d; Stage 2 does not compact",
			n.id, n.logBytes+uint64(len(encoded)), maxLogFileSize))
	}

	n.nextIO++
	tagWrite := n.nextIO
	n.nextIO++
	tagSync := n.nextIO

	n.busy = true
	n.pending = &pendingPersist{
		kind:       persistLog,
		action:     action,
		phase:      1,
		tagWrite:   tagWrite,
		tagSync:    tagSync,
		request:    cloneRequest(req),
		batch:      cloneBatch(batch),
		encodedLen: uint64(len(encoded)),
	}
	n.env.Append(tagWrite, logFile, encoded)
}

func (n *Node) continuePersist(p *pendingPersist) {
	switch p.kind {
	case persistMeta:
		n.continueMetaPersist(p)
	case persistLog:
		n.continueLogPersist(p)
	default:
		panic(fmt.Sprintf("raft: node %d invalid persist kind %d", n.id, p.kind))
	}
}

func (n *Node) continueMetaPersist(p *pendingPersist) {
	switch p.action {
	case actionElectionSelfVote:
		if n.role != Candidate || n.currentTerm != p.metaTerm || n.votedFor != p.metaVote {
			panic(fmt.Sprintf("raft: node %d election continuation role=%s term=%d vote=%d, want candidate/%d/%d",
				n.id, n.role, n.currentTerm, n.votedFor, p.metaTerm, p.metaVote))
		}
		n.selfVoted = true
		if len(n.peers) == 0 {
			n.becomeLeader()
			return
		}
		for _, peer := range n.peers {
			n.sendVoteRequest(peer)
		}
		n.resetElectionTimer()
	case actionVoteReply:
		if n.currentTerm != p.metaTerm {
			return
		}
		if p.resetElection {
			n.resetElectionTimer()
		}
		n.sendVoteResponse(p.request.peer, p.metaTerm, p.grant)
	case actionStepDown:
		if n.currentTerm != p.metaTerm {
			return
		}
		if p.resetElection {
			n.resetElectionTimer()
		}
	case actionAppendAfterTerm:
		if n.currentTerm != p.metaTerm {
			return
		}
		n.resetElectionTimer()
		n.continueAppendRequest(p.request)
	default:
		panic(fmt.Sprintf("raft: node %d invalid meta persist action %d", n.id, p.action))
	}
}

func (n *Node) continueLogPersist(p *pendingPersist) {
	if len(p.batch.entries) == 0 {
		panic(fmt.Sprintf("raft: node %d log continuation has empty batch", n.id))
	}
	last := p.batch.entries[len(p.batch.entries)-1].Index
	if n.durableLastIndex < last {
		panic(fmt.Sprintf("raft: node %d log continuation durable last %d, batch last %d",
			n.id, n.durableLastIndex, last))
	}

	switch p.action {
	case actionLeaderNoop:
		entry := p.batch.entries[len(p.batch.entries)-1]
		if n.role != Leader || n.currentTerm != entry.Term {
			panic(fmt.Sprintf("raft: node %d no-op continuation role=%s term=%d, entry term=%d",
				n.id, n.role, n.currentTerm, entry.Term))
		}
		n.advanceCommit()
		n.broadcastAppendEntries()
		n.scheduleHeartbeatTimer()
	case actionLeaderClientAppend:
		entry := p.batch.entries[len(p.batch.entries)-1]
		if n.role != Leader || n.currentTerm != entry.Term {
			panic(fmt.Sprintf("raft: node %d client continuation role=%s term=%d, entry term=%d",
				n.id, n.role, n.currentTerm, entry.Term))
		}
		n.pendingClients[entry.Index] = clientRef{ClientID: p.request.clientID, Seq: p.request.clientSeq}
		n.sendClientResponse(p.request.clientID, ClientResponse{
			Term:     n.currentTerm,
			Accepted: true,
			LeaderID: n.id,
			Index:    entry.Index,
		})
		n.advanceCommit()
		n.broadcastAppendEntries()
		n.scheduleHeartbeatTimer()
	case actionFollowerAppendReply:
		if n.role != Follower || n.currentTerm != p.request.term {
			panic(fmt.Sprintf("raft: node %d follower append continuation role=%s term=%d, request term=%d",
				n.id, n.role, n.currentTerm, p.request.term))
		}
		lastNew := p.request.prevIndex + uint64(len(p.request.entries))
		n.advanceFollowerCommit(p.request.leaderCommit, lastNew)
		n.sendAppendResponse(p.request.peer, AppendResponse{
			Term:       n.currentTerm,
			Success:    true,
			MatchIndex: lastNew,
		})
	default:
		panic(fmt.Sprintf("raft: node %d invalid log persist action %d", n.id, p.action))
	}
}

func (n *Node) finishRecovery() {
	term, vote := decodeMeta(n.recoveredMeta)
	log := decodeLogFile(n.recoveredLog)
	if last := log[len(log)-1]; last.Term > term {
		panic(fmt.Sprintf("raft: node %d recovered log term %d ahead of meta term %d",
			n.id, last.Term, term))
	}

	n.currentTerm = term
	n.votedFor = vote
	n.durableTerm = term
	n.durableVotedFor = vote
	n.log = log
	n.durableLastIndex = n.lastLogIndex()
	n.logBytes = uint64(len(n.recoveredLog))
	n.commitIndex = 0
	n.lastApplied = 0
	n.leaderID = 0
	n.role = Follower
	n.nextIndex = make([]uint64, len(n.peers))
	n.matchIndex = make([]uint64, len(n.peers))
	n.votesGranted = make([]bool, len(n.peers))
	n.selfVoted = false
	n.pendingClients = make(map[uint64]clientRef)
	n.electionTimer = 0
	n.heartbeatTimer = 0
	n.busy = false
	n.pending = nil
	n.nextIO = firstRuntimeTag - 1
	n.recovering = false
	n.recoveredMeta = nil
	n.recoveredLog = nil
	n.scheduleElectionTimer()
	n.pump()
}

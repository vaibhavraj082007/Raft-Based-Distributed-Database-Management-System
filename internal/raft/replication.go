package raft

import (
	"bytes"
	"fmt"
	"sort"

	"raftkv/internal/sim"
)

func requestSummaryFromAppend(from sim.NodeID, req AppendRequest) requestSummary {
	return requestSummary{
		peer:         from,
		term:         req.Term,
		prevIndex:    req.PrevLogIndex,
		prevTerm:     req.PrevLogTerm,
		leaderCommit: req.LeaderCommit,
		entries:      cloneEntries(req.Entries),
	}
}

func (n *Node) handleAppendRequest(msg sim.Message) {
	req := decodeAppendRequest(msg.Payload)
	if !n.validateAppendRequest(msg.From, req) {
		return
	}

	if req.Term < n.currentTerm {
		n.sendAppendResponse(msg.From, AppendResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: n.lastLogIndex() + 1,
		})
		return
	}

	summary := requestSummaryFromAppend(msg.From, req)
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, req.LeaderID)
		n.beginMetaPersist(req.Term, 0, actionAppendAfterTerm, summary, false, true)
		return
	}

	n.leaderID = req.LeaderID
	if n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.resetElectionTimer()
	n.continueAppendRequest(summary)
}

func (n *Node) continueAppendRequest(req requestSummary) {
	last := n.lastLogIndex()
	if req.prevIndex > last {
		n.sendAppendResponse(req.peer, AppendResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: last + 1,
		})
		return
	}

	if req.prevIndex >= 1 {
		localTerm := n.log[req.prevIndex].Term
		if localTerm != req.prevTerm {
			if req.prevIndex <= n.commitIndex {
				panic(fmt.Sprintf("raft: node %d committed conflict at index %d: local term %d, request term %d, commit %d",
					n.id, req.prevIndex, localTerm, req.prevTerm, n.commitIndex))
			}
			conflictIndex, ok := n.firstIndexOfTerm(localTerm)
			if !ok {
				panic(fmt.Sprintf("raft: node %d missing first index of local term %d", n.id, localTerm))
			}
			n.sendAppendResponse(req.peer, AppendResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  localTerm,
			})
			return
		}
	}

	base := req.prevIndex + 1
	firstChanged := uint64(0)
	for i, e := range req.entries {
		index := base + uint64(i)
		if index > last {
			firstChanged = index
			break
		}
		local := n.log[index]
		if local.Term == e.Term {
			if local.Kind != e.Kind || !bytes.Equal(local.Data, e.Data) {
				panic(fmt.Sprintf("raft: node %d log matching violation at index %d term %d: local %s/%x, request %s/%x",
					n.id, index, e.Term, local.Kind, local.Data, e.Kind, e.Data))
			}
			continue
		}
		if index <= n.commitIndex {
			panic(fmt.Sprintf("raft: node %d would replace committed index %d: local term %d, request term %d, commit %d",
				n.id, index, local.Term, e.Term, n.commitIndex))
		}
		firstChanged = index
		break
	}

	if firstChanged == 0 {
		lastNew := req.prevIndex + uint64(len(req.entries))
		n.advanceFollowerCommit(req.leaderCommit, lastNew)
		n.sendAppendResponse(req.peer, AppendResponse{
			Term:       n.currentTerm,
			Success:    true,
			MatchIndex: lastNew,
		})
		return
	}

	if firstChanged <= n.commitIndex {
		panic(fmt.Sprintf("raft: node %d would truncate at committed index %d, commit %d",
			n.id, firstChanged, n.commitIndex))
	}
	newEntries := cloneEntries(req.entries[firstChanged-base:])
	n.log = n.log[:firstChanged]
	n.log = append(n.log, newEntries...)

	n.beginLogPersist(logBatch{baseIndex: firstChanged, entries: newEntries}, actionFollowerAppendReply, req)
}

func (n *Node) advanceFollowerCommit(leaderCommit, lastNew uint64) {
	if leaderCommit <= n.commitIndex {
		return
	}
	newCommit := leaderCommit
	if newCommit > lastNew {
		newCommit = lastNew
	}
	if newCommit <= n.commitIndex {
		return
	}
	if newCommit > n.lastLogIndex() {
		panic(fmt.Sprintf("raft: node %d commit %d beyond last log index %d", n.id, newCommit, n.lastLogIndex()))
	}
	n.commitIndex = newCommit
	n.apply()
}

func (n *Node) handleAppendResponse(msg sim.Message) {
	resp := decodeAppendResponse(msg.Payload)
	if !n.isPeer(msg.From) {
		return
	}
	if resp.Term < n.currentTerm {
		return
	}
	if resp.Term > n.currentTerm {
		wasLeader := n.role == Leader
		n.becomeFollower(resp.Term, 0)
		n.beginMetaPersist(resp.Term, 0, actionStepDown,
			requestSummary{peer: msg.From}, false, wasLeader)
		return
	}
	if n.role != Leader {
		return
	}
	idx := n.peerIndex(msg.From)
	if idx < 0 {
		return
	}

	if resp.Success {
		if resp.MatchIndex > n.lastLogIndex() {
			panic(fmt.Sprintf("raft: node %d append success beyond log: match=%d last=%d",
				n.id, resp.MatchIndex, n.lastLogIndex()))
		}
		if resp.MatchIndex > n.matchIndex[idx] {
			n.matchIndex[idx] = resp.MatchIndex
		}
		if n.matchIndex[idx]+1 > n.nextIndex[idx] {
			n.nextIndex[idx] = n.matchIndex[idx] + 1
		}
		n.advanceCommit()
		if n.role == Leader && n.nextIndex[idx] <= n.lastLogIndex() {
			n.sendAppendEntries(idx)
		}
		return
	}

	next := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		if index, ok := n.lastIndexOfTerm(resp.ConflictTerm); ok {
			next = index + 1
		}
	}
	if next < n.matchIndex[idx]+1 {
		next = n.matchIndex[idx] + 1
	}
	if next > n.lastLogIndex()+1 {
		next = n.lastLogIndex() + 1
	}
	if next < n.nextIndex[idx] {
		n.nextIndex[idx] = next
		n.sendAppendEntries(idx)
	}
}

func (n *Node) sendAppendEntries(peerIndex int) {
	if n.role != Leader {
		return
	}
	if peerIndex < 0 || peerIndex >= len(n.peers) {
		panic(fmt.Sprintf("raft: node %d invalid peer index %d", n.id, peerIndex))
	}
	next := n.nextIndex[peerIndex]
	if min := n.matchIndex[peerIndex] + 1; next < min {
		next = min
	}
	lastPlus := n.lastLogIndex() + 1
	if next > lastPlus {
		next = lastPlus
	}
	n.nextIndex[peerIndex] = next

	prev := next - 1
	prevTerm := n.log[prev].Term
	end := next + uint64(n.cfg.MaxAppendEntries)
	if end > lastPlus {
		end = lastPlus
	}
	entries := n.log[int(next):int(end)]
	req := AppendRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.env.Send(n.peers[peerIndex], KindAppendRequest, encodeAppendRequest(req))
}

func (n *Node) broadcastAppendEntries() {
	if n.role != Leader {
		return
	}
	for i := range n.peers {
		n.sendAppendEntries(i)
	}
}

func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	values := make([]uint64, 0, len(n.peers)+1)
	values = append(values, n.durableLastIndex)
	values = append(values, n.matchIndex...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	majority := len(values)/2 + 1
	candidate := values[len(values)-majority]
	if candidate <= n.commitIndex {
		return
	}
	if candidate > n.lastLogIndex() {
		panic(fmt.Sprintf("raft: node %d commit candidate %d beyond last log index %d",
			n.id, candidate, n.lastLogIndex()))
	}
	if n.log[candidate].Term != n.currentTerm {
		return
	}
	n.commitIndex = candidate
	n.apply()
	n.broadcastAppendEntries()
}

func (n *Node) apply() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.lastApplied > n.lastLogIndex() {
			panic(fmt.Sprintf("raft: node %d apply index %d beyond last log index %d",
				n.id, n.lastApplied, n.lastLogIndex()))
		}
		entry := n.log[n.lastApplied]
		if n.cfg.Apply != nil {
			n.cfg.Apply(entry)
		}
		if n.role == Leader {
			if ref, ok := n.pendingClients[entry.Index]; ok {
				n.sendClientResponse(ref.ClientID, ClientResponse{
					Term:      n.currentTerm,
					Committed: true,
					LeaderID:  n.id,
					Index:     entry.Index,
				})
				delete(n.pendingClients, entry.Index)
			}
		}
	}
}

func (n *Node) sendAppendResponse(peer sim.NodeID, resp AppendResponse) {
	n.env.Send(peer, KindAppendResponse, encodeAppendResponse(resp))
}

func (n *Node) validateAppendRequest(from sim.NodeID, req AppendRequest) bool {
	if req.Term < 1 {
		panic(fmt.Sprintf("raft: node %d append request term %d", n.id, req.Term))
	}
	if req.LeaderID == 0 {
		panic(fmt.Sprintf("raft: node %d append request with zero leader", n.id))
	}
	if !n.isPeer(req.LeaderID) {
		return false
	}
	if from != req.LeaderID {
		panic(fmt.Sprintf("raft: node %d append request from %d claims leader %d", n.id, from, req.LeaderID))
	}
	if len(req.Entries) > n.cfg.MaxAppendEntries {
		panic(fmt.Sprintf("raft: node %d append request has %d entries, cap %d",
			n.id, len(req.Entries), n.cfg.MaxAppendEntries))
	}
	if req.PrevLogIndex == 0 && req.PrevLogTerm != 0 {
		panic(fmt.Sprintf("raft: node %d append request has prev term %d at index 0",
			n.id, req.PrevLogTerm))
	}
	if req.PrevLogIndex > 0 && req.PrevLogTerm < 1 {
		panic(fmt.Sprintf("raft: node %d append request has prev index %d with zero term",
			n.id, req.PrevLogIndex))
	}
	if req.PrevLogTerm > req.Term {
		panic(fmt.Sprintf("raft: node %d append request prev term %d exceeds request term %d",
			n.id, req.PrevLogTerm, req.Term))
	}
	if req.PrevLogIndex == ^uint64(0) {
		panic(fmt.Sprintf("raft: node %d append request prev index overflow", n.id))
	}

	expected := req.PrevLogIndex + 1
	prevTerm := req.PrevLogTerm
	for i, e := range req.Entries {
		if e.Index != expected {
			panic(fmt.Sprintf("raft: node %d append entry %d index %d, want %d",
				n.id, i, e.Index, expected))
		}
		if e.Term < 1 {
			panic(fmt.Sprintf("raft: node %d append entry %d has term %d", n.id, i, e.Term))
		}
		if e.Term > req.Term {
			panic(fmt.Sprintf("raft: node %d append entry %d term %d exceeds request term %d",
				n.id, i, e.Term, req.Term))
		}
		if e.Term < prevTerm {
			panic(fmt.Sprintf("raft: node %d append entry %d term %d below previous term %d",
				n.id, i, e.Term, prevTerm))
		}
		if e.Kind != EntryNoop && e.Kind != EntryCommand {
			panic(fmt.Sprintf("raft: node %d append entry %d has kind %s", n.id, i, e.Kind))
		}
		if len(e.Data) > maxEntryBytes {
			panic(fmt.Sprintf("raft: node %d append entry %d data %d exceeds cap %d",
				n.id, i, len(e.Data), maxEntryBytes))
		}
		prevTerm = e.Term
		expected++
	}
	return true
}

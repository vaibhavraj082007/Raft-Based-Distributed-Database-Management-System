package raft

import (
	"fmt"

	"raftkv/internal/sim"
)

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		panic(fmt.Sprintf("raft: node %d missing log sentinel", n.id))
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		panic(fmt.Sprintf("raft: node %d missing log sentinel", n.id))
	}
	return n.log[len(n.log)-1].Term
}

// startElection begins a new term. The self-vote is durable before any
// RequestVote leaves the node.
func (n *Node) startElection() {
	n.cancelElectionTimer()
	n.cancelHeartbeatTimer()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = 0
	n.selfVoted = false
	for i := range n.votesGranted {
		n.votesGranted[i] = false
	}
	n.beginMetaPersist(n.currentTerm, n.votedFor, actionElectionSelfVote, 0, false, false, false)
}

func (n *Node) handleVoteRequest(msg sim.Message) {
	req := decodeVoteRequest(msg.Payload)
	if !n.validateVoteRequest(msg.From, req) {
		return
	}

	wasLeader := n.role == Leader

	if req.Term < n.currentTerm {
		n.sendVoteResponse(msg.From, n.currentTerm, false)
		return
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, 0)
	}

	newTerm := n.currentTerm
	newVote := n.votedFor
	localTerm, localIndex := n.lastLogTerm(), n.lastLogIndex()
	upToDate := req.LastLogTerm > localTerm ||
		(req.LastLogTerm == localTerm && req.LastLogIndex >= localIndex)
	grant := (newVote == 0 || newVote == req.CandidateID) && upToDate
	if grant {
		newVote = req.CandidateID
	}

	restartElectionTimer := grant || (wasLeader && n.role == Follower)

	if newTerm != n.durableTerm || newVote != n.durableVotedFor {
		n.currentTerm = newTerm
		n.votedFor = newVote
		n.beginMetaPersist(newTerm, newVote, actionVoteReply, msg.From, grant, false, restartElectionTimer)
		return
	}

	if restartElectionTimer {
		n.resetElectionTimer()
	}
	n.sendVoteResponse(msg.From, n.currentTerm, grant)
}

func (n *Node) handleVoteResponse(msg sim.Message) {
	resp := decodeVoteResponse(msg.Payload)
	if !n.isPeer(msg.From) {
		return
	}
	if resp.Term < n.currentTerm {
		return
	}
	if resp.Term > n.currentTerm {
		wasLeader := n.role == Leader
		n.becomeFollower(resp.Term, 0)
		n.beginMetaPersist(resp.Term, 0, actionStepDown, msg.From, false, false, wasLeader)
		return
	}
	if n.role != Candidate || !resp.VoteGranted {
		return
	}
	idx := n.peerIndex(msg.From)
	if idx < 0 || n.votesGranted[idx] {
		return
	}
	n.votesGranted[idx] = true
	if 1+n.countVotes() >= n.majority() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendRequest(msg sim.Message) {
	req := decodeAppendRequest(msg.Payload)
	if !n.validateAppendRequest(msg.From, req) {
		return
	}

	if req.Term < n.currentTerm {
		n.sendAppendResponse(msg.From, n.currentTerm, false)
		return
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, req.LeaderID)
		n.beginMetaPersist(req.Term, 0, actionAppendReply, msg.From, false, true, true)
		return
	}

	n.leaderID = req.LeaderID
	if n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.resetElectionTimer()
	n.sendAppendResponse(msg.From, n.currentTerm, true)
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
		n.beginMetaPersist(resp.Term, 0, actionStepDown, msg.From, false, false, wasLeader)
		return
	}
	if n.role != Leader {
		return
	}
	// Stage 2A heartbeats carry no log information. A same-term failure cannot
	// be produced by a correct follower, so it is ignored rather than acted on.
}

// becomeLeader runs only after the election self-vote is durable and a
// majority of votes is present. It starts heartbeats immediately; Stage 2B
// inserts the durable no-op before this point.
func (n *Node) becomeLeader() {
	if n.role != Candidate {
		panic(fmt.Sprintf("raft: node %d became leader from role %s", n.id, n.role))
	}
	if n.currentTerm != n.durableTerm {
		panic(fmt.Sprintf("raft: node %d became leader with term %d, durable %d", n.id, n.currentTerm, n.durableTerm))
	}
	if n.votedFor != n.id || n.durableVotedFor != n.id {
		panic(fmt.Sprintf("raft: node %d became leader with vote %d, durable %d", n.id, n.votedFor, n.durableVotedFor))
	}
	if !n.selfVoted {
		panic(fmt.Sprintf("raft: node %d became leader before its self-vote was durable", n.id))
	}
	if 1+n.countVotes() < n.majority() {
		panic(fmt.Sprintf("raft: node %d became leader with %d votes, needs %d", n.id, 1+n.countVotes(), n.majority()))
	}

	n.role = Leader
	n.leaderID = n.id
	n.cancelElectionTimer()
	n.sendHeartbeats()
	n.scheduleHeartbeatTimer()
}

func (n *Node) sendVoteRequest(peer sim.NodeID) {
	req := VoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	n.env.Send(peer, KindVoteRequest, encodeVoteRequest(req))
}

func (n *Node) sendVoteResponse(peer sim.NodeID, term uint64, granted bool) {
	n.env.Send(peer, KindVoteResponse, encodeVoteResponse(VoteResponse{
		Term:        term,
		VoteGranted: granted,
	}))
}

func (n *Node) sendAppendResponse(peer sim.NodeID, term uint64, success bool) {
	n.env.Send(peer, KindAppendResponse, encodeAppendResponse(AppendResponse{
		Term:    term,
		Success: success,
	}))
}

func (n *Node) sendHeartbeats() {
	if n.role != Leader {
		return
	}
	for _, peer := range n.peers {
		req := AppendRequest{
			Term:     n.currentTerm,
			LeaderID: n.id,
		}
		n.env.Send(peer, KindAppendRequest, encodeAppendRequest(req))
	}
}

func (n *Node) validateVoteRequest(from sim.NodeID, req VoteRequest) bool {
	if req.Term < 1 {
		panic(fmt.Sprintf("raft: node %d vote request term %d", n.id, req.Term))
	}
	if req.CandidateID == 0 {
		panic(fmt.Sprintf("raft: node %d vote request with zero candidate", n.id))
	}
	if !n.isPeer(req.CandidateID) {
		return false
	}
	if from != req.CandidateID {
		panic(fmt.Sprintf("raft: node %d vote request from %d claims candidate %d", n.id, from, req.CandidateID))
	}
	if req.LastLogIndex == 0 && req.LastLogTerm != 0 {
		panic(fmt.Sprintf("raft: node %d vote request has term %d at empty log", n.id, req.LastLogTerm))
	}
	if req.LastLogIndex > 0 && req.LastLogTerm < 1 {
		panic(fmt.Sprintf("raft: node %d vote request has index %d with zero term", n.id, req.LastLogIndex))
	}
	return true
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
	if req.PrevLogIndex != 0 || req.PrevLogTerm != 0 || req.LeaderCommit != 0 {
		panic(fmt.Sprintf("raft: node %d Stage 2A heartbeat has log fields prev=%d/%d commit=%d",
			n.id, req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit))
	}
	return true
}

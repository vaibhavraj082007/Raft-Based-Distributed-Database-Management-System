package raft

import (
	"testing"

	"raftkv/internal/sim"
)

func TestElectionSmoke(t *testing.T) {
	s, ids := newRaftSim(t, 42, 5)
	if err := s.RunUntil(200); err != nil {
		t.Fatalf("RunUntil(200): %v", err)
	}
	leader := singleLeader(t, s, ids)
	if leader.Term < 1 {
		t.Fatalf("leader %d has term %d", leader.ID, leader.Term)
	}
	leaderNode := mustNode(t, s, leader.ID)
	if votes := 1 + leaderNode.countVotes(); votes < leaderNode.majority() {
		t.Fatalf("leader %d has %d votes, needs %d", leader.ID, votes, leaderNode.majority())
	}
	if leaderNode.durableTerm != leaderNode.currentTerm || leaderNode.durableVotedFor != leaderNode.id {
		t.Fatalf("leader durable state term=%d vote=%d, current term=%d id=%d",
			leaderNode.durableTerm, leaderNode.durableVotedFor, leaderNode.currentTerm, leaderNode.id)
	}

	if err := s.RunUntil(260); err != nil {
		t.Fatalf("RunUntil(260): %v", err)
	}
	again := singleLeader(t, s, ids)
	if again.ID != leader.ID || again.Term != leader.Term {
		t.Fatalf("leadership changed without a fault: %s -> %s", viewSummary([]View{leader}), viewSummary([]View{again}))
	}
	views := liveViews(t, s, ids)
	if len(views) != 5 {
		t.Fatalf("got %d live views, want 5", len(views))
	}
	for _, v := range views {
		if v.Term != again.Term {
			t.Fatalf("node %d term %d, leader term %d", v.ID, v.Term, again.Term)
		}
		if v.ID == again.ID {
			continue
		}
		if v.Role != Follower {
			t.Fatalf("node %d role %s, want follower", v.ID, v.Role)
		}
		if v.LeaderID != again.ID {
			t.Fatalf("node %d leader hint %d, want %d", v.ID, v.LeaderID, again.ID)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	s, ids := newRaftSim(t, 7, 5)
	if err := s.RunUntil(200); err != nil {
		t.Fatalf("RunUntil(200): %v", err)
	}
	old := singleLeader(t, s, ids)
	if err := s.Crash(old.ID); err != nil {
		t.Fatalf("Crash(%d): %v", old.ID, err)
	}
	if err := s.RunUntil(350); err != nil {
		t.Fatalf("RunUntil(350): %v", err)
	}
	newLeader := singleLeader(t, s, ids)
	if newLeader.ID == old.ID {
		t.Fatalf("crashed leader %d was elected again", old.ID)
	}
	if newLeader.Term <= old.Term {
		t.Fatalf("new leader term %d, old term %d", newLeader.Term, old.Term)
	}

	if err := s.Restart(old.ID); err != nil {
		t.Fatalf("Restart(%d): %v", old.ID, err)
	}
	if err := s.RunUntil(500); err != nil {
		t.Fatalf("RunUntil(500): %v", err)
	}
	final := singleLeader(t, s, ids)
	for _, v := range liveViews(t, s, ids) {
		if v.Term != final.Term {
			t.Fatalf("node %d term %d, leader term %d", v.ID, v.Term, final.Term)
		}
	}
}

func TestIsolatedLeaderCannotBlockMajority(t *testing.T) {
	s, ids := newRaftSim(t, 11, 5)
	if err := s.RunUntil(200); err != nil {
		t.Fatalf("RunUntil(200): %v", err)
	}
	old := singleLeader(t, s, ids)
	if err := s.Isolate(old.ID); err != nil {
		t.Fatalf("Isolate(%d): %v", old.ID, err)
	}
	if err := s.RunUntil(350); err != nil {
		t.Fatalf("RunUntil(350): %v", err)
	}

	var majorityIDs []sim.NodeID
	for _, id := range ids {
		if id != old.ID {
			majorityIDs = append(majorityIDs, id)
		}
	}
	majorityLeaders := leadersIn(t, s, majorityIDs)
	if len(majorityLeaders) != 1 {
		t.Fatalf("majority side has %d leaders: %s", len(majorityLeaders), viewSummary(majorityLeaders))
	}
	newLeader := majorityLeaders[0]
	if newLeader.Term <= old.Term {
		t.Fatalf("new leader term %d, isolated leader term %d", newLeader.Term, old.Term)
	}
	oldView := mustFindView(t, s, old.ID)
	if oldView.Role != Leader || oldView.Term != old.Term {
		t.Fatalf("isolated old leader changed unexpectedly: %s", viewSummary([]View{oldView}))
	}

	s.HealAll()
	if err := s.RunUntil(500); err != nil {
		t.Fatalf("RunUntil(500): %v", err)
	}
	final := singleLeader(t, s, ids)
	for _, v := range liveViews(t, s, ids) {
		if v.Term != final.Term {
			t.Fatalf("node %d term %d, leader term %d", v.ID, v.Term, final.Term)
		}
	}
	oldView = mustFindView(t, s, old.ID)
	if oldView.Role != Follower {
		t.Fatalf("old isolated leader role %s after heal, want follower", oldView.Role)
	}
}

func mustFindView(t *testing.T, s *sim.Sim, id sim.NodeID) View {
	t.Helper()
	node, up := s.Node(id)
	if !up {
		t.Fatalf("node %d is down", id)
	}
	viewer, ok := node.(interface{ View() View })
	if !ok {
		t.Fatalf("node %d does not implement View", id)
	}
	return viewer.View()
}

func TestVotePersistsBeforeReply(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()

	n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
	if len(env.sent) != 0 {
		t.Fatalf("vote reply sent before persistence: %d messages", len(env.sent))
	}
	if len(env.replaces) != 1 {
		t.Fatalf("meta replaces = %d, want 1", len(env.replaces))
	}
	if len(env.syncs) != 0 {
		t.Fatalf("syncs = %d before replace completion, want 0", len(env.syncs))
	}

	completeReplace(t, env, n)
	if len(env.syncs) != 1 {
		t.Fatalf("syncs = %d after replace completion, want 1", len(env.syncs))
	}
	if len(env.sent) != 0 {
		t.Fatalf("vote reply sent before sync completion: %d messages", len(env.sent))
	}

	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
	if len(env.sent) != 1 {
		t.Fatalf("messages after sync = %d, want 1", len(env.sent))
	}
	resp := decodeVoteResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.VoteGranted {
		t.Fatalf("vote response = %+v, want term 1 granted", resp)
	}
	if n.currentTerm != 1 || n.votedFor != 1 || n.durableTerm != 1 || n.durableVotedFor != 1 {
		t.Fatalf("node state term=%d vote=%d durable=%d/%d, want 1/1/1/1",
			n.currentTerm, n.votedFor, n.durableTerm, n.durableVotedFor)
	}
}

func TestCrashBeforeMetaSync(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()

	n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
	completeReplace(t, env, n)
	oldSync := env.lastSync()

	env.crash()
	restarted := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	if restarted.currentTerm != 0 || restarted.votedFor != 0 ||
		restarted.durableTerm != 0 || restarted.durableVotedFor != 0 {
		t.Fatalf("recovered term=%d vote=%d durable=%d/%d, want 0/0/0/0",
			restarted.currentTerm, restarted.votedFor, restarted.durableTerm, restarted.durableVotedFor)
	}
	if len(env.sent) != 0 {
		t.Fatalf("a vote reply leaked across the crash: %d messages", len(env.sent))
	}
	mustPanic(t, "stale sync completion", func() {
		restarted.HandleIO(sim.IOEvent{Tag: oldSync.tag, Op: sim.IOSync})
	})
}

func TestCrashAfterMetaSync(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()

	n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
	completePersist(t, env, n)
	if len(env.sent) != 1 {
		t.Fatalf("messages after sync = %d, want 1", len(env.sent))
	}

	env.crash()
	restarted := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	if restarted.currentTerm != 1 || restarted.votedFor != 1 ||
		restarted.durableTerm != 1 || restarted.durableVotedFor != 1 {
		t.Fatalf("recovered term=%d vote=%d durable=%d/%d, want 1/1/1/1",
			restarted.currentTerm, restarted.votedFor, restarted.durableTerm, restarted.durableVotedFor)
	}

	env.clearTraffic()
	restarted.HandleMessage(voteRequestMessage(1, 1, 0, 0))
	if len(env.replaces) != 0 || len(env.syncs) != 0 {
		t.Fatalf("duplicate vote request rewrote durable state: replaces=%d syncs=%d",
			len(env.replaces), len(env.syncs))
	}
	if len(env.sent) != 1 {
		t.Fatalf("duplicate vote request replies = %d, want 1", len(env.sent))
	}
	resp := decodeVoteResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.VoteGranted {
		t.Fatalf("duplicate vote response = %+v, want term 1 granted", resp)
	}
}

func TestVoteGrantRules(t *testing.T) {
	t.Run("lower-term", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.HandleMessage(heartbeatMessage(3, 2))
		completePersist(t, env, n)
		env.clearTraffic()

		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		if len(env.replaces) != 0 || len(env.syncs) != 0 {
			t.Fatalf("lower-term vote request persisted state")
		}
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 2 || resp.VoteGranted {
			t.Fatalf("response = %+v, want term 2 denied", resp)
		}
	})

	t.Run("second-candidate-same-term", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		completePersist(t, env, n)
		env.clearTraffic()
		timerBefore := n.electionTimer

		n.HandleMessage(voteRequestMessage(3, 1, 0, 0))
		if len(env.replaces) != 0 || len(env.syncs) != 0 {
			t.Fatalf("denied same-term vote persisted state")
		}
		if n.electionTimer != timerBefore {
			t.Fatalf("denied vote reset the election timer: %d -> %d", timerBefore, n.electionTimer)
		}
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 1 || resp.VoteGranted {
			t.Fatalf("response = %+v, want term 1 denied", resp)
		}
	})

	t.Run("up-to-date-candidate", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		env.clearTraffic()

		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		if len(env.sent) != 0 {
			t.Fatalf("vote reply sent before persistence")
		}
		completePersist(t, env, n)
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 1 || !resp.VoteGranted {
			t.Fatalf("response = %+v, want term 1 granted", resp)
		}
	})

	t.Run("stale-log", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.log = append(n.log, Entry{Index: 1, Term: 5, Kind: EntryNoop})
		n.durableLastIndex = 1
		env.clearTraffic()

		n.HandleMessage(voteRequestMessage(1, 2, 1, 4))
		if len(env.sent) != 0 {
			t.Fatalf("denial sent before the higher term was durable")
		}
		completePersist(t, env, n)
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 2 || resp.VoteGranted {
			t.Fatalf("response = %+v, want term 2 denied", resp)
		}
		if n.votedFor != 0 || n.durableVotedFor != 0 {
			t.Fatalf("denied higher-term request left vote %d/%d, want 0/0", n.votedFor, n.durableVotedFor)
		}
	})

	t.Run("duplicate-request", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		completePersist(t, env, n)
		env.clearTraffic()

		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		if len(env.replaces) != 0 || len(env.syncs) != 0 {
			t.Fatalf("duplicate vote request rewrote durable state")
		}
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 1 || !resp.VoteGranted {
			t.Fatalf("response = %+v, want term 1 granted", resp)
		}
	})
}

func TestElectionTimeoutDrawsFromEnv(t *testing.T) {
	env := newTestEnv(1)
	factory := NewFactory(Config{ID: 1, Peers: []sim.NodeID{2, 3}})
	n, ok := factory(env).(*Node)
	if !ok {
		t.Fatalf("factory did not return *Node")
	}
	if len(env.intRangeCalls) != 0 {
		t.Fatalf("constructor drew randomness: %v", env.intRangeCalls)
	}
	n.Start()
	if len(env.intRangeCalls) != 0 {
		t.Fatalf("Start drew randomness before recovery: %v", env.intRangeCalls)
	}
	read := env.lastRead()
	n.HandleIO(sim.IOEvent{Tag: read.tag, Op: sim.IORead, Data: env.readData()})
	if len(env.intRangeCalls) != 1 {
		t.Fatalf("recovery drew %d ranges, want 1", len(env.intRangeCalls))
	}
	if got := env.intRangeCalls[0]; got != [2]int{12, 24} {
		t.Fatalf("election timeout range = %v, want [12 24]", got)
	}
	if got := env.timerDelay(n.electionTimer); got != 12 {
		t.Fatalf("first election delay = %d, want 12", got)
	}

	fireElectionTimer(t, env, n)
	if len(env.intRangeCalls) != 1 {
		t.Fatalf("election start drew randomness before sync: %v", env.intRangeCalls)
	}
	completePersist(t, env, n)
	if len(env.intRangeCalls) != 2 {
		t.Fatalf("post-election reset drew %d ranges, want 2", len(env.intRangeCalls))
	}
	if got := env.intRangeCalls[1]; got != [2]int{12, 24} {
		t.Fatalf("second election timeout range = %v, want [12 24]", got)
	}
	if got := env.timerDelay(n.electionTimer); got != 13 {
		t.Fatalf("second election delay = %d, want 13", got)
	}
}

func TestSingleNodeElection(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, nil)
	env.clearTraffic()

	fireElectionTimer(t, env, n)
	if len(env.replaces) != 1 {
		t.Fatalf("meta replaces = %d, want 1", len(env.replaces))
	}
	completePersist(t, env, n)

	if n.role != Leader {
		t.Fatalf("role = %s, want leader", n.role)
	}
	if n.currentTerm != 1 || n.durableTerm != 1 || n.votedFor != 1 || n.durableVotedFor != 1 {
		t.Fatalf("term/vote = %d/%d durable %d/%d, want 1/1/1/1",
			n.currentTerm, n.votedFor, n.durableTerm, n.durableVotedFor)
	}
	if len(env.sent) != 0 {
		t.Fatalf("single-node leader sent %d vote/heartbeat messages", len(env.sent))
	}
	if env.timerCount(timerTagHeartbeat) != 1 || env.timerCount(timerTagElection) != 0 {
		t.Fatalf("timers heartbeat=%d election=%d, want 1/0",
			env.timerCount(timerTagHeartbeat), env.timerCount(timerTagElection))
	}

	fireHeartbeatTimer(t, env, n)
	if len(env.sent) != 0 {
		t.Fatalf("single-node heartbeat sent %d messages", len(env.sent))
	}
	if env.timerCount(timerTagHeartbeat) != 1 {
		t.Fatalf("heartbeat timer was not rescheduled")
	}
}

func TestCandidateStepsDownOnHigherTerm(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	fireElectionTimer(t, env, n)
	completePersist(t, env, n)
	if n.role != Candidate {
		t.Fatalf("role = %s, want candidate", n.role)
	}
	env.clearTraffic()
	timerBefore := n.electionTimer

	n.HandleMessage(voteResponseMessage(1, 2, false))
	if len(env.replaces) != 1 {
		t.Fatalf("meta replaces = %d, want 1", len(env.replaces))
	}
	if len(env.sent) != 0 {
		t.Fatalf("candidate replied before the higher term was durable")
	}
	completePersist(t, env, n)

	if n.role != Follower || n.currentTerm != 2 || n.votedFor != 0 {
		t.Fatalf("state role=%s term=%d vote=%d, want follower/2/0", n.role, n.currentTerm, n.votedFor)
	}
	if n.durableTerm != 2 || n.durableVotedFor != 0 {
		t.Fatalf("durable state = %d/%d, want 2/0", n.durableTerm, n.durableVotedFor)
	}
	if n.electionTimer != timerBefore {
		t.Fatalf("non-leader stepdown reset the election timer: %d -> %d", timerBefore, n.electionTimer)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	fireElectionTimer(t, env, n)
	completePersist(t, env, n)
	n.HandleMessage(voteResponseMessage(2, 1, true))
	if n.role != Leader {
		t.Fatalf("role = %s, want leader", n.role)
	}
	if env.timerCount(timerTagHeartbeat) != 1 || env.timerCount(timerTagElection) != 0 {
		t.Fatalf("leader timers heartbeat=%d election=%d, want 1/0",
			env.timerCount(timerTagHeartbeat), env.timerCount(timerTagElection))
	}
	env.clearTraffic()

	n.HandleMessage(appendResponseMessage(2, 2, true))
	if len(env.replaces) != 1 {
		t.Fatalf("meta replaces = %d, want 1", len(env.replaces))
	}
	if len(env.sent) != 0 {
		t.Fatalf("leader replied before the higher term was durable")
	}
	completePersist(t, env, n)

	if n.role != Follower || n.currentTerm != 2 || n.votedFor != 0 {
		t.Fatalf("state role=%s term=%d vote=%d, want follower/2/0", n.role, n.currentTerm, n.votedFor)
	}
	if n.durableTerm != 2 || n.durableVotedFor != 0 {
		t.Fatalf("durable state = %d/%d, want 2/0", n.durableTerm, n.durableVotedFor)
	}
	if n.heartbeatTimer != 0 || env.timerCount(timerTagHeartbeat) != 0 {
		t.Fatalf("heartbeat timer survived stepdown")
	}
	if env.timerCount(timerTagElection) != 1 {
		t.Fatalf("election timers = %d, want 1", env.timerCount(timerTagElection))
	}
}

func TestHeartbeatHigherTermPersistsBeforeReply(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()

	n.HandleMessage(heartbeatMessage(1, 1))
	if len(env.sent) != 0 {
		t.Fatalf("heartbeat reply sent before persistence")
	}
	if len(env.replaces) != 1 {
		t.Fatalf("meta replaces = %d, want 1", len(env.replaces))
	}
	completeReplace(t, env, n)
	if len(env.syncs) != 1 || len(env.sent) != 0 {
		t.Fatalf("after replace syncs=%d sent=%d, want 1/0", len(env.syncs), len(env.sent))
	}
	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
	if len(env.sent) != 1 {
		t.Fatalf("messages after sync = %d, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.Success {
		t.Fatalf("append response = %+v, want term 1 success", resp)
	}
	if n.currentTerm != 1 || n.votedFor != 0 || n.durableTerm != 1 || n.durableVotedFor != 0 {
		t.Fatalf("state term=%d vote=%d durable=%d/%d, want 1/0/1/0",
			n.currentTerm, n.votedFor, n.durableTerm, n.durableVotedFor)
	}
	if env.timerCount(timerTagElection) != 1 {
		t.Fatalf("election timers = %d, want 1", env.timerCount(timerTagElection))
	}
}

func TestHeartbeatSameTermResetsTimer(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	n.HandleMessage(heartbeatMessage(1, 1))
	completePersist(t, env, n)
	env.clearTraffic()
	timerBefore := n.electionTimer

	n.HandleMessage(heartbeatMessage(1, 1))
	if len(env.replaces) != 0 || len(env.syncs) != 0 {
		t.Fatalf("same-term heartbeat persisted state")
	}
	if len(env.sent) != 1 {
		t.Fatalf("responses = %d, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.Success {
		t.Fatalf("response = %+v, want term 1 success", resp)
	}
	if n.electionTimer == timerBefore {
		t.Fatalf("same-term heartbeat did not reset the election timer")
	}
	if n.leaderID != 1 {
		t.Fatalf("leader hint = %d, want 1", n.leaderID)
	}
}

func TestLowerTermHeartbeatRejected(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	n.HandleMessage(heartbeatMessage(3, 2))
	completePersist(t, env, n)
	env.clearTraffic()
	timerBefore := n.electionTimer

	n.HandleMessage(heartbeatMessage(1, 1))
	if len(env.replaces) != 0 || len(env.syncs) != 0 {
		t.Fatalf("lower-term heartbeat persisted state")
	}
	if len(env.sent) != 1 {
		t.Fatalf("responses = %d, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 2 || resp.Success {
		t.Fatalf("response = %+v, want term 2 failure", resp)
	}
	if n.electionTimer != timerBefore {
		t.Fatalf("rejected heartbeat reset the election timer: %d -> %d", timerBefore, n.electionTimer)
	}
}

func TestRejectedVoteDoesNotResetTimer(t *testing.T) {
	t.Run("same-term", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.HandleMessage(voteRequestMessage(1, 1, 0, 0))
		completePersist(t, env, n)
		env.clearTraffic()
		timerBefore := n.electionTimer

		n.HandleMessage(voteRequestMessage(3, 1, 0, 0))
		if len(env.replaces) != 0 || len(env.syncs) != 0 {
			t.Fatalf("denied vote persisted state")
		}
		if n.electionTimer != timerBefore {
			t.Fatalf("denied vote reset the election timer")
		}
	})

	t.Run("higher-term-stale-log", func(t *testing.T) {
		env := newTestEnv(2)
		n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
		n.log = append(n.log, Entry{Index: 1, Term: 5, Kind: EntryNoop})
		n.durableLastIndex = 1
		env.clearTraffic()
		timerBefore := n.electionTimer

		n.HandleMessage(voteRequestMessage(1, 2, 1, 4))
		if len(env.sent) != 0 {
			t.Fatalf("denial sent before the higher term was durable")
		}
		completePersist(t, env, n)
		if len(env.sent) != 1 {
			t.Fatalf("responses = %d, want 1", len(env.sent))
		}
		resp := decodeVoteResponse(env.sent[0].payload)
		if resp.Term != 2 || resp.VoteGranted {
			t.Fatalf("response = %+v, want term 2 denied", resp)
		}
		if n.electionTimer != timerBefore {
			t.Fatalf("denied higher-term vote reset the election timer")
		}
	})
}

func TestStaleTimerIgnored(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	stale := sim.TimerEvent{ID: n.electionTimer + 1000, Tag: timerTagElection}
	n.HandleTimer(stale)
	if len(env.replaces) != 0 {
		t.Fatalf("stale timer started an election")
	}
	if n.role != Follower {
		t.Fatalf("role = %s, want follower", n.role)
	}

	fireElectionTimer(t, env, n)
	if len(env.replaces) != 1 {
		t.Fatalf("real election timer did not start an election")
	}
}

func TestStaleIOCompletionRejected(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	mustPanic(t, "stale IO completion", func() {
		n.HandleIO(sim.IOEvent{Tag: 999, Op: sim.IOSync})
	})
}

func TestSnapshotCanonical(t *testing.T) {
	envA := newTestEnv(1)
	a := bootTestNode(t, envA, 1, []sim.NodeID{2, 3})
	envB := newTestEnv(1)
	b := bootTestNode(t, envB, 1, []sim.NodeID{2, 3})

	if got, want := string(a.Snapshot()), string(b.Snapshot()); got != want {
		t.Fatalf("identical nodes have different snapshots:\n%s\n%s", got, want)
	}

	a.HandleMessage(heartbeatMessage(2, 1))
	if string(a.Snapshot()) == string(b.Snapshot()) {
		t.Fatalf("different states produced identical snapshots")
	}
}

func TestElectionSafetyInvariant(t *testing.T) {
	t.Run("two-leaders-same-term", func(t *testing.T) {
		views := map[sim.NodeID]View{
			1: {ID: 1, Incarnation: 1, Role: Leader, Term: 1, DurableTerm: 1, VotedFor: 1, DurableVotedFor: 1},
			2: {ID: 2, Incarnation: 1, Role: Leader, Term: 1, DurableTerm: 1, VotedFor: 2, DurableVotedFor: 2},
		}
		inv := NewElectionSafetyWithSource([]sim.NodeID{1, 2}, func(_ *sim.Sim, id sim.NodeID) (View, bool) {
			v, ok := views[id]
			return v, ok
		})
		if err := inv.Check(nil); err == nil {
			t.Fatalf("two leaders in term 1 were accepted")
		}
	})

	t.Run("two-leaders-same-term-later-step", func(t *testing.T) {
		views := map[sim.NodeID]View{
			1: {ID: 1, Incarnation: 1, Role: Leader, Term: 1, DurableTerm: 1, VotedFor: 1, DurableVotedFor: 1},
			2: {ID: 2, Incarnation: 1, Role: Follower, Term: 1, DurableTerm: 1},
		}
		inv := NewElectionSafetyWithSource([]sim.NodeID{1, 2}, func(_ *sim.Sim, id sim.NodeID) (View, bool) {
			v, ok := views[id]
			return v, ok
		})
		if err := inv.Check(nil); err != nil {
			t.Fatalf("first observation: %v", err)
		}
		views[1] = View{ID: 1, Incarnation: 1, Role: Follower, Term: 1, DurableTerm: 1, VotedFor: 1, DurableVotedFor: 1}
		views[2] = View{ID: 2, Incarnation: 1, Role: Leader, Term: 1, DurableTerm: 1, VotedFor: 2, DurableVotedFor: 2}
		if err := inv.Check(nil); err == nil {
			t.Fatalf("a second leader in term 1 was accepted on a later step")
		}
	})

	t.Run("different-terms", func(t *testing.T) {
		views := map[sim.NodeID]View{
			1: {ID: 1, Incarnation: 1, Role: Leader, Term: 1, DurableTerm: 1, VotedFor: 1, DurableVotedFor: 1},
			2: {ID: 2, Incarnation: 1, Role: Leader, Term: 2, DurableTerm: 2, VotedFor: 2, DurableVotedFor: 2},
		}
		inv := NewElectionSafetyWithSource([]sim.NodeID{1, 2}, func(_ *sim.Sim, id sim.NodeID) (View, bool) {
			v, ok := views[id]
			return v, ok
		})
		if err := inv.Check(nil); err != nil {
			t.Fatalf("leaders in different terms rejected: %v", err)
		}
	})

	t.Run("durable-vote-change", func(t *testing.T) {
		view := View{ID: 1, Incarnation: 1, Role: Follower, Term: 3, DurableTerm: 3, VotedFor: 1, DurableVotedFor: 1}
		inv := NewElectionSafetyWithSource([]sim.NodeID{1}, func(_ *sim.Sim, id sim.NodeID) (View, bool) {
			return view, id == 1
		})
		if err := inv.Check(nil); err != nil {
			t.Fatalf("first observation: %v", err)
		}
		view.DurableVotedFor = 2
		if err := inv.Check(nil); err == nil {
			t.Fatalf("durable vote change at the same term was accepted")
		}
	})

	t.Run("term-regression", func(t *testing.T) {
		view := View{ID: 1, Incarnation: 1, Role: Follower, Term: 3, DurableTerm: 3}
		inv := NewElectionSafetyWithSource([]sim.NodeID{1}, func(_ *sim.Sim, id sim.NodeID) (View, bool) {
			return view, id == 1
		})
		if err := inv.Check(nil); err != nil {
			t.Fatalf("first observation: %v", err)
		}
		view.Term = 2
		view.DurableTerm = 2
		if err := inv.Check(nil); err == nil {
			t.Fatalf("term regression was accepted")
		}
	})
}

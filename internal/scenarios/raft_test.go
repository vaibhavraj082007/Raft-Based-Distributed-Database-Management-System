package scenarios

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"raftkv/internal/raft"
	"raftkv/internal/sim"
)

var raftSeeds = flag.Int("raft-seeds", 50, "number of seeds for TestRaftMultiSeed")

func newRaftWithTrace(t *testing.T, seed uint64, end sim.Tick) (*sim.Sim, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s, err := NewRaft(sim.Config{
		Seed:     seed,
		Scenario: RaftScenario,
		Trace:    &buf,
	}, RaftNodeCount, end)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	return s, &buf
}

func runRaft(t *testing.T, seed uint64, end sim.Tick) (string, uint64, uint64) {
	t.Helper()
	s, buf := newRaftWithTrace(t, seed, end)
	if err := s.RunUntil(end); err != nil {
		t.Fatalf("seed %d: %v", seed, err)
	}
	return buf.String(), s.TraceHash(), s.StateHash()
}

type raftTestCluster struct {
	s        *sim.Sim
	ids      []sim.NodeID
	recorder *raft.ApplyRecorder
	clientID sim.NodeID
}

func newFaultFreeRaftCluster(t *testing.T, seed uint64, withClient bool) *raftTestCluster {
	t.Helper()
	s, err := sim.New(sim.Config{Seed: seed, Scenario: "raft-fault-free"})
	if err != nil {
		t.Fatalf("sim.New: %v", err)
	}
	ids := make([]sim.NodeID, RaftNodeCount)
	for i := range ids {
		ids[i] = sim.NodeID(i + 1)
	}
	recorder := raft.NewApplyRecorder(ids)
	for _, id := range ids {
		if err := s.AddNode(id, newRaftNodeFactory(id, raftPeers(ids, id), recorder)); err != nil {
			t.Fatalf("AddNode %d: %v", id, err)
		}
	}
	var clientID sim.NodeID
	if withClient {
		clientID = sim.NodeID(len(ids) + 1)
		if err := s.AddNode(clientID, newClientFactory(clientID, ids)); err != nil {
			t.Fatalf("AddNode client: %v", err)
		}
	}
	s.RegisterInvariant(raft.NewElectionSafety(ids))
	s.RegisterInvariant(raft.NewLogMatching(ids))
	s.RegisterInvariant(raft.NewStateMachineSafety(recorder))
	s.RegisterInvariant(raft.NewCommitDurability(ids, recorder))
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return &raftTestCluster{s: s, ids: ids, recorder: recorder, clientID: clientID}
}

func raftView(t *testing.T, s *sim.Sim, id sim.NodeID) (raft.View, bool) {
	t.Helper()
	node, up := s.Node(id)
	if !up {
		return raft.View{}, false
	}
	viewer, ok := node.(interface{ View() raft.View })
	if !ok {
		t.Fatalf("node %d does not implement View()", id)
	}
	v := viewer.View()
	if v.Recovering {
		return raft.View{}, false
	}
	return v, true
}

func raftViews(t *testing.T, s *sim.Sim, ids []sim.NodeID) []raft.View {
	t.Helper()
	views := make([]raft.View, 0, len(ids))
	for _, id := range ids {
		v, ok := raftView(t, s, id)
		if !ok {
			t.Fatalf("raft node %d is down or recovering", id)
		}
		views = append(views, v)
	}
	return views
}

func findLeaderView(t *testing.T, s *sim.Sim, ids []sim.NodeID) (raft.View, int) {
	t.Helper()
	var leader raft.View
	count := 0
	for _, id := range ids {
		v, ok := raftView(t, s, id)
		if !ok {
			continue
		}
		if v.Role == raft.Leader {
			leader = v
			count++
		}
	}
	return leader, count
}

func entriesEqual(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Kind == b.Kind && bytes.Equal(a.Data, b.Data)
}

func raftReplay(seed uint64) string {
	return fmt.Sprintf("go run ./cmd/simctl -seed %d -scenario raft -ticks %d -trace", seed, DefaultRaftEnd)
}

// assertRaftFinalState checks the Stage 2C final-state properties. When
// requireCommit is true it also checks the common committed prefix, apply
// position, and at least one committed client command.
func assertRaftFinalState(t *testing.T, s *sim.Sim, ids []sim.NodeID, requireCommit bool) {
	t.Helper()
	for _, id := range ids {
		if !s.IsUp(id) {
			t.Fatalf("raft node %d is down at end", id)
		}
	}
	leader, count := findLeaderView(t, s, ids)
	if count != 1 {
		t.Fatalf("found %d leaders at end, want 1", count)
	}
	views := raftViews(t, s, ids)
	for _, v := range views {
		if v.Term != leader.Term {
			t.Fatalf("node %d term %d differs from leader %d term %d", v.ID, v.Term, leader.ID, leader.Term)
		}
	}
	if !requireCommit {
		return
	}
	if leader.CommitIndex > leader.LastLogIndex {
		t.Fatalf("leader %d commit %d exceeds last log index %d", leader.ID, leader.CommitIndex, leader.LastLogIndex)
	}
	for _, v := range views {
		if v.CommitIndex != leader.CommitIndex {
			t.Fatalf("node %d commit %d differs from leader %d commit %d",
				v.ID, v.CommitIndex, leader.ID, leader.CommitIndex)
		}
		if v.LastApplied != v.CommitIndex {
			t.Fatalf("node %d applied %d but commit is %d", v.ID, v.LastApplied, v.CommitIndex)
		}
		if v.CommitIndex > v.LastLogIndex {
			t.Fatalf("node %d commit %d exceeds last log index %d", v.ID, v.CommitIndex, v.LastLogIndex)
		}
		for i := uint64(1); i <= v.CommitIndex; i++ {
			if !entriesEqual(v.Log[i], leader.Log[i]) {
				t.Fatalf("node %d log at committed index %d differs from leader %d", v.ID, i, leader.ID)
			}
		}
	}
	commandIndex := uint64(0)
	for i := uint64(1); i <= leader.CommitIndex; i++ {
		if leader.Log[i].Kind == raft.EntryCommand {
			commandIndex = i
			break
		}
	}
	if commandIndex == 0 {
		t.Fatalf("committed log has no EntryCommand")
	}
	for _, v := range views {
		if v.LastApplied < commandIndex {
			t.Fatalf("node %d applied only through %d, committed command is at %d",
				v.ID, v.LastApplied, commandIndex)
		}
		if !entriesEqual(v.Log[commandIndex], leader.Log[commandIndex]) {
			t.Fatalf("node %d log at committed command index %d differs from leader %d",
				v.ID, commandIndex, leader.ID)
		}
	}
}

func TestRaftElectionSmoke(t *testing.T) {
	cluster := newFaultFreeRaftCluster(t, 7, false)
	if err := cluster.s.RunUntil(100); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	leader, count := findLeaderView(t, cluster.s, cluster.ids)
	if count != 1 {
		t.Fatalf("found %d leaders, want 1", count)
	}
	for _, id := range cluster.ids {
		v, ok := raftView(t, cluster.s, id)
		if !ok {
			t.Fatalf("raft node %d is down or recovering", id)
		}
		if v.Term != leader.Term {
			t.Fatalf("node %d term %d differs from leader %d term %d", id, v.Term, leader.ID, leader.Term)
		}
	}
}

func TestRaftReplicationSmoke(t *testing.T) {
	cluster := newFaultFreeRaftCluster(t, 11, true)
	if err := cluster.s.RunUntil(300); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	assertRaftFinalState(t, cluster.s, cluster.ids, true)
	if err := cluster.recorder.Err(); err != nil {
		t.Fatalf("apply recorder: %v", err)
	}
}

func TestRaftSameSeedProducesIdenticalTrace(t *testing.T) {
	const seed = 42
	trace0, hash0, state0 := runRaft(t, seed, DefaultRaftEnd)
	for i := 1; i < 3; i++ {
		trace, hash, state := runRaft(t, seed, DefaultRaftEnd)
		if trace != trace0 {
			t.Fatalf("run %d trace differs\n%s", i, firstDiff(trace0, trace))
		}
		if hash != hash0 {
			t.Fatalf("run %d trace hash = %016x, want %016x", i, hash, hash0)
		}
		if state != state0 {
			t.Fatalf("run %d state hash = %016x, want %016x", i, state, state0)
		}
	}
}

func TestRaftInterleavedSimsDoNotShareState(t *testing.T) {
	solo, soloBuf := newRaftWithTrace(t, 42, DefaultRaftEnd)
	if err := solo.RunUntil(DefaultRaftEnd); err != nil {
		t.Fatalf("solo run: %v", err)
	}

	a, aBuf := newRaftWithTrace(t, 42, DefaultRaftEnd)
	b, bBuf := newRaftWithTrace(t, 42, DefaultRaftEnd)
	for tick := sim.Tick(50); tick <= DefaultRaftEnd; tick += 50 {
		if err := a.RunUntil(tick); err != nil {
			t.Fatalf("a RunUntil(%d): %v", tick, err)
		}
		if err := b.RunUntil(tick); err != nil {
			t.Fatalf("b RunUntil(%d): %v", tick, err)
		}
		if a.StateHash() != b.StateHash() {
			t.Fatalf("interleaved state hashes differ at t=%d: %016x vs %016x",
				tick, a.StateHash(), b.StateHash())
		}
	}

	if a.TraceHash() != solo.TraceHash() || b.TraceHash() != solo.TraceHash() {
		t.Fatalf("interleaved trace hash differs: solo=%016x a=%016x b=%016x",
			solo.TraceHash(), a.TraceHash(), b.TraceHash())
	}
	if a.StateHash() != solo.StateHash() || b.StateHash() != solo.StateHash() {
		t.Fatalf("interleaved state hash differs: solo=%016x a=%016x b=%016x",
			solo.StateHash(), a.StateHash(), b.StateHash())
	}
	if aBuf.String() != soloBuf.String() {
		t.Fatalf("interleaved a trace differs\n%s", firstDiff(soloBuf.String(), aBuf.String()))
	}
	if bBuf.String() != soloBuf.String() {
		t.Fatalf("interleaved b trace differs\n%s", firstDiff(soloBuf.String(), bBuf.String()))
	}
}

func TestRaftGoldenTraceSeed42(t *testing.T) {
	s, buf := newRaftWithTrace(t, 42, DefaultRaftEnd)
	if err := s.RunUntil(DefaultRaftEnd); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	path := filepath.Join("..", "..", "testdata", "stage2", "raft-seed-42.trace")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir golden dir: %v", err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatalf("write golden trace: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden trace: %v (regenerate with go test ./internal/scenarios -run TestRaftGoldenTraceSeed42 -update)", err)
	}
	if got := buf.String(); got != string(want) {
		t.Fatalf("golden trace mismatch\n%s", firstDiff(string(want), got))
	}
}

func TestRaftSameSeedAcrossProcesses(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not available")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("module root: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "simctl")
	build := exec.Command(goTool, "build", "-o", bin, "./cmd/simctl")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build simctl: %v\n%s", err, out)
	}

	args := []string{"-seed", "42", "-scenario", "raft", "-ticks", "400", "-trace"}
	first, err := runSimctl(bin, args...)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := runSimctl(bin, args...)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("same seed produced different CLI output\nfirst:\n%s\nsecond:\n%s", first, second)
	}
	for _, want := range []string{"status=ok", "scenario=raft", "nodes=6", "trace-hash=", "state-hash="} {
		if !bytes.Contains(first, []byte(want)) {
			t.Fatalf("CLI output missing %q:\n%s", want, first)
		}
	}
}

func runSimctl(bin string, args ...string) ([]byte, error) {
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%v\nstderr:\n%s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

func TestRaftDifferentSeedsDiffer(t *testing.T) {
	trace1, hash1, state1 := runRaft(t, 1, DefaultRaftEnd)
	trace2, hash2, state2 := runRaft(t, 2, DefaultRaftEnd)
	if trace1 == trace2 {
		t.Fatal("seeds 1 and 2 produced identical traces")
	}
	if hash1 == hash2 {
		t.Fatalf("seeds 1 and 2 produced the same trace hash %016x", hash1)
	}
	if state1 == state2 {
		t.Fatalf("seeds 1 and 2 produced the same state hash %016x", state1)
	}
}

func TestRaftMultiSeed(t *testing.T) {
	n := *raftSeeds
	if n < 1 {
		t.Fatalf("-raft-seeds must be >= 1, got %d", n)
	}
	for seed := 1; seed <= n; seed++ {
		s, err := NewRaft(sim.Config{
			Seed:     uint64(seed),
			Scenario: RaftScenario,
		}, RaftNodeCount, DefaultRaftEnd)
		if err != nil {
			t.Fatalf("seed %d setup: %v\nreplay: %s", seed, err, raftReplay(uint64(seed)))
		}
		if err := s.RunUntil(DefaultRaftEnd); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		ids := raftNodeIDs(s)
		for _, id := range ids {
			if !s.IsUp(id) {
				t.Fatalf("seed %d: raft node %d is down at end\nreplay: %s", seed, id, raftReplay(uint64(seed)))
			}
		}
		leader, count := findLeaderView(t, s, ids)
		if count != 1 {
			t.Fatalf("seed %d: found %d leaders at end, want 1\nreplay: %s",
				seed, count, raftReplay(uint64(seed)))
		}
		for _, id := range ids {
			v, ok := raftView(t, s, id)
			if !ok {
				t.Fatalf("seed %d: raft node %d is down or recovering\nreplay: %s",
					seed, id, raftReplay(uint64(seed)))
			}
			if v.Term != leader.Term {
				t.Fatalf("seed %d: node %d term %d differs from leader %d term %d\nreplay: %s",
					seed, id, v.Term, leader.ID, leader.Term, raftReplay(uint64(seed)))
			}
		}
		if seed%1000 == 0 {
			t.Logf("raft multi-seed progress: %d/%d", seed, n)
		}
	}
}

func TestRaftFinalLeaderAndCommit(t *testing.T) {
	s, _ := newRaftWithTrace(t, 42, DefaultRaftEnd)
	if err := s.RunUntil(DefaultRaftEnd); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	ids := raftNodeIDs(s)
	assertRaftFinalState(t, s, ids, true)

	leader, _ := findLeaderView(t, s, ids)
	t.Logf("seed 42 final: leader=%d term=%d commit=%d last=%d", leader.ID, leader.Term, leader.CommitIndex, leader.LastLogIndex)

	clientID := sim.NodeID(len(ids) + 1)
	node, up := s.Node(clientID)
	if !up {
		t.Fatalf("client node %d is down", clientID)
	}
	client, ok := node.(*clientNode)
	if !ok {
		t.Fatalf("node %d is not a clientNode", clientID)
	}
	if client.sent == 0 {
		t.Fatal("client sent no requests")
	}
	if client.accepted == 0 {
		t.Fatal("client observed no accepted requests")
	}
	t.Logf("seed 42 client: sent=%d accepted=%d committed=%d", client.sent, client.accepted, client.committed)
}

func TestRaftReplayPrefixIndependentOfEnd(t *testing.T) {
	short, shortBuf := newRaftWithTrace(t, 42, 100)
	if err := short.RunUntil(100); err != nil {
		t.Fatalf("short run: %v", err)
	}
	long, longBuf := newRaftWithTrace(t, 42, DefaultRaftEnd)
	if err := long.RunUntil(DefaultRaftEnd); err != nil {
		t.Fatalf("long run: %v", err)
	}
	if !strings.HasPrefix(longBuf.String(), shortBuf.String()) {
		t.Fatalf("long trace does not extend the short trace\n%s",
			firstDiff(shortBuf.String(), longBuf.String()[:min(len(longBuf.String()), len(shortBuf.String()))]))
	}
}

func TestRaftCrashRestartConvergence(t *testing.T) {
	cluster := newFaultFreeRaftCluster(t, 23, true)
	s := cluster.s
	ids := cluster.ids
	follower := ids[len(ids)-1]

	if err := s.ScheduleFunc(30, "crash-follower", func(s *sim.Sim) error {
		if !s.IsUp(follower) {
			return nil
		}
		return s.Crash(follower)
	}); err != nil {
		t.Fatalf("schedule follower crash: %v", err)
	}
	if err := s.ScheduleFunc(50, "restart-follower", func(s *sim.Sim) error {
		if s.IsUp(follower) {
			return nil
		}
		return s.Restart(follower)
	}); err != nil {
		t.Fatalf("schedule follower restart: %v", err)
	}

	var scheduleLeaderCrash func(at sim.Tick) error
	scheduleLeaderCrash = func(at sim.Tick) error {
		return s.ScheduleFunc(at, "crash-leader", func(s *sim.Sim) error {
			leader, count := findLeaderView(t, s, ids)
			if count != 1 {
				if at+10 < 200 {
					return scheduleLeaderCrash(at + 10)
				}
				return nil
			}
			return s.Crash(leader.ID)
		})
	}
	if err := scheduleLeaderCrash(100); err != nil {
		t.Fatalf("schedule leader crash: %v", err)
	}
	if err := s.ScheduleFunc(220, "restart-all", func(s *sim.Sim) error {
		for _, id := range ids {
			if !s.IsUp(id) {
				if err := s.Restart(id); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("schedule restart-all: %v", err)
	}

	if err := s.RunUntil(360); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	assertRaftFinalState(t, s, ids, true)
	if err := cluster.recorder.Err(); err != nil {
		t.Fatalf("apply recorder: %v", err)
	}
}

func TestRaftInvariantCatchesElectionViolation(t *testing.T) {
	views := map[sim.NodeID]raft.View{
		1: {
			ID: 1, Incarnation: 1, Role: raft.Leader, Term: 5,
			DurableTerm: 5, VotedFor: 1, DurableVotedFor: 1,
		},
		2: {
			ID: 2, Incarnation: 1, Role: raft.Leader, Term: 5,
			DurableTerm: 5, VotedFor: 2, DurableVotedFor: 2,
		},
	}
	inv := raft.NewElectionSafetyWithSource([]sim.NodeID{1, 2}, viewMapSource(views))
	err := inv.Check(nil)
	if err == nil || !strings.Contains(err.Error(), "Election Safety") {
		t.Fatalf("expected Election Safety violation, got %v", err)
	}
}

func TestRaftInvariantCatchesLogMatchingViolation(t *testing.T) {
	logA := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryNoop},
		{Index: 2, Term: 1, Kind: raft.EntryCommand, Data: []byte("a")},
	}
	logB := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryNoop},
		{Index: 2, Term: 1, Kind: raft.EntryCommand, Data: []byte("b")},
	}
	views := map[sim.NodeID]raft.View{
		1: {
			ID: 1, Incarnation: 1, Role: raft.Follower, Term: 1,
			CommitIndex: 2, LastApplied: 2, LastLogIndex: 2, LastLogTerm: 1,
			DurableLastIndex: 2, Log: logA,
		},
		2: {
			ID: 2, Incarnation: 1, Role: raft.Follower, Term: 1,
			CommitIndex: 2, LastApplied: 2, LastLogIndex: 2, LastLogTerm: 1,
			DurableLastIndex: 2, Log: logB,
		},
	}
	inv := raft.NewLogMatchingWithSource([]sim.NodeID{1, 2}, viewMapSource(views))
	err := inv.Check(nil)
	if err == nil || !strings.Contains(err.Error(), "Log Matching") {
		t.Fatalf("expected Log Matching violation, got %v", err)
	}
}

func TestRaftInvariantCatchesStateMachineViolation(t *testing.T) {
	ids := []sim.NodeID{1, 2}
	recorder := raft.NewApplyRecorder(ids)
	recorder.Apply(1, raft.Entry{Index: 1, Term: 1, Kind: raft.EntryCommand, Data: []byte("a")})
	recorder.Apply(2, raft.Entry{Index: 1, Term: 1, Kind: raft.EntryCommand, Data: []byte("b")})
	if err := recorder.Err(); err == nil || !strings.Contains(err.Error(), "State Machine Safety") {
		t.Fatalf("expected recorder State Machine Safety violation, got %v", err)
	}
	log := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryCommand, Data: []byte("a")},
	}
	views := map[sim.NodeID]raft.View{
		1: {
			ID: 1, Incarnation: 1, Role: raft.Follower, Term: 1,
			CommitIndex: 1, LastApplied: 1, LastLogIndex: 1, LastLogTerm: 1,
			DurableLastIndex: 1, Log: log,
		},
	}
	inv := raft.NewStateMachineSafetyWithSource(recorder, viewMapSource(views))
	if err := inv.Check(nil); err == nil || !strings.Contains(err.Error(), "State Machine Safety") {
		t.Fatalf("expected invariant State Machine Safety violation, got %v", err)
	}
}

func TestRaftCommitDurabilityAllowsStaleLeader(t *testing.T) {
	// Regression for S2-001. A leader in an old term that is partitioned
	// from the majority may legitimately lack entries committed by a newer
	// term. Leader Completeness only requires entries committed at or before
	// the leader's own term.
	ids := []sim.NodeID{1, 2}
	recorder := raft.NewApplyRecorder(ids)
	views := make(map[sim.NodeID]raft.View)
	inv := raft.NewCommitDurabilityWithSource(ids, recorder, viewMapSource(views))

	term1Log := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryNoop},
	}
	views[1] = raft.View{
		ID: 1, Incarnation: 1, Role: raft.Leader, Term: 1,
		DurableTerm: 1, VotedFor: 1, DurableVotedFor: 1,
		CommitIndex: 1, LastApplied: 1, LastLogIndex: 1, LastLogTerm: 1,
		DurableLastIndex: 1, Log: term1Log,
	}
	if err := inv.Check(nil); err != nil {
		t.Fatalf("initial term-1 leader observation: %v", err)
	}

	term2Log := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryNoop},
		{Index: 2, Term: 2, Kind: raft.EntryCommand, Data: []byte("x")},
	}
	views[2] = raft.View{
		ID: 2, Incarnation: 1, Role: raft.Follower, Term: 2,
		CommitIndex: 2, LastApplied: 2, LastLogIndex: 2, LastLogTerm: 2,
		DurableLastIndex: 2, Log: term2Log,
	}
	if err := inv.Check(nil); err != nil {
		t.Fatalf("term-2 commit observation: %v", err)
	}

	if err := inv.Check(nil); err != nil {
		t.Fatalf("stale term-1 leader must be accepted: %v", err)
	}
}

func TestRaftInvariantCatchesCommitLoss(t *testing.T) {
	ids := []sim.NodeID{1}
	recorder := raft.NewApplyRecorder(ids)
	views := make(map[sim.NodeID]raft.View)
	inv := raft.NewCommitDurabilityWithSource(ids, recorder, viewMapSource(views))

	fullLog := []raft.Entry{
		{Index: 0, Term: 0, Kind: raft.EntrySentinel},
		{Index: 1, Term: 1, Kind: raft.EntryNoop},
		{Index: 2, Term: 1, Kind: raft.EntryCommand, Data: []byte("x")},
	}
	views[1] = raft.View{
		ID: 1, Incarnation: 1, Role: raft.Follower, Term: 1,
		CommitIndex: 2, LastApplied: 2, LastLogIndex: 2, LastLogTerm: 1,
		DurableLastIndex: 2, Log: fullLog,
	}
	if err := inv.Check(nil); err != nil {
		t.Fatalf("initial commit observation: %v", err)
	}

	views[1] = raft.View{
		ID: 1, Incarnation: 2, Role: raft.Follower, Term: 1,
		CommitIndex: 0, LastApplied: 0, LastLogIndex: 1, LastLogTerm: 1,
		DurableLastIndex: 1, Log: fullLog[:2],
	}
	err := inv.Check(nil)
	if err == nil || !strings.Contains(err.Error(), "committed") {
		t.Fatalf("expected Commit Durability violation, got %v", err)
	}
}

func TestRaftNewRaftValidation(t *testing.T) {
	if _, err := NewRaft(sim.Config{Seed: 1}, 2, DefaultRaftEnd); err == nil {
		t.Fatal("NewRaft accepted nodeCount 2")
	}
	if _, err := NewRaft(sim.Config{Seed: 1}, RaftNodeCount, -1); err == nil {
		t.Fatal("NewRaft accepted a negative end tick")
	}
}

func viewMapSource(views map[sim.NodeID]raft.View) raft.ViewSource {
	return func(_ *sim.Sim, id sim.NodeID) (raft.View, bool) {
		v, ok := views[id]
		return v, ok
	}
}

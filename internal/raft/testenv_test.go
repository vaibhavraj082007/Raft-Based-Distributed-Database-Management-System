package raft

import (
	"fmt"
	"sort"
	"testing"

	"raftkv/internal/sim"
)

type testMessage struct {
	from    sim.NodeID
	to      sim.NodeID
	kind    string
	payload []byte
}

type testRead struct {
	tag  uint64
	name string
	off  int64
	n    int64
}

type testReplace struct {
	tag  uint64
	name string
	data []byte
}

type testSync struct {
	tag uint64
}

type testAppend struct {
	tag  uint64
	name string
	data []byte
}

// testEnv is a deterministic sim.Env with a manually driven disk and clock.
// It models the simulator's durable/visible split: Replace changes only the
// visible bytes, Sync promotes them, and crash discards unsynced bytes.
type testEnv struct {
	id  sim.NodeID
	now sim.Tick

	nextTimer   sim.TimerID
	timers      map[sim.TimerID]sim.TimerEvent
	timerDelays map[sim.TimerID]sim.Tick

	sent     []testMessage
	reads    []testRead
	replaces []testReplace
	syncs    []testSync
	appends  []testAppend

	intRangeCalls [][2]int
	intnCalls     []int

	metaVisible []byte
	metaDurable []byte
}

func newTestEnv(id sim.NodeID) *testEnv {
	return &testEnv{
		id:          id,
		timers:      make(map[sim.TimerID]sim.TimerEvent),
		timerDelays: make(map[sim.TimerID]sim.Tick),
	}
}

func (e *testEnv) ID() sim.NodeID { return e.id }
func (e *testEnv) Now() sim.Tick  { return e.now }

func (e *testEnv) Send(to sim.NodeID, kind string, payload []byte) {
	e.sent = append(e.sent, testMessage{
		from:    e.id,
		to:      to,
		kind:    kind,
		payload: cloneTestBytes(payload),
	})
}

func (e *testEnv) After(delay sim.Tick, tag uint64) sim.TimerID {
	if delay < 0 {
		panic("testEnv: negative timer delay")
	}
	e.nextTimer++
	id := e.nextTimer
	e.timers[id] = sim.TimerEvent{ID: id, Tag: tag}
	e.timerDelays[id] = delay
	return id
}

func (e *testEnv) CancelTimer(id sim.TimerID) {
	delete(e.timers, id)
	delete(e.timerDelays, id)
}

func (e *testEnv) Uint64() uint64 { return 0 }
func (e *testEnv) Intn(n int) int {
	if n <= 0 {
		panic("testEnv: Intn requires n > 0")
	}
	e.intnCalls = append(e.intnCalls, n)
	return 0
}

func (e *testEnv) IntRange(min, max int) int {
	if max < min {
		panic("testEnv: IntRange min > max")
	}
	e.intRangeCalls = append(e.intRangeCalls, [2]int{min, max})
	span := max - min + 1
	return min + (len(e.intRangeCalls)-1)%span
}

func (e *testEnv) Int64Range(min, max int64) int64 {
	if max < min {
		panic("testEnv: Int64Range min > max")
	}
	return min
}

func (e *testEnv) Chance(permille int) bool { return false }

func (e *testEnv) Read(tag uint64, name string, off, n int64) {
	e.reads = append(e.reads, testRead{tag: tag, name: name, off: off, n: n})
}

func (e *testEnv) Append(tag uint64, name string, data []byte) {
	e.appends = append(e.appends, testAppend{tag: tag, name: name, data: cloneTestBytes(data)})
	if name == metaFile {
		e.metaVisible = append(e.metaVisible, data...)
	}
}

func (e *testEnv) Replace(tag uint64, name string, data []byte) {
	e.replaces = append(e.replaces, testReplace{tag: tag, name: name, data: cloneTestBytes(data)})
	if name == metaFile {
		e.metaVisible = cloneTestBytes(data)
	}
}

func (e *testEnv) Sync(tag uint64) {
	e.syncs = append(e.syncs, testSync{tag: tag})
}

func (e *testEnv) completeSync(tag uint64) {
	e.metaDurable = cloneTestBytes(e.metaVisible)
}

func (e *testEnv) List(tag uint64) {}

func (e *testEnv) crash() {
	e.metaVisible = cloneTestBytes(e.metaDurable)
	e.timers = make(map[sim.TimerID]sim.TimerEvent)
	e.timerDelays = make(map[sim.TimerID]sim.Tick)
}

func (e *testEnv) readData() []byte {
	return cloneTestBytes(e.metaDurable)
}

func (e *testEnv) clearTraffic() {
	e.sent = nil
	e.reads = nil
	e.replaces = nil
	e.syncs = nil
	e.appends = nil
}

func (e *testEnv) lastReplace() testReplace {
	if len(e.replaces) == 0 {
		panic("testEnv: no replace")
	}
	return e.replaces[len(e.replaces)-1]
}

func (e *testEnv) lastSync() testSync {
	if len(e.syncs) == 0 {
		panic("testEnv: no sync")
	}
	return e.syncs[len(e.syncs)-1]
}

func (e *testEnv) lastRead() testRead {
	if len(e.reads) == 0 {
		panic("testEnv: no read")
	}
	return e.reads[len(e.reads)-1]
}

func (e *testEnv) takeTimer(tag uint64) sim.TimerEvent {
	var ids []sim.TimerID
	for id, ev := range e.timers {
		if ev.Tag == tag {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 {
		panic(fmt.Sprintf("testEnv: no timer with tag %d", tag))
	}
	id := ids[0]
	ev := e.timers[id]
	delete(e.timers, id)
	delete(e.timerDelays, id)
	return ev
}

func (e *testEnv) timerCount(tag uint64) int {
	count := 0
	for _, ev := range e.timers {
		if ev.Tag == tag {
			count++
		}
	}
	return count
}

func (e *testEnv) timerDelay(id sim.TimerID) sim.Tick {
	return e.timerDelays[id]
}

func cloneTestBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func bootTestNode(t *testing.T, env *testEnv, id sim.NodeID, peers []sim.NodeID) *Node {
	t.Helper()
	factory := NewFactory(Config{ID: id, Peers: peers})
	created := factory(env)
	n, ok := created.(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", created)
	}
	n.Start()
	read := env.lastRead()
	n.HandleIO(sim.IOEvent{Tag: read.tag, Op: sim.IORead, Data: env.readData()})
	return n
}

func completeReplace(t *testing.T, env *testEnv, n *Node) {
	t.Helper()
	replace := env.lastReplace()
	n.HandleIO(sim.IOEvent{Tag: replace.tag, Op: sim.IOReplace})
}

func deliverSync(t *testing.T, env *testEnv, n *Node, tag uint64) {
	t.Helper()
	env.completeSync(tag)
	n.HandleIO(sim.IOEvent{Tag: tag, Op: sim.IOSync})
}

func completePersist(t *testing.T, env *testEnv, n *Node) {
	t.Helper()
	completeReplace(t, env, n)
	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
}

func fireElectionTimer(t *testing.T, env *testEnv, n *Node) {
	t.Helper()
	n.HandleTimer(env.takeTimer(timerTagElection))
}

func fireHeartbeatTimer(t *testing.T, env *testEnv, n *Node) {
	t.Helper()
	n.HandleTimer(env.takeTimer(timerTagHeartbeat))
}

func voteRequestMessage(from sim.NodeID, term uint64, lastIndex, lastTerm uint64) sim.Message {
	return sim.Message{
		From: from,
		To:   0,
		Kind: KindVoteRequest,
		Payload: encodeVoteRequest(VoteRequest{
			Term:         term,
			CandidateID:  from,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}),
	}
}

func voteResponseMessage(from sim.NodeID, term uint64, granted bool) sim.Message {
	return sim.Message{
		From:    from,
		Kind:    KindVoteResponse,
		Payload: encodeVoteResponse(VoteResponse{Term: term, VoteGranted: granted}),
	}
}

func heartbeatMessage(from sim.NodeID, term uint64) sim.Message {
	return sim.Message{
		From: from,
		Kind: KindAppendRequest,
		Payload: encodeAppendRequest(AppendRequest{
			Term:     term,
			LeaderID: from,
		}),
	}
}

func appendResponseMessage(from sim.NodeID, term uint64, success bool) sim.Message {
	return sim.Message{
		From:    from,
		Kind:    KindAppendResponse,
		Payload: encodeAppendResponse(AppendResponse{Term: term, Success: success}),
	}
}

func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	fn()
}

func newRaftSim(t *testing.T, seed uint64, nodeCount int) (*sim.Sim, []sim.NodeID) {
	t.Helper()
	s, err := sim.New(sim.Config{Seed: seed, Scenario: "raft-test"})
	if err != nil {
		t.Fatalf("sim.New: %v", err)
	}
	ids := make([]sim.NodeID, nodeCount)
	for i := range ids {
		ids[i] = sim.NodeID(i + 1)
	}
	for _, id := range ids {
		peers := make([]sim.NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		if err := s.AddNode(id, NewFactory(Config{ID: id, Peers: peers})); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	s.RegisterInvariant(NewElectionSafety(ids))
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return s, ids
}

func liveViews(t *testing.T, s *sim.Sim, ids []sim.NodeID) []View {
	t.Helper()
	var out []View
	for _, id := range ids {
		node, up := s.Node(id)
		if !up {
			continue
		}
		viewer, ok := node.(interface{ View() View })
		if !ok {
			t.Fatalf("node %d does not implement View", id)
		}
		v := viewer.View()
		if v.Recovering {
			continue
		}
		out = append(out, v)
	}
	return out
}

func mustNode(t *testing.T, s *sim.Sim, id sim.NodeID) *Node {
	t.Helper()
	node, up := s.Node(id)
	if !up {
		t.Fatalf("node %d is down", id)
	}
	n, ok := node.(*Node)
	if !ok {
		t.Fatalf("node %d is %T, want *Node", id, node)
	}
	return n
}

func singleLeader(t *testing.T, s *sim.Sim, ids []sim.NodeID) View {
	t.Helper()
	leaders := leadersIn(t, s, ids)
	if len(leaders) != 1 {
		t.Fatalf("want exactly one leader, got %d: %s", len(leaders), viewSummary(leaders))
	}
	return leaders[0]
}

func leadersIn(t *testing.T, s *sim.Sim, ids []sim.NodeID) []View {
	t.Helper()
	var leaders []View
	for _, v := range liveViews(t, s, ids) {
		if v.Role == Leader {
			leaders = append(leaders, v)
		}
	}
	return leaders
}

func viewSummary(views []View) string {
	if len(views) == 0 {
		return "[]"
	}
	out := ""
	for _, v := range views {
		out += fmt.Sprintf("[id=%d inc=%d role=%s term=%d vote=%d dterm=%d dvote=%d leader=%d]",
			v.ID, v.Incarnation, v.Role, v.Term, v.VotedFor, v.DurableTerm, v.DurableVotedFor, v.LeaderID)
	}
	return out
}

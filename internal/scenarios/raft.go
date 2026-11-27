package scenarios

import (
	"fmt"
	"sort"
	"strings"

	"raftkv/internal/raft"
	"raftkv/internal/sim"
)

// Stage 2C Troublemaker scenario. The simulator scenario name stays "raft" so
// replay commands are stable; RaftChaosProfile is the human-readable profile
// label used by the documentation.
const (
	RaftScenario     = "raft"
	RaftChaosProfile = "troublemaker"
	RaftNodeCount    = 5
	RaftChaosEnd     = sim.Tick(200)
	RaftClientStop   = sim.Tick(380)
	DefaultRaftEnd   = sim.Tick(400)
)

const (
	raftChaosStart   = sim.Tick(30)
	clientTagRequest = uint64(1)
)

// NewRaft builds the 5-node Troublemaker cluster, adds one non-voting client
// actor, registers all Stage 2 safety invariants, starts the simulation, and
// schedules the fixed chaos/finalizer events.
func NewRaft(cfg sim.Config, nodeCount int, end sim.Tick) (*sim.Sim, error) {
	if nodeCount < 3 {
		return nil, fmt.Errorf("scenarios: raft requires at least 3 nodes, got %d", nodeCount)
	}
	if end < 0 {
		return nil, fmt.Errorf("scenarios: end tick must be >= 0, got %d", end)
	}
	if cfg.Scenario == "" {
		cfg.Scenario = RaftScenario
	}

	s, err := sim.New(cfg)
	if err != nil {
		return nil, err
	}

	ids := make([]sim.NodeID, nodeCount)
	for i := range ids {
		ids[i] = sim.NodeID(i + 1)
	}
	clientID := sim.NodeID(nodeCount + 1)
	recorder := raft.NewApplyRecorder(ids)

	for _, id := range ids {
		factory := newRaftNodeFactory(id, raftPeers(ids, id), recorder)
		if err := s.AddNode(id, factory); err != nil {
			return nil, err
		}
	}

	if err := s.AddNode(clientID, newClientFactory(clientID, ids)); err != nil {
		return nil, err
	}

	s.RegisterInvariant(raft.NewElectionSafety(ids))
	s.RegisterInvariant(raft.NewLogMatching(ids))
	s.RegisterInvariant(raft.NewStateMachineSafety(recorder))
	s.RegisterInvariant(raft.NewCommitDurability(ids, recorder))

	if err := s.Start(); err != nil {
		return nil, err
	}
	if err := ScheduleRaftChaos(s, RaftChaosEnd); err != nil {
		return nil, err
	}
	return s, nil
}

func raftPeers(ids []sim.NodeID, id sim.NodeID) []sim.NodeID {
	peers := make([]sim.NodeID, 0, len(ids)-1)
	for _, other := range ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	return peers
}

func newRaftNodeFactory(id sim.NodeID, peers []sim.NodeID, recorder *raft.ApplyRecorder) sim.NodeFactory {
	nodeID := id
	return raft.NewFactory(raft.Config{
		ID:                 nodeID,
		Peers:              peers,
		HeartbeatInterval:  3,
		ElectionTimeoutMin: 12,
		ElectionTimeoutMax: 24,
		MaxAppendEntries:   64,
		Apply: func(e raft.Entry) {
			recorder.Apply(nodeID, e)
		},
	})
}

// clientNode is a deterministic non-voting client. It sends one command per
// timer and learns the current leader from Raft client responses. It never
// reads or writes its simulator disk.
type clientNode struct {
	env       sim.Env
	id        sim.NodeID
	peers     []sim.NodeID
	seq       uint64
	sent      uint64
	accepted  uint64
	committed uint64
	leaderID  sim.NodeID
	term      uint64
}

func newClientFactory(id sim.NodeID, peers []sim.NodeID) sim.NodeFactory {
	peerCopy := append([]sim.NodeID(nil), peers...)
	if len(peerCopy) == 0 {
		panic("scenarios: raft client requires at least one peer")
	}
	return func(env sim.Env) sim.Node {
		return &clientNode{
			env:   env,
			id:    id,
			peers: append([]sim.NodeID(nil), peerCopy...),
		}
	}
}

func (c *clientNode) Start() {
	delay := sim.Tick(c.env.IntRange(5, 15))
	c.env.After(delay, clientTagRequest)
}

func (c *clientNode) HandleIO(sim.IOEvent) {}

func (c *clientNode) HandleMessage(msg sim.Message) {
	if msg.Kind != raft.KindClientResponse {
		return
	}
	resp, err := raft.DecodeClientResponse(msg.Payload)
	if err != nil {
		panic(fmt.Sprintf("scenarios: client %d bad %s: %v", c.id, msg.Kind, err))
	}
	if resp.Term > c.term {
		c.term = resp.Term
		c.leaderID = 0
	}
	if resp.Term == c.term && resp.LeaderID != 0 {
		c.leaderID = resp.LeaderID
	}
	if resp.Accepted {
		c.accepted++
	}
	if resp.Committed {
		c.committed++
	}
}

func (c *clientNode) HandleTimer(ev sim.TimerEvent) {
	if ev.Tag != clientTagRequest {
		return
	}
	if c.env.Now() >= RaftClientStop {
		return
	}

	c.seq++
	var target sim.NodeID
	if c.leaderID != 0 && c.env.Intn(4) != 0 {
		target = c.leaderID
	} else {
		target = c.peers[c.env.IntRange(0, len(c.peers)-1)]
	}
	req := raft.ClientRequest{
		ClientID: c.id,
		Seq:      c.seq,
		Data:     []byte(fmt.Sprintf("cmd-%d", c.seq)),
	}
	c.env.Send(target, raft.KindClientRequest, raft.EncodeClientRequest(req))
	c.sent++

	next := c.env.Now() + sim.Tick(c.env.IntRange(15, 35))
	if next > RaftClientStop {
		next = RaftClientStop
	}
	c.env.After(next-c.env.Now(), clientTagRequest)
}

func (c *clientNode) Snapshot() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "client id=%d peers=%d", c.id, len(c.peers))
	for _, peer := range c.peers {
		fmt.Fprintf(&b, " %d", peer)
	}
	fmt.Fprintf(&b, " seq=%d sent=%d accepted=%d committed=%d leader=%d term=%d",
		c.seq, c.sent, c.accepted, c.committed, c.leaderID, c.term)
	return []byte(b.String())
}

// ScheduleRaftChaos schedules the fixed Troublemaker driver and the finalizer.
// The driver starts at tick 30 and never schedules another action at or after
// chaosEnd. The finalizer always runs at chaosEnd; it is simply not reached
// when the caller runs with a smaller end tick.
func ScheduleRaftChaos(s *sim.Sim, chaosEnd sim.Tick) error {
	if chaosEnd < 0 {
		return fmt.Errorf("scenarios: raft chaos end must be >= 0, got %d", chaosEnd)
	}
	if chaosEnd > raftChaosStart {
		if err := s.ScheduleFunc(raftChaosStart, "chaos", func(s *sim.Sim) error {
			return runRaftChaos(s, chaosEnd)
		}); err != nil {
			return err
		}
	}
	return s.ScheduleFunc(chaosEnd, "finalize", func(s *sim.Sim) error {
		return finalizeRaft(s)
	})
}

func runRaftChaos(s *sim.Sim, chaosEnd sim.Tick) error {
	if err := raftChaosAction(s); err != nil {
		return err
	}
	next := s.Now() + sim.Tick(s.IntRange(25, 60))
	if next >= chaosEnd {
		return nil
	}
	return s.ScheduleFunc(next, "chaos", func(s *sim.Sim) error {
		return runRaftChaos(s, chaosEnd)
	})
}

func raftChaosAction(s *sim.Sim) error {
	ids := raftNodeIDs(s)
	live := make([]sim.NodeID, 0, len(ids))
	down := make([]sim.NodeID, 0, len(ids))
	for _, id := range ids {
		if s.IsUp(id) {
			live = append(live, id)
		} else {
			down = append(down, id)
		}
	}

	switch roll := s.Intn(100); {
	case roll < 25:
		if len(live) > 1 {
			return s.Crash(live[s.IntRange(0, len(live)-1)])
		}
		if len(down) == 0 {
			return nil
		}
		return s.Restart(down[s.IntRange(0, len(down)-1)])
	case roll < 45:
		if len(down) == 0 {
			return nil
		}
		return s.Restart(down[s.IntRange(0, len(down)-1)])
	case roll < 60:
		if len(live) == 0 {
			return nil
		}
		return s.Isolate(live[s.IntRange(0, len(live)-1)])
	case roll < 75:
		if len(ids) < 2 {
			return nil
		}
		cut := s.IntRange(1, len(ids)-1)
		return s.Partition(ids[:cut], ids[cut:])
	case roll < 85:
		s.HealAll()
		return nil
	case roll < 95:
		return s.SetDropPermille(s.IntRange(0, 200))
	default:
		min := s.IntRange(1, 3)
		max := min + s.IntRange(0, 2)
		return s.SetLatency(sim.Tick(min), sim.Tick(max))
	}
}

func finalizeRaft(s *sim.Sim) error {
	s.HealAll()
	if err := s.SetDropPermille(0); err != nil {
		return err
	}
	if err := s.SetLatency(1, 3); err != nil {
		return err
	}
	for _, id := range raftNodeIDs(s) {
		if !s.IsUp(id) {
			if err := s.Restart(id); err != nil {
				return err
			}
		}
	}
	return nil
}

// raftNodeIDs returns the Raft peer IDs in ascending order. The scenario adds
// the client last as nodeCount+1, so the highest ID is not a Raft peer.
func raftNodeIDs(s *sim.Sim) []sim.NodeID {
	ids := append([]sim.NodeID(nil), s.NodeIDs()...)
	if len(ids) == 0 {
		return nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[:len(ids)-1]
}

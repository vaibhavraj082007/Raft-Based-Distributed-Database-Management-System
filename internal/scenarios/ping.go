// Package scenarios contains deterministic end-to-end workloads for the
// simulator. The ping scenario is a Stage 1 toy: it exercises clocks, the
// network, the async disk, crashes, restarts, and partitions without
// implementing any Raft behavior.
package scenarios

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"raftkv/internal/sim"
)

const (
	PingScenario       = "ping"
	journalFile        = "journal"
	tagRecoverRead     = uint64(1)
	timerTagPing       = uint64(1)
	baseJournalTag     = uint64(10)
	recoverReadMaxSize = int64(1 << 20)
)

// PingNode is a deterministic toy node. It pings random peers, replies to
// pings, journals each outgoing ping, and recovers the journal after restart.
type PingNode struct {
	env         sim.Env
	id          sim.NodeID
	peers       []sim.NodeID
	incarnation uint64
	seq         uint64
	sent        uint64
	recv        uint64
	pongIn      uint64
	pongOut     uint64

	durable      uint64
	recovered    uint64
	recoveryDone bool
	nextIO       uint64
	pendingSync  map[uint64]bool
}

func NewPingFactory(id sim.NodeID, peers []sim.NodeID) sim.NodeFactory {
	peerCopy := append([]sim.NodeID(nil), peers...)
	var incarnation uint64
	return func(env sim.Env) sim.Node {
		incarnation++
		return &PingNode{
			env:         env,
			id:          id,
			peers:       append([]sim.NodeID(nil), peerCopy...),
			incarnation: incarnation,
			pendingSync: make(map[uint64]bool),
		}
	}
}

func (n *PingNode) Start() {
	n.env.Read(tagRecoverRead, journalFile, 0, recoverReadMaxSize)
	n.schedulePing()
}

func (n *PingNode) schedulePing() {
	delay := sim.Tick(n.env.IntRange(10, 20))
	n.env.After(delay, timerTagPing)
}

func (n *PingNode) HandleTimer(ev sim.TimerEvent) {
	if ev.Tag != timerTagPing || len(n.peers) == 0 {
		return
	}
	peer := n.peers[n.env.IntRange(0, len(n.peers)-1)]
	n.seq++
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, n.seq)
	n.env.Send(peer, "ping", payload)
	n.sent++
	n.journal()
	n.schedulePing()
}

func (n *PingNode) HandleMessage(msg sim.Message) {
	switch msg.Kind {
	case "ping":
		n.recv++
		n.env.Send(msg.From, "pong", simClone(msg.Payload))
		n.pongOut++
	case "pong":
		n.pongIn++
	}
}

func (n *PingNode) HandleIO(ev sim.IOEvent) {
	if ev.Tag == tagRecoverRead && ev.Op == sim.IORead {
		n.recovered = uint64(bytes.Count(ev.Data, []byte{'\n'}))
		n.recoveryDone = true
		return
	}
	if n.pendingSync[ev.Tag] && ev.Op == sim.IOSync {
		delete(n.pendingSync, ev.Tag)
		n.durable++
	}
}

func (n *PingNode) journal() {
	n.nextIO++
	appendTag := baseJournalTag + n.nextIO*2
	syncTag := appendTag + 1
	line := fmt.Sprintf("p %d %d\n", n.id, n.seq)
	n.env.Append(appendTag, journalFile, []byte(line))
	n.env.Sync(syncTag)
	n.pendingSync[syncTag] = true
}

func (n *PingNode) DurableRecords() uint64   { return n.durable }
func (n *PingNode) RecoveredRecords() uint64 { return n.recovered }
func (n *PingNode) RecoveryDone() bool       { return n.recoveryDone }
func (n *PingNode) Incarnation() uint64      { return n.incarnation }
func (n *PingNode) Sent() uint64             { return n.sent }
func (n *PingNode) Received() uint64         { return n.recv }

func (n *PingNode) Snapshot() []byte {
	return []byte(fmt.Sprintf("ping id=%d inc=%d seq=%d sent=%d recv=%d pong-in=%d pong-out=%d durable=%d recovered=%d recovery-done=%t",
		n.id, n.incarnation, n.seq, n.sent, n.recv, n.pongIn, n.pongOut, n.durable, n.recovered, n.recoveryDone))
}

func simClone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// PingInvariant tracks, per node, the cumulative number of records observed
// synced across all incarnations. A restart must never recover more records
// than could have been synced by all prior incarnations.
type PingInvariant struct {
	ids      []sim.NodeID
	observed map[sim.NodeID]pingObservation
}

type pingObservation struct {
	incarnation      uint64
	lastDurable      uint64
	cumulativeSynced uint64
}

func NewPingInvariant(ids []sim.NodeID) *PingInvariant {
	cp := append([]sim.NodeID(nil), ids...)
	return &PingInvariant{ids: cp, observed: make(map[sim.NodeID]pingObservation)}
}

func (p *PingInvariant) Name() string { return "ping.recovery" }

func (p *PingInvariant) Check(s *sim.Sim) error {
	for _, id := range p.ids {
		node, up := s.Node(id)
		if !up {
			continue
		}
		pn, ok := node.(*PingNode)
		if !ok {
			return fmt.Errorf("node %d is not a PingNode", id)
		}
		obs := p.observed[id]
		if obs.incarnation != pn.Incarnation() {
			obs.incarnation = pn.Incarnation()
			obs.lastDurable = 0
		}
		d := pn.DurableRecords()
		if d < obs.lastDurable {
			return fmt.Errorf("node %d durable count went backwards within incarnation %d: %d -> %d",
				id, obs.incarnation, obs.lastDurable, d)
		}
		obs.cumulativeSynced += d - obs.lastDurable
		obs.lastDurable = d
		p.observed[id] = obs
		if pn.RecoveryDone() && pn.RecoveredRecords() > obs.cumulativeSynced {
			return fmt.Errorf("node %d recovered %d records but at most %d were observed synced",
				id, pn.RecoveredRecords(), obs.cumulativeSynced)
		}
	}
	return nil
}

// NewPing builds a ping simulation with node IDs 1..nodeCount, registers the
// recovery invariant, and schedules the chaos driver through end.
func NewPing(cfg sim.Config, nodeCount int, end sim.Tick) (*sim.Sim, error) {
	if nodeCount < 1 {
		return nil, fmt.Errorf("scenarios: nodeCount must be >= 1, got %d", nodeCount)
	}
	if end < 0 {
		return nil, fmt.Errorf("scenarios: end tick must be >= 0, got %d", end)
	}
	if cfg.Scenario == "" {
		cfg.Scenario = PingScenario
	}
	s, err := sim.New(cfg)
	if err != nil {
		return nil, err
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
		if err := s.AddNode(id, NewPingFactory(id, peers)); err != nil {
			return nil, err
		}
	}
	if err := s.Start(); err != nil {
		return nil, err
	}
	s.RegisterInvariant(NewPingInvariant(ids))
	if err := ScheduleChaos(s, end); err != nil {
		return nil, err
	}
	return s, nil
}

type chaos struct {
	end sim.Tick
}

// ScheduleChaos schedules the first chaos action at t=0, recurring every
// 50-150 ticks, plus a final heal/restart-all event at end.
func ScheduleChaos(s *sim.Sim, end sim.Tick) error {
	c := &chaos{end: end}
	if err := s.ScheduleFunc(0, "chaos", func(s *sim.Sim) error { return c.run(s) }); err != nil {
		return err
	}
	return s.ScheduleFunc(end, "finalize", func(s *sim.Sim) error { return c.finalize(s) })
}

func (c *chaos) run(s *sim.Sim) error {
	if err := c.act(s); err != nil {
		return err
	}
	if s.Now() >= c.end {
		return nil
	}
	next := s.Now() + sim.Tick(s.IntRange(50, 150))
	if next >= c.end {
		return nil
	}
	return s.ScheduleFunc(next, "chaos", func(s *sim.Sim) error { return c.run(s) })
}

func (c *chaos) act(s *sim.Sim) error {
	switch r := s.Intn(100); {
	case r < 25:
		live := s.LiveNodeIDs()
		if len(live) == 0 {
			return nil
		}
		return s.Crash(live[s.Intn(len(live))])
	case r < 45:
		down := s.DownNodeIDs()
		if len(down) == 0 {
			return nil
		}
		return s.Restart(down[s.Intn(len(down))])
	case r < 65:
		ids := s.NodeIDs()
		if len(ids) < 2 {
			return nil
		}
		cut := 1 + s.Intn(len(ids)-1)
		return s.Partition(ids[:cut], ids[cut:])
	case r < 75:
		s.HealAll()
		return nil
	case r < 90:
		return s.SetDropPermille(s.IntRange(0, 300))
	default:
		min := s.IntRange(1, 3)
		max := min + s.IntRange(0, 4)
		return s.SetLatency(sim.Tick(min), sim.Tick(max))
	}
}

func (c *chaos) finalize(s *sim.Sim) error {
	s.HealAll()
	if err := s.SetDropPermille(0); err != nil {
		return err
	}
	if err := s.SetLatency(1, 2); err != nil {
		return err
	}
	for _, id := range s.DownNodeIDs() {
		if err := s.Restart(id); err != nil {
			return err
		}
	}
	return nil
}

package sim

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type Tick int64

type NodeID uint16

type Message struct {
	From    NodeID
	To      NodeID
	Kind    string
	Payload []byte
}

type TimerID uint64

type TimerEvent struct {
	ID  TimerID
	Tag uint64
}

const (
	IORead    = "read"
	IOAppend  = "append"
	IOReplace = "replace"
	IOSync    = "sync"
	IOList    = "list"
)

type IOEvent struct {
	Tag  uint64
	Op   string
	Data []byte
	Err  error
}

// Node is a deterministic actor. All callbacks run on the simulator goroutine.
// Start is called once per boot; a restart constructs a fresh Node.
type Node interface {
	Start()
	HandleMessage(Message)
	HandleTimer(TimerEvent)
	HandleIO(IOEvent)
}

type NodeFactory func(Env) Node

// Env is the only capability surface available to simulated nodes.
type Env interface {
	ID() NodeID
	Now() Tick
	Send(to NodeID, kind string, payload []byte)
	After(delay Tick, tag uint64) TimerID
	CancelTimer(id TimerID)
	Uint64() uint64
	Intn(n int) int
	IntRange(min, max int) int
	Int64Range(min, max int64) int64
	Chance(permille int) bool
	Read(tag uint64, name string, off, n int64)
	Append(tag uint64, name string, data []byte)
	Replace(tag uint64, name string, data []byte)
	Sync(tag uint64)
	List(tag uint64)
}

type Config struct {
	Seed       uint64
	Scenario   string
	Network    NetworkConfig
	Disk       DiskConfig
	MaxSteps   uint64
	MaxTick    Tick
	Trace      io.Writer
	TraceLevel TraceLevel
}

type Stats struct {
	Events            uint64
	MessagesSent      uint64
	MessagesDelivered uint64
	MessagesDropped   uint64
	DiskOps           uint64
	Boots             uint64
	Crashes           uint64
	Restarts          uint64
}

type nodeState struct {
	id      NodeID
	factory NodeFactory
	node    Node
	env     *env
	disk    *disk
	up      bool
	epoch   uint64
}

type timerState struct {
	node  NodeID
	epoch uint64
	tag   uint64
}

type Sim struct {
	cfg Config

	now         Tick
	step        uint64
	nextSeq     uint64
	nextTimerID uint64
	queue       eventQueue
	rng         *Rand
	nodes       map[NodeID]*nodeState
	order       []NodeID
	timers      map[TimerID]timerState
	network     *network
	invariants  []Invariant
	tracer      *tracer
	stats       Stats
	started     bool
	terminal    error
}

func New(cfg Config) (*Sim, error) {
	if cfg.Scenario == "" {
		cfg.Scenario = "custom"
	}
	if cfg.Network.MinLatency == 0 && cfg.Network.MaxLatency == 0 {
		cfg.Network.MinLatency = 1
		cfg.Network.MaxLatency = 4
	}
	if cfg.Network.MinLatency < 0 || cfg.Network.MaxLatency < cfg.Network.MinLatency {
		return nil, fmt.Errorf("sim: invalid network latency range [%d,%d]", cfg.Network.MinLatency, cfg.Network.MaxLatency)
	}
	if cfg.Network.DropPermille < 0 || cfg.Network.DropPermille > 1000 {
		return nil, fmt.Errorf("sim: DropPermille must be in [0,1000], got %d", cfg.Network.DropPermille)
	}
	if cfg.Disk.Latency == 0 {
		cfg.Disk.Latency = 1
	}
	if cfg.Disk.Latency < 0 {
		return nil, fmt.Errorf("sim: disk latency must be >= 0, got %d", cfg.Disk.Latency)
	}
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = 1_000_000
	}
	if cfg.MaxTick == 0 {
		cfg.MaxTick = 1_000_000_000
	}
	if cfg.TraceLevel < TraceEvents || cfg.TraceLevel > TraceDecisions {
		return nil, fmt.Errorf("sim: invalid trace level %d", cfg.TraceLevel)
	}

	s := &Sim{
		cfg:     cfg,
		rng:     NewRand(cfg.Seed),
		nodes:   make(map[NodeID]*nodeState),
		timers:  make(map[TimerID]timerState),
		tracer:  newTracer(cfg.Trace, cfg.TraceLevel),
		started: false,
	}
	s.tracer.writeRaw(fmt.Sprintf("sim v=%d seed=%d scenario=%s level=%d\n",
		Version, cfg.Seed, cfg.Scenario, int(cfg.TraceLevel)))
	if s.tracer.err != nil {
		return nil, fmt.Errorf("sim: trace header: %w", s.tracer.err)
	}
	s.network = newNetwork(s, cfg.Network)
	return s, nil
}

func (s *Sim) AddNode(id NodeID, f NodeFactory) error {
	if s.started {
		return fmt.Errorf("sim: cannot add node %d after Start", id)
	}
	if id == 0 {
		return fmt.Errorf("sim: node IDs must be non-zero")
	}
	if f == nil {
		return fmt.Errorf("sim: node %d has nil factory", id)
	}
	if _, ok := s.nodes[id]; ok {
		return fmt.Errorf("sim: duplicate node %d", id)
	}
	ns := &nodeState{
		id:      id,
		factory: f,
		disk:    newDisk(s, id, s.cfg.Disk.Latency),
	}
	s.nodes[id] = ns
	s.order = append(s.order, id)
	sort.Slice(s.order, func(i, j int) bool { return s.order[i] < s.order[j] })
	return nil
}

func (s *Sim) Start() error {
	if s.terminal != nil {
		return s.terminal
	}
	if s.started {
		return fmt.Errorf("sim: already started")
	}
	s.started = true
	s.tracef("nodes=%s", nodeIDList(s.order))
	for _, id := range s.order {
		ns := s.nodes[id]
		if err := s.schedule(0, "boot", fmt.Sprintf("node=%d", id), func(*Sim) error {
			return s.boot(ns)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sim) boot(ns *nodeState) (err error) {
	if ns.up {
		return fmt.Errorf("sim: node %d is already up", ns.id)
	}
	env := &env{sim: s, id: ns.id, epoch: ns.epoch}
	defer func() {
		if r := recover(); r != nil {
			ns.up = false
			ns.node = nil
			ns.env = nil
			err = fmt.Errorf("sim: node %d panicked during boot: %v", ns.id, r)
		}
	}()
	node := ns.factory(env)
	if node == nil {
		return fmt.Errorf("sim: node %d factory returned nil", ns.id)
	}
	ns.node = node
	ns.env = env
	ns.up = true
	s.stats.Boots++
	s.tracef("boot node=%d epoch=%d", ns.id, ns.epoch)
	node.Start()
	return nil
}

func (s *Sim) Crash(id NodeID) error {
	ns, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("sim: unknown node %d", id)
	}
	if !ns.up {
		return fmt.Errorf("sim: node %d is down", id)
	}
	ns.epoch++
	ns.up = false
	ns.node = nil
	ns.env = nil
	ns.disk.crash()
	for tid, ts := range s.timers {
		if ts.node == id {
			delete(s.timers, tid)
		}
	}
	s.stats.Crashes++
	s.tracef("crash node=%d epoch=%d", id, ns.epoch)
	return nil
}

func (s *Sim) Restart(id NodeID) error {
	ns, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("sim: unknown node %d", id)
	}
	if ns.up {
		return fmt.Errorf("sim: node %d is already up", id)
	}
	s.stats.Restarts++
	s.tracef("restart node=%d epoch=%d", id, ns.epoch)
	return s.boot(ns)
}

func (s *Sim) ScheduleFunc(at Tick, name string, fn func(*Sim) error) error {
	if s.terminal != nil {
		return s.terminal
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("sim: schedule requires a non-empty name")
	}
	return s.schedule(at, "scenario", name, fn)
}

func (s *Sim) RegisterInvariant(inv Invariant) {
	if inv == nil {
		panic("sim: nil invariant")
	}
	if strings.TrimSpace(inv.Name()) == "" {
		panic("sim: invariant requires a name")
	}
	s.invariants = append(s.invariants, inv)
}

func (s *Sim) Block(from, to NodeID) error {
	if err := s.validateNode(from); err != nil {
		return err
	}
	if err := s.validateNode(to); err != nil {
		return err
	}
	s.network.block(from, to)
	s.tracef("block from=%d to=%d", from, to)
	return nil
}

func (s *Sim) Unblock(from, to NodeID) error {
	if err := s.validateNode(from); err != nil {
		return err
	}
	if err := s.validateNode(to); err != nil {
		return err
	}
	s.network.unblock(from, to)
	s.tracef("unblock from=%d to=%d", from, to)
	return nil
}

func (s *Sim) Partition(groups ...[]NodeID) error {
	seen := make(map[NodeID]bool)
	for _, g := range groups {
		for _, id := range g {
			if err := s.validateNode(id); err != nil {
				return err
			}
			if seen[id] {
				return fmt.Errorf("sim: node %d appears in more than one partition group", id)
			}
			seen[id] = true
		}
	}
	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			for _, a := range groups[i] {
				for _, b := range groups[j] {
					s.network.block(a, b)
					s.network.block(b, a)
				}
			}
		}
	}
	s.tracef("partition groups=%s", groupsString(groups))
	return nil
}

func (s *Sim) Isolate(id NodeID) error {
	if err := s.validateNode(id); err != nil {
		return err
	}
	for _, other := range s.order {
		if other == id {
			continue
		}
		s.network.block(id, other)
		s.network.block(other, id)
	}
	s.tracef("isolate node=%d", id)
	return nil
}

func (s *Sim) HealAll() {
	s.network.healAll()
	s.tracef("heal-all")
}

func (s *Sim) SetDropPermille(v int) error {
	if v < 0 || v > 1000 {
		return fmt.Errorf("sim: DropPermille must be in [0,1000], got %d", v)
	}
	s.network.cfg.DropPermille = v
	s.tracef("net drop-permille=%d", v)
	return nil
}

func (s *Sim) SetLatency(min, max Tick) error {
	if min < 0 || max < min {
		return fmt.Errorf("sim: invalid network latency range [%d,%d]", min, max)
	}
	s.network.cfg.MinLatency = min
	s.network.cfg.MaxLatency = max
	s.tracef("net latency min=%d max=%d", min, max)
	return nil
}

func (s *Sim) validateNode(id NodeID) error {
	if _, ok := s.nodes[id]; !ok {
		return fmt.Errorf("sim: unknown node %d", id)
	}
	return nil
}

func (s *Sim) Now() Tick                { return s.now }
func (s *Sim) Steps() uint64            { return s.step }
func (s *Sim) Stats() Stats             { return s.stats }
func (s *Sim) Seed() uint64             { return s.cfg.Seed }
func (s *Sim) Scenario() string         { return s.cfg.Scenario }
func (s *Sim) PendingEvents() int       { return len(s.queue) }
func (s *Sim) TraceHash() uint64        { return s.tracer.sum64() }
func (s *Sim) TraceTail(n int) []string { return s.tracer.tailCopy(n) }
func (s *Sim) Failed() error            { return s.terminal }

func (s *Sim) NodeIDs() []NodeID {
	out := make([]NodeID, len(s.order))
	copy(out, s.order)
	return out
}

func (s *Sim) LiveNodeIDs() []NodeID {
	var out []NodeID
	for _, id := range s.order {
		if s.nodes[id].up {
			out = append(out, id)
		}
	}
	return out
}

func (s *Sim) DownNodeIDs() []NodeID {
	var out []NodeID
	for _, id := range s.order {
		if !s.nodes[id].up {
			out = append(out, id)
		}
	}
	return out
}

func (s *Sim) Node(id NodeID) (Node, bool) {
	ns, ok := s.nodes[id]
	if !ok || !ns.up {
		return nil, false
	}
	return ns.node, true
}

func (s *Sim) IsUp(id NodeID) bool {
	ns, ok := s.nodes[id]
	return ok && ns.up
}

func (s *Sim) NextEventTick() (Tick, bool) {
	if len(s.queue) == 0 {
		return 0, false
	}
	return s.queue[0].at, true
}

func (s *Sim) Uint64() uint64 {
	v := s.rng.Uint64()
	if s.tracer.level >= TraceDecisions {
		s.tracef("rng value=%d", v)
	}
	return v
}

func (s *Sim) Intn(n int) int {
	v := s.rng.Intn(n)
	if s.tracer.level >= TraceDecisions {
		s.tracef("rng intn=%d n=%d", v, n)
	}
	return v
}

func (s *Sim) IntRange(min, max int) int {
	v := s.rng.IntRange(min, max)
	if s.tracer.level >= TraceDecisions {
		s.tracef("rng range=%d min=%d max=%d", v, min, max)
	}
	return v
}

func (s *Sim) Int64Range(min, max int64) int64 {
	v := s.rng.Int64Range(min, max)
	if s.tracer.level >= TraceDecisions {
		s.tracef("rng range64=%d min=%d max=%d", v, min, max)
	}
	return v
}

func (s *Sim) Chance(permille int) bool {
	v := s.rng.Chance(permille)
	if s.tracer.level >= TraceDecisions {
		s.tracef("rng chance=%t permille=%d", v, permille)
	}
	return v
}

func (s *Sim) tracef(format string, args ...any) {
	s.tracer.line(s.now, s.step, format, args...)
}

func nodeIDList(ids []NodeID) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", id)
	}
	return b.String()
}

func groupsString(groups [][]NodeID) string {
	var b strings.Builder
	for i, g := range groups {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(nodeIDList(g))
	}
	return b.String()
}

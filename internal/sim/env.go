package sim

import "fmt"

type env struct {
	sim   *Sim
	id    NodeID
	epoch uint64
}

func (e *env) live() *nodeState {
	ns, ok := e.sim.nodes[e.id]
	if !ok || !ns.up || ns.epoch != e.epoch {
		panic(fmt.Sprintf("sim: stale environment used by node %d", e.id))
	}
	return ns
}

func (e *env) ID() NodeID { return e.id }

func (e *env) Now() Tick {
	e.live()
	return e.sim.now
}

func (e *env) Send(to NodeID, kind string, payload []byte) {
	e.live()
	e.sim.network.send(e.id, to, kind, payload)
}

func (e *env) After(delay Tick, tag uint64) TimerID {
	e.live()
	if delay < 0 {
		panic(fmt.Sprintf("sim: node %d scheduled negative timer delay %d", e.id, delay))
	}
	at := e.sim.now + delay
	if at < e.sim.now {
		panic(fmt.Sprintf("sim: node %d timer overflow", e.id))
	}
	e.sim.nextTimerID++
	id := TimerID(e.sim.nextTimerID)
	e.sim.timers[id] = timerState{node: e.id, epoch: e.epoch, tag: tag}
	err := e.sim.schedule(at, "timer", fmt.Sprintf("node=%d id=%d tag=%d", e.id, id, tag), func(s *Sim) error {
		ts, ok := s.timers[id]
		if !ok {
			return nil
		}
		delete(s.timers, id)
		ns := s.nodes[ts.node]
		if !ns.up || ns.epoch != ts.epoch {
			s.tracef("drop timer node=%d id=%d reason=stale", ts.node, id)
			return nil
		}
		s.tracef("timer node=%d id=%d tag=%d", ts.node, id, ts.tag)
		ns.node.HandleTimer(TimerEvent{ID: id, Tag: ts.tag})
		return nil
	})
	if err != nil {
		panic(err)
	}
	return id
}

func (e *env) CancelTimer(id TimerID) {
	e.live()
	ts, ok := e.sim.timers[id]
	if !ok || ts.node != e.id || ts.epoch != e.epoch {
		return
	}
	delete(e.sim.timers, id)
	e.sim.tracef("cancel timer node=%d id=%d tag=%d", e.id, id, ts.tag)
}

func (e *env) Uint64() uint64 {
	e.live()
	return e.sim.Uint64()
}

func (e *env) Intn(n int) int {
	e.live()
	return e.sim.Intn(n)
}

func (e *env) IntRange(min, max int) int {
	e.live()
	return e.sim.IntRange(min, max)
}

func (e *env) Int64Range(min, max int64) int64 {
	e.live()
	return e.sim.Int64Range(min, max)
}

func (e *env) Chance(permille int) bool {
	e.live()
	return e.sim.Chance(permille)
}

func (e *env) Read(tag uint64, name string, off, n int64) {
	ns := e.live()
	ns.disk.issue(e.epoch, diskOp{tag: tag, kind: IORead, name: name, off: off, n: n})
}

func (e *env) Append(tag uint64, name string, data []byte) {
	ns := e.live()
	ns.disk.issue(e.epoch, diskOp{tag: tag, kind: IOAppend, name: name, data: cloneBytes(data)})
}

func (e *env) Replace(tag uint64, name string, data []byte) {
	ns := e.live()
	ns.disk.issue(e.epoch, diskOp{tag: tag, kind: IOReplace, name: name, data: cloneBytes(data)})
}

func (e *env) Sync(tag uint64) {
	ns := e.live()
	ns.disk.issue(e.epoch, diskOp{tag: tag, kind: IOSync})
}

func (e *env) List(tag uint64) {
	ns := e.live()
	ns.disk.issue(e.epoch, diskOp{tag: tag, kind: IOList})
}

package sim

import (
	"container/heap"
	"fmt"
)

type pendingEvent struct {
	at   Tick
	seq  uint64
	kind string
	desc string
	fn   func(*Sim) error
}

type eventQueue []*pendingEvent

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*pendingEvent)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return ev
}

func (s *Sim) schedule(at Tick, kind, desc string, fn func(*Sim) error) error {
	if fn == nil {
		return fmt.Errorf("sim: nil event handler for %s", kind)
	}
	if at < s.now {
		return fmt.Errorf("sim: cannot schedule %s at t=%d in the past (now=%d)", kind, at, s.now)
	}
	if at > s.cfg.MaxTick {
		return fmt.Errorf("sim: event %s at t=%d exceeds MaxTick=%d", kind, at, s.cfg.MaxTick)
	}
	s.nextSeq++
	heap.Push(&s.queue, &pendingEvent{
		at:   at,
		seq:  s.nextSeq,
		kind: kind,
		desc: desc,
		fn:   fn,
	})
	return nil
}

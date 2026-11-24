package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"runtime/debug"
)

func (s *Sim) RunUntil(target Tick) error {
	if err := s.preRun(target); err != nil {
		return err
	}
	if target < s.now {
		return fmt.Errorf("sim: cannot run backwards from t=%d to t=%d", s.now, target)
	}
	for len(s.queue) > 0 && s.queue[0].at <= target {
		if err := s.stepOnce(); err != nil {
			return err
		}
	}
	if s.now < target {
		s.now = target
	}
	return s.finishRun()
}

func (s *Sim) RunSteps(n uint64) error {
	if err := s.preRun(s.now); err != nil {
		return err
	}
	for i := uint64(0); i < n; i++ {
		if len(s.queue) == 0 {
			break
		}
		if err := s.stepOnce(); err != nil {
			return err
		}
	}
	return s.finishRun()
}

func (s *Sim) RunUntilIdle() error {
	if err := s.preRun(s.now); err != nil {
		return err
	}
	for len(s.queue) > 0 {
		if err := s.stepOnce(); err != nil {
			return err
		}
	}
	return s.finishRun()
}

func (s *Sim) preRun(target Tick) error {
	if s.terminal != nil {
		return s.terminal
	}
	if !s.started {
		return fmt.Errorf("sim: Start must be called before running")
	}
	if target > s.cfg.MaxTick {
		return fmt.Errorf("sim: run target t=%d exceeds MaxTick=%d", target, s.cfg.MaxTick)
	}
	return nil
}

func (s *Sim) stepOnce() error {
	if s.terminal != nil {
		return s.terminal
	}
	if len(s.queue) == 0 {
		return nil
	}
	if s.step >= s.cfg.MaxSteps {
		return s.setFailure(nil, "", fmt.Errorf("sim: exceeded MaxSteps=%d", s.cfg.MaxSteps))
	}
	ev := heap.Pop(&s.queue).(*pendingEvent)
	if ev.at < s.now {
		return s.setFailure(ev, "structure", fmt.Errorf("event scheduled in the past: at=%d now=%d", ev.at, s.now))
	}
	s.now = ev.at
	s.step++
	s.stats.Events++
	s.tracef("event seq=%d kind=%s desc=%s", ev.seq, ev.kind, ev.desc)

	if err := s.dispatch(ev); err != nil {
		return s.setFailure(ev, "", err)
	}
	if s.tracer.err != nil {
		return s.setFailure(ev, "", fmt.Errorf("trace write: %w", s.tracer.err))
	}
	if name, err := s.checkInvariants(); err != nil {
		return s.setFailure(ev, name, err)
	}
	return nil
}

func (s *Sim) dispatch(ev *pendingEvent) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v\n%s", ev.desc, r, debug.Stack())
		}
	}()
	return ev.fn(s)
}

func (s *Sim) finishRun() error {
	if s.terminal != nil {
		return s.terminal
	}
	if s.tracer.err != nil {
		return s.setFailure(nil, "", fmt.Errorf("trace write: %w", s.tracer.err))
	}
	if name, err := s.checkInvariants(); err != nil {
		return s.setFailure(nil, name, err)
	}
	return nil
}

func (s *Sim) checkInvariants() (string, error) {
	if s.now < 0 {
		return "structure", fmt.Errorf("negative simulated time %d", s.now)
	}
	if len(s.queue) > 0 && s.queue[0].at < s.now {
		return "structure", fmt.Errorf("queue contains event in the past: at=%d now=%d", s.queue[0].at, s.now)
	}
	for _, inv := range s.invariants {
		if err := safeInvariant(inv, s); err != nil {
			return inv.Name(), err
		}
	}
	return "", nil
}

func safeInvariant(inv Invariant, s *Sim) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invariant panic: %v", r)
		}
	}()
	return inv.Check(s)
}

func (s *Sim) setFailure(ev *pendingEvent, invariant string, cause error) error {
	if s.terminal != nil {
		return s.terminal
	}
	var prior *Failure
	if errors.As(cause, &prior) {
		s.terminal = cause
		return cause
	}
	event := "end"
	if ev != nil {
		event = fmt.Sprintf("seq=%d kind=%s desc=%s", ev.seq, ev.kind, ev.desc)
	}
	s.tracef("failed event=%s invariant=%s cause=%s", event, invariant, cause)
	f := &Failure{
		Seed:      s.cfg.Seed,
		Scenario:  s.cfg.Scenario,
		Version:   Version,
		Step:      s.step,
		Tick:      s.now,
		Event:     event,
		Invariant: invariant,
		Cause:     cause,
		Replay:    fmt.Sprintf("go run ./cmd/simctl -seed %d -scenario %s -ticks %d -trace", s.cfg.Seed, s.cfg.Scenario, s.now),
		TraceTail: s.tracer.tailCopy(40),
	}
	s.terminal = f
	return f
}

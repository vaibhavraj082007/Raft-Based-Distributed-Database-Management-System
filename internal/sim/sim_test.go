package sim

import (
	"errors"
	"strings"
	"testing"
)

type testNode struct {
	env     Env
	starts  int
	onStart func(*testNode)
	onMsg   func(*testNode, Message)
	onTimer func(*testNode, TimerEvent)
	onIO    func(*testNode, IOEvent)
}

func (n *testNode) Start() {
	n.starts++
	if n.onStart != nil {
		n.onStart(n)
	}
}

func (n *testNode) HandleMessage(m Message) {
	if n.onMsg != nil {
		n.onMsg(n, m)
	}
}

func (n *testNode) HandleTimer(ev TimerEvent) {
	if n.onTimer != nil {
		n.onTimer(n, ev)
	}
}

func (n *testNode) HandleIO(ev IOEvent) {
	if n.onIO != nil {
		n.onIO(n, ev)
	}
}

func installTestNode(t *testing.T, s *Sim, id NodeID, configure func(*testNode)) **testNode {
	t.Helper()
	var current *testNode
	factory := func(env Env) Node {
		n := &testNode{env: env}
		if configure != nil {
			configure(n)
		}
		current = n
		return n
	}
	if err := s.AddNode(id, factory); err != nil {
		t.Fatalf("AddNode(%d): %v", id, err)
	}
	return &current
}

func mustNew(t *testing.T, cfg Config) *Sim {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func mustStart(t *testing.T, s *Sim) {
	t.Helper()
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestRunUntilAdvancesIdleClock(t *testing.T) {
	s := mustNew(t, Config{Seed: 1})
	mustStart(t, s)
	if err := s.RunUntil(10); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	if s.Now() != 10 {
		t.Fatalf("now = %d, want 10", s.Now())
	}
	if s.Steps() != 0 {
		t.Fatalf("steps = %d, want 0", s.Steps())
	}
}

func TestSameTickOrdering(t *testing.T) {
	s := mustNew(t, Config{Seed: 2})
	mustStart(t, s)
	var order []string
	if err := s.ScheduleFunc(5, "a", func(s *Sim) error {
		order = append(order, "a")
		return s.ScheduleFunc(5, "d", func(*Sim) error {
			order = append(order, "d")
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b", "c"} {
		name := name
		if err := s.ScheduleFunc(5, name, func(*Sim) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RunUntil(5); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	want := []string{"a", "b", "c", "d"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestSchedulePastRejected(t *testing.T) {
	s := mustNew(t, Config{Seed: 3})
	mustStart(t, s)
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleFunc(4, "past", func(*Sim) error { return nil }); err == nil {
		t.Fatal("expected past scheduling error")
	}
}

func TestTimersFireOnce(t *testing.T) {
	s := mustNew(t, Config{Seed: 4})
	var fired int
	installTestNode(t, s, 1, func(n *testNode) {
		n.onStart = func(n *testNode) { n.env.After(2, 7) }
		n.onTimer = func(_ *testNode, ev TimerEvent) {
			if ev.Tag != 7 {
				t.Errorf("tag = %d, want 7", ev.Tag)
			}
			fired++
		}
	})
	mustStart(t, s)
	if err := s.RunUntil(10); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	if fired != 1 {
		t.Fatalf("timer fired %d times, want 1", fired)
	}
}

func TestCancelTimer(t *testing.T) {
	s := mustNew(t, Config{Seed: 40})
	var fired int
	installTestNode(t, s, 1, func(n *testNode) {
		n.onStart = func(n *testNode) {
			id := n.env.After(5, 9)
			n.env.CancelTimer(id)
		}
		n.onTimer = func(*testNode, TimerEvent) { fired++ }
	})
	mustStart(t, s)
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if fired != 0 {
		t.Fatalf("canceled timer fired %d times", fired)
	}
}

func TestMaxStepsGuard(t *testing.T) {
	s := mustNew(t, Config{Seed: 5, MaxSteps: 5})
	mustStart(t, s)
	var loop func(*Sim) error
	loop = func(s *Sim) error {
		return s.ScheduleFunc(s.Now(), "loop", loop)
	}
	if err := s.ScheduleFunc(0, "loop", loop); err != nil {
		t.Fatal(err)
	}
	err := s.RunUntilIdle()
	if err == nil {
		t.Fatal("expected MaxSteps failure")
	}
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is %T, want *Failure", err)
	}
	if !strings.Contains(err.Error(), "MaxSteps") {
		t.Fatalf("error does not mention MaxSteps: %v", err)
	}
	if !strings.Contains(f.Replay, "-seed 5") {
		t.Fatalf("replay = %q", f.Replay)
	}
}

func TestInvariantRunsEveryStep(t *testing.T) {
	s := mustNew(t, Config{Seed: 6})
	mustStart(t, s)
	checks := 0
	s.RegisterInvariant(NewInvariant("count", func(*Sim) error {
		checks++
		return nil
	}))
	for _, at := range []Tick{0, 1, 2} {
		if err := s.ScheduleFunc(at, "tick", func(*Sim) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RunUntilIdle(); err != nil {
		t.Fatalf("RunUntilIdle: %v", err)
	}
	if checks != 4 {
		t.Fatalf("invariant checks = %d, want 4 (3 events + final)", checks)
	}
}

func TestInvariantFailureReport(t *testing.T) {
	s := mustNew(t, Config{Seed: 77, Scenario: "invariant-test"})
	mustStart(t, s)
	s.RegisterInvariant(NewInvariant("always-fails", func(*Sim) error {
		return errors.New("boom")
	}))
	if err := s.ScheduleFunc(0, "one", func(*Sim) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := s.RunUntilIdle()
	if err == nil {
		t.Fatal("expected invariant failure")
	}
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is %T, want *Failure", err)
	}
	if f.Seed != 77 || f.Step != 1 || f.Invariant != "always-fails" {
		t.Fatalf("bad failure: %+v", f)
	}
	if !strings.Contains(f.Replay, "-seed 77") || !strings.Contains(f.Replay, "invariant-test") {
		t.Fatalf("bad replay: %q", f.Replay)
	}
	if len(f.TraceTail) == 0 {
		t.Fatal("failure has no trace tail")
	}
}

func TestPanicIsCaptured(t *testing.T) {
	s := mustNew(t, Config{Seed: 8})
	var sender *testNode
	installTestNode(t, s, 1, func(n *testNode) { sender = n })
	installTestNode(t, s, 2, func(n *testNode) {
		n.onMsg = func(*testNode, Message) { panic("kaboom") }
	})
	mustStart(t, s)
	if err := s.ScheduleFunc(5, "boom", func(*Sim) error {
		sender.env.Send(2, "boom", nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := s.RunUntil(10)
	if err == nil {
		t.Fatal("expected panic failure")
	}
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is %T, want *Failure", err)
	}
	if !strings.Contains(err.Error(), "kaboom") || !strings.Contains(f.Replay, "-seed 8") {
		t.Fatalf("bad failure: %v", err)
	}
}

func TestFreshNodeFactoryOnRestart(t *testing.T) {
	s := mustNew(t, Config{Seed: 9})
	factories := 0
	var node *testNode
	if err := s.AddNode(1, func(env Env) Node {
		factories++
		node = &testNode{env: env}
		return node
	}); err != nil {
		t.Fatal(err)
	}
	mustStart(t, s)
	if err := s.RunUntil(0); err != nil {
		t.Fatal(err)
	}
	first := node
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	if factories != 2 {
		t.Fatalf("factory calls = %d, want 2", factories)
	}
	if node == first {
		t.Fatal("restart reused the old node instance")
	}
	if node.starts != 1 {
		t.Fatalf("new node starts = %d, want 1", node.starts)
	}
}

func TestTimerAndIOEpochFiltering(t *testing.T) {
	s := mustNew(t, Config{Seed: 10})
	var delivered []uint64
	boots := 0
	installTestNode(t, s, 1, func(n *testNode) {
		boots++
		if boots != 1 {
			return // the fresh incarnation must not re-issue the old operations
		}
		n.onStart = func(n *testNode) {
			n.env.After(5, 100)
			n.env.Append(200, "f", []byte("old"))
		}
		n.onTimer = func(_ *testNode, ev TimerEvent) { delivered = append(delivered, ev.Tag) }
		n.onIO = func(_ *testNode, ev IOEvent) { delivered = append(delivered, ev.Tag) }
	})
	mustStart(t, s)
	if err := s.RunUntil(0); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 0 {
		t.Fatalf("stale events delivered: %v", delivered)
	}
	trace := strings.Join(s.TraceTail(200), "\n")
	if !strings.Contains(trace, "drop disk") {
		t.Fatalf("expected stale disk drop in trace:\n%s", trace)
	}
}

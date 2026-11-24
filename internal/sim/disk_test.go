package sim

import (
	"testing"
)

type ioProbe struct {
	env     Env
	sink    *[]IOEvent
	onStart func(*ioProbe)
}

func (p *ioProbe) Start() {
	if p.onStart != nil {
		p.onStart(p)
	}
}

func (p *ioProbe) HandleMessage(Message)  {}
func (p *ioProbe) HandleTimer(TimerEvent) {}
func (p *ioProbe) HandleIO(ev IOEvent)    { *p.sink = append(*p.sink, ev) }

func installIOProbe(t *testing.T, s *Sim, id NodeID, onStart func(*ioProbe)) (**ioProbe, *[]IOEvent) {
	t.Helper()
	var current *ioProbe
	var sink []IOEvent
	factory := func(env Env) Node {
		p := &ioProbe{env: env, sink: &sink, onStart: onStart}
		current = p
		return p
	}
	if err := s.AddNode(id, factory); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return &current, &sink
}

func bootIOProbe(t *testing.T, cfg Config) (*Sim, **ioProbe, *[]IOEvent) {
	t.Helper()
	s := mustNew(t, cfg)
	p, sink := installIOProbe(t, s, 1, nil)
	mustStart(t, s)
	if err := s.RunUntil(0); err != nil {
		t.Fatalf("boot: %v", err)
	}
	return s, p, sink
}

func findIO(sink []IOEvent, tag uint64) (IOEvent, bool) {
	for _, ev := range sink {
		if ev.Tag == tag {
			return ev, true
		}
	}
	return IOEvent{}, false
}

func TestDiskReadYourWritesBeforeSync(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 20})
	(*p).env.Append(1, "f", []byte("hello"))
	(*p).env.Read(2, "f", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, ok := findIO(*sink, 2)
	if !ok {
		t.Fatal("read did not complete")
	}
	if string(ev.Data) != "hello" {
		t.Fatalf("read = %q, want hello", ev.Data)
	}
}

func TestDiskUnsyncedDataLostOnCrash(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 21})
	(*p).env.Append(1, "f", []byte("volatile"))
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	(*p).env.Read(2, "f", 0, 100)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, ok := findIO(*sink, 2)
	if !ok {
		t.Fatal("post-restart read did not complete")
	}
	if len(ev.Data) != 0 {
		t.Fatalf("read after crash = %q, want empty", ev.Data)
	}
}

func TestDiskSyncedDataSurvivesRestart(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 22})
	(*p).env.Append(1, "f", []byte("durable"))
	(*p).env.Sync(2)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	(*p).env.Read(3, "f", 0, 100)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, ok := findIO(*sink, 3)
	if !ok {
		t.Fatal("post-restart read did not complete")
	}
	if string(ev.Data) != "durable" {
		t.Fatalf("read after restart = %q, want durable", ev.Data)
	}
}

func TestDiskPendingOpsCanceledOnCrash(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 23})
	(*p).env.Append(1, "f", []byte("lost"))
	// Crash before the completion at t=1.
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if _, ok := findIO(*sink, 1); ok {
		t.Fatal("pending append completion was delivered after crash")
	}
	(*p).env.Read(2, "f", 0, 100)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, ok := findIO(*sink, 2)
	if !ok {
		t.Fatal("read did not complete")
	}
	if len(ev.Data) != 0 {
		t.Fatalf("canceled append was applied: %q", ev.Data)
	}
}

func TestDiskFIFO(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 24})
	(*p).env.Append(1, "f", []byte("a"))
	(*p).env.Append(2, "f", []byte("b"))
	(*p).env.Sync(3)
	(*p).env.Read(4, "f", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if len(*sink) != 4 {
		t.Fatalf("completions = %d, want 4", len(*sink))
	}
	for i, want := range []uint64{1, 2, 3, 4} {
		if (*sink)[i].Tag != want {
			t.Fatalf("completion %d tag = %d, want %d", i, (*sink)[i].Tag, want)
		}
	}
	if string((*sink)[3].Data) != "ab" {
		t.Fatalf("read = %q, want ab", (*sink)[3].Data)
	}
}

func TestDiskSyncCoversPriorAppends(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 25})
	(*p).env.Append(1, "f", []byte("a"))
	(*p).env.Append(2, "f", []byte("b"))
	(*p).env.Sync(3)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	(*p).env.Read(4, "f", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, _ := findIO(*sink, 4)
	if string(ev.Data) != "ab" {
		t.Fatalf("read = %q, want ab", ev.Data)
	}
}

func TestDiskListAndRecovery(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 26})
	(*p).env.Append(1, "x", []byte("a"))
	(*p).env.Append(2, "y", []byte("bb"))
	(*p).env.Sync(3)
	(*p).env.List(4)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, _ := findIO(*sink, 4)
	if string(ev.Data) != "x\t1\ny\t2\n" {
		t.Fatalf("list = %q", ev.Data)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	(*p).env.List(5)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, _ = findIO(*sink, 5)
	if string(ev.Data) != "x\t1\ny\t2\n" {
		t.Fatalf("list after restart = %q", ev.Data)
	}
}

func TestDiskReplace(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 27})
	(*p).env.Replace(1, "f", []byte("abc"))
	(*p).env.Sync(2)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	(*p).env.Append(3, "f", []byte("d"))
	(*p).env.Sync(4)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	(*p).env.Read(5, "f", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, _ := findIO(*sink, 5)
	if string(ev.Data) != "abcd" {
		t.Fatalf("read = %q, want abcd", ev.Data)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	(*p).env.Read(6, "f", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, _ = findIO(*sink, 6)
	if string(ev.Data) != "abcd" {
		t.Fatalf("read after restart = %q, want abcd", ev.Data)
	}
}

func TestDiskCompletionLatency(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 28, Disk: DiskConfig{Latency: 3}})
	(*p).env.Append(1, "f", []byte("x"))
	if err := s.RunUntil(2); err != nil {
		t.Fatal(err)
	}
	if len(*sink) != 0 {
		t.Fatal("disk completion arrived before latency elapsed")
	}
	if err := s.RunUntil(3); err != nil {
		t.Fatal(err)
	}
	if len(*sink) != 1 {
		t.Fatalf("completions = %d, want 1", len(*sink))
	}
}

func TestDiskReadMissingFile(t *testing.T) {
	s, p, sink := bootIOProbe(t, Config{Seed: 29})
	(*p).env.Read(1, "missing", 0, 10)
	if err := s.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	ev, ok := findIO(*sink, 1)
	if !ok {
		t.Fatal("read did not complete")
	}
	if ev.Err != nil || len(ev.Data) != 0 {
		t.Fatalf("read missing file: data=%q err=%v", ev.Data, ev.Err)
	}
}

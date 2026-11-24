package scenarios

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"raftkv/internal/sim"
)

var updateGolden = flag.Bool("update", false, "update golden trace files")

func newPingWithTrace(t *testing.T, seed uint64, end sim.Tick) (*sim.Sim, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s, err := NewPing(sim.Config{
		Seed:     seed,
		Scenario: PingScenario,
		Trace:    &buf,
	}, 5, end)
	if err != nil {
		t.Fatalf("NewPing: %v", err)
	}
	return s, &buf
}

func runPing(t *testing.T, seed uint64, end sim.Tick) (string, uint64, uint64) {
	t.Helper()
	s, buf := newPingWithTrace(t, seed, end)
	if err := s.RunUntil(end); err != nil {
		t.Fatalf("RunUntil: %v", err)
	}
	return buf.String(), s.TraceHash(), s.StateHash()
}

func TestSameSeedProducesIdenticalTrace(t *testing.T) {
	const seed = 42
	trace0, hash0, state0 := runPing(t, seed, 500)
	for i := 1; i < 3; i++ {
		trace, hash, state := runPing(t, seed, 500)
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

func TestInterleavedSimsDoNotShareState(t *testing.T) {
	const seed = 77
	const end sim.Tick = 500

	soloTrace, soloHash, soloState := runPing(t, seed, end)

	a, aBuf := newPingWithTrace(t, seed, end)
	b, bBuf := newPingWithTrace(t, seed, end)
	for {
		advanced := false
		for _, s := range []*sim.Sim{a, b} {
			if at, ok := s.NextEventTick(); ok && at <= end {
				if err := s.RunSteps(1); err != nil {
					t.Fatalf("interleaved step: %v", err)
				}
				advanced = true
			}
		}
		if !advanced {
			break
		}
	}
	for _, s := range []*sim.Sim{a, b} {
		if err := s.RunUntil(end); err != nil {
			t.Fatalf("finish interleaved: %v", err)
		}
	}
	if aBuf.String() != bBuf.String() {
		t.Fatalf("interleaved traces differ\n%s", firstDiff(aBuf.String(), bBuf.String()))
	}
	if aBuf.String() != soloTrace {
		t.Fatalf("interleaved trace differs from solo\n%s", firstDiff(soloTrace, aBuf.String()))
	}
	if a.TraceHash() != soloHash || b.TraceHash() != soloHash {
		t.Fatalf("interleaved trace hashes %016x/%016x, want %016x", a.TraceHash(), b.TraceHash(), soloHash)
	}
	if a.StateHash() != soloState || b.StateHash() != soloState {
		t.Fatalf("interleaved state hashes %016x/%016x, want %016x", a.StateHash(), b.StateHash(), soloState)
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	_, hash1, state1 := runPing(t, 1, 200)
	_, hash2, state2 := runPing(t, 2, 200)
	if hash1 == hash2 {
		t.Fatalf("different seeds produced the same trace hash %016x", hash1)
	}
	if state1 == state2 {
		t.Fatalf("different seeds produced the same state hash %016x", state1)
	}
}

func TestGoldenTraceSeed42(t *testing.T) {
	const seed = 42
	const end sim.Tick = 120
	trace, _, _ := runPing(t, seed, end)
	path := filepath.Join("..", "..", "testdata", "stage1", "ping-seed-42.trace")
	if *updateGolden {
		if err := os.WriteFile(path, []byte(trace), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	want := string(wantBytes)
	if trace != want {
		t.Fatalf("golden trace differs\n%s", firstDiff(want, trace))
	}
}

func TestPingInvariantAcrossMultipleRestarts(t *testing.T) {
	s, err := sim.New(sim.Config{Seed: 5, Scenario: "ping-restarts"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []sim.NodeID{1, 2, 3, 4, 5}
	for _, id := range ids {
		var peers []sim.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		if err := s.AddNode(id, NewPingFactory(id, peers)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	s.RegisterInvariant(NewPingInvariant(ids))
	for _, at := range []sim.Tick{50, 150, 250} {
		at := at
		if err := s.ScheduleFunc(at, "crash-1", func(s *sim.Sim) error { return s.Crash(1) }); err != nil {
			t.Fatal(err)
		}
		if err := s.ScheduleFunc(at+1, "restart-1", func(s *sim.Sim) error { return s.Restart(1) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RunUntil(500); err != nil {
		t.Fatalf("multiple restarts violated recovery invariant: %v", err)
	}
	node, ok := s.Node(1)
	if !ok {
		t.Fatal("node 1 is down at end")
	}
	pn := node.(*PingNode)
	if pn.Incarnation() != 4 {
		t.Fatalf("node 1 incarnation = %d, want 4", pn.Incarnation())
	}
	if pn.RecoveredRecords() == 0 {
		t.Fatal("expected node 1 to recover journal records")
	}
}

func Test100SeedSmoke(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		s, err := NewPing(sim.Config{Seed: seed, Scenario: PingScenario}, 5, 300)
		if err != nil {
			t.Fatalf("seed %d setup: %v", seed, err)
		}
		if err := s.RunUntil(300); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

func firstDiff(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < len(wantLines) && i < len(gotLines); i++ {
		if wantLines[i] != gotLines[i] {
			return fmt.Sprintf("line %d:\n  want: %s\n  got:  %s", i+1, wantLines[i], gotLines[i])
		}
	}
	return fmt.Sprintf("length differs: want %d lines, got %d lines", len(wantLines), len(gotLines))
}

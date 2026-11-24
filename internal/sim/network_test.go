package sim

import (
	"bytes"
	"testing"
)

type deliveredMsg struct {
	at  Tick
	msg Message
}

func networkPair(t *testing.T, cfg NetworkConfig) (*Sim, **testNode, **testNode, *[]deliveredMsg) {
	t.Helper()
	s := mustNew(t, Config{Seed: 11, Network: cfg})
	var delivered []deliveredMsg
	sender := installTestNode(t, s, 1, nil)
	receiver := installTestNode(t, s, 2, func(n *testNode) {
		n.onMsg = func(n *testNode, msg Message) {
			delivered = append(delivered, deliveredMsg{at: n.env.Now(), msg: msg})
		}
	})
	mustStart(t, s)
	if err := s.RunUntil(0); err != nil {
		t.Fatal(err)
	}
	return s, sender, receiver, &delivered
}

func TestNetworkLatencyWithinBounds(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 2, MaxLatency: 5})
	for i := 0; i < 20; i++ {
		(*sender).env.Send(2, "msg", nil)
	}
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 20 {
		t.Fatalf("delivered %d, want 20", len(*delivered))
	}
	for i, d := range *delivered {
		if d.at < 2 || d.at > 5 {
			t.Fatalf("delivery %d at t=%d outside [2,5]", i, d.at)
		}
	}
}

func TestNetworkDropRate(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 1, MaxLatency: 1, DropPermille: 250})
	const n = 10_000
	for i := 0; i < n; i++ {
		(*sender).env.Send(2, "msg", nil)
	}
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) < 7200 || len(*delivered) > 7800 {
		t.Fatalf("delivered %d of %d at 25%% loss; expected ~7500", len(*delivered), n)
	}
}

func TestNetworkDropAll(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 1, MaxLatency: 1, DropPermille: 1000})
	for i := 0; i < 100; i++ {
		(*sender).env.Send(2, "msg", nil)
	}
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 0 {
		t.Fatalf("delivered %d messages with 100%% loss", len(*delivered))
	}
}

func TestNetworkPartitionBothDirections(t *testing.T) {
	s, sender, receiver, delivered := networkPair(t, NetworkConfig{MinLatency: 1, MaxLatency: 1})
	if err := s.Block(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Block(2, 1); err != nil {
		t.Fatal(err)
	}
	(*sender).env.Send(2, "a", nil)
	(*receiver).env.Send(1, "b", nil)
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 0 {
		t.Fatalf("delivered %d messages across a partition", len(*delivered))
	}
	if err := s.Unblock(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Unblock(2, 1); err != nil {
		t.Fatal(err)
	}
	(*sender).env.Send(2, "c", nil)
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 1 {
		t.Fatalf("after heal delivered %d, want 1", len(*delivered))
	}
}

func TestNetworkHeal(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 1, MaxLatency: 1})
	if err := s.Block(1, 2); err != nil {
		t.Fatal(err)
	}
	(*sender).env.Send(2, "lost", nil)
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	s.HealAll()
	(*sender).env.Send(2, "kept", nil)
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 1 || (*delivered)[0].msg.Kind != "kept" {
		t.Fatalf("delivered = %+v, want only kind=kept", *delivered)
	}
}

func TestInFlightMessageDroppedWhenPartitionForms(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 5, MaxLatency: 5})
	(*sender).env.Send(2, "inflight", nil)
	if err := s.ScheduleFunc(2, "cut", func(s *Sim) error {
		return s.Block(1, 2)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 0 {
		t.Fatalf("in-flight message was delivered across a new partition")
	}
}

func TestMessagePayloadIsolation(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 1, MaxLatency: 1})
	payload := []byte("abc")
	(*sender).env.Send(2, "msg", payload)
	copy(payload, "xyz")
	if err := s.RunUntil(5); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 1 {
		t.Fatalf("delivered %d, want 1", len(*delivered))
	}
	if !bytes.Equal((*delivered)[0].msg.Payload, []byte("abc")) {
		t.Fatalf("payload = %q, want abc", (*delivered)[0].msg.Payload)
	}
}

func TestCrashDropsInFlightMessages(t *testing.T) {
	s, sender, _, delivered := networkPair(t, NetworkConfig{MinLatency: 5, MaxLatency: 5})
	(*sender).env.Send(2, "old", nil)
	if err := s.ScheduleFunc(1, "crash", func(s *Sim) error { return s.Crash(2) }); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleFunc(2, "restart", func(s *Sim) error { return s.Restart(2) }); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if len(*delivered) != 0 {
		t.Fatalf("old message leaked into restarted node")
	}
}

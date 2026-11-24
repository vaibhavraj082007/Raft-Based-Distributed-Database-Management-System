package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"raftkv/internal/scenarios"
	"raftkv/internal/sim"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		seed      = flag.Int64("seed", 1, "simulation seed (must be >= 0)")
		scenario  = flag.String("scenario", scenarios.PingScenario, "scenario to run")
		ticks     = flag.Int64("ticks", 1000, "run until this simulated tick")
		trace     = flag.Bool("trace", false, "write the event trace to stdout")
		decisions = flag.Bool("decisions", false, "include RNG and decision records in the trace")
	)
	flag.Parse()

	if *seed < 0 {
		return fmt.Errorf("simctl: seed must be >= 0, got %d", *seed)
	}
	if *ticks < 0 {
		return fmt.Errorf("simctl: ticks must be >= 0, got %d", *ticks)
	}
	if *scenario != scenarios.PingScenario {
		return fmt.Errorf("simctl: unknown scenario %q (available: %s)", *scenario, scenarios.PingScenario)
	}

	var traceOut io.Writer
	if *trace {
		traceOut = os.Stdout
	}
	level := sim.TraceEvents
	if *decisions {
		level = sim.TraceDecisions
	}

	s, err := scenarios.NewPing(sim.Config{
		Seed:       uint64(*seed),
		Scenario:   *scenario,
		Trace:      traceOut,
		TraceLevel: level,
	}, 5, sim.Tick(*ticks))
	if err != nil {
		return fmt.Errorf("simctl: setup: %w", err)
	}
	if err := s.RunUntil(sim.Tick(*ticks)); err != nil {
		return err
	}

	st := s.Stats()
	fmt.Printf("run v=%d seed=%d scenario=%s nodes=%d end-tick=%d status=ok\n",
		sim.Version, s.Seed(), s.Scenario(), len(s.NodeIDs()), *ticks)
	fmt.Printf("steps=%d events=%d sent=%d delivered=%d dropped=%d disk-ops=%d boots=%d crashes=%d restarts=%d\n",
		s.Steps(), st.Events, st.MessagesSent, st.MessagesDelivered, st.MessagesDropped,
		st.DiskOps, st.Boots, st.Crashes, st.Restarts)
	fmt.Printf("trace-hash=%016x state-hash=%016x\n", s.TraceHash(), s.StateHash())
	return nil
}

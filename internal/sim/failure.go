package sim

import (
	"fmt"
	"strings"
)

// Failure is a deterministic, replayable simulation failure.
type Failure struct {
	Seed      uint64
	Scenario  string
	Version   int
	Step      uint64
	Tick      Tick
	Event     string
	Invariant string
	Cause     error
	Replay    string
	TraceTail []string
}

func (f *Failure) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "simulation failed: seed=%d scenario=%s version=%d step=%d tick=%d event=%s\n",
		f.Seed, f.Scenario, f.Version, f.Step, f.Tick, f.Event)
	if f.Invariant != "" {
		fmt.Fprintf(&b, "invariant=%s\n", f.Invariant)
	}
	if f.Cause != nil {
		fmt.Fprintf(&b, "cause: %v\n", f.Cause)
	}
	fmt.Fprintf(&b, "replay: %s\n", f.Replay)
	if len(f.TraceTail) > 0 {
		b.WriteString("last trace:\n")
		for _, line := range f.TraceTail {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func (f *Failure) Unwrap() error { return f.Cause }

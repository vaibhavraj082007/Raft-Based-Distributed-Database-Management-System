package sim

// Invariant is checked after every dispatched simulation event and once at the
// end of every run. Check must not mutate simulation state or consume RNG.
type Invariant interface {
	Name() string
	Check(*Sim) error
}

// InvariantFunc adapts a function to Invariant.
type InvariantFunc struct {
	InvariantName string
	CheckFn       func(*Sim) error
}

func (f InvariantFunc) Name() string { return f.InvariantName }

func (f InvariantFunc) Check(s *Sim) error {
	if f.CheckFn == nil {
		return nil
	}
	return f.CheckFn(s)
}

func NewInvariant(name string, fn func(*Sim) error) Invariant {
	return InvariantFunc{InvariantName: name, CheckFn: fn}
}

// Snapshotter is implemented by nodes that can produce a canonical snapshot
// for the simulator state digest.
type Snapshotter interface {
	Snapshot() []byte
}

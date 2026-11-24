package sim

import "fmt"

// Version is bumped whenever simulation semantics change in a way that alters
// traces or random-number consumption. Traces record it for replay forensics.
const Version = 1

// Rand is a frozen SplitMix64 generator. It is intentionally implemented here
// instead of using math/rand so that replay is independent of the Go release.
type Rand struct {
	state uint64
}

func NewRand(seed uint64) *Rand {
	return &Rand{state: seed}
}

func (r *Rand) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a uniform integer in [0, n). It uses rejection sampling so the
// result is not biased.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic(fmt.Sprintf("sim: Intn requires n > 0, got %d", n))
	}
	return int(r.uint64n(uint64(n)))
}

func (r *Rand) uint64n(n uint64) uint64 {
	max := ^uint64(0)
	limit := max - (max % n)
	for {
		v := r.Uint64()
		if v < limit {
			return v % n
		}
	}
}

func (r *Rand) IntRange(min, max int) int {
	if max < min {
		panic(fmt.Sprintf("sim: IntRange min %d > max %d", min, max))
	}
	return min + r.Intn(max-min+1)
}

func (r *Rand) Int64Range(min, max int64) int64 {
	if max < min {
		panic(fmt.Sprintf("sim: Int64Range min %d > max %d", min, max))
	}
	span := uint64(max - min + 1)
	return min + int64(r.uint64n(span))
}

func (r *Rand) Chance(permille int) bool {
	if permille <= 0 {
		return false
	}
	if permille >= 1000 {
		return true
	}
	return r.Intn(1000) < permille
}

func (r *Rand) stateValue() uint64 { return r.state }

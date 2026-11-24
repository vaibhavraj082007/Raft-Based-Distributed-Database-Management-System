package sim

import "testing"

func TestRNGGoldenVectors(t *testing.T) {
	want := []uint64{
		0xe220a8397b1dcdaf,
		0x6e789e6aa1b965f4,
		0x06c45d188009454f,
		0xf88bb8a8724c81ec,
		0x1b39896a51a8749b,
		0x53cb9f0c747ea2ea,
	}
	r := NewRand(0)
	for i, expected := range want {
		if got := r.Uint64(); got != expected {
			t.Fatalf("draw %d: got %016x, want %016x", i, got, expected)
		}
	}
}

func TestRNGSeedReplay(t *testing.T) {
	a := NewRand(42)
	b := NewRand(42)
	for i := 0; i < 100; i++ {
		if av, bv := a.Uint64(), b.Uint64(); av != bv {
			t.Fatalf("draw %d differs: %d vs %d", i, av, bv)
		}
	}
}

func TestRNGDifferentSeedsDiffer(t *testing.T) {
	a := NewRand(1)
	b := NewRand(2)
	same := 0
	for i := 0; i < 100; i++ {
		if a.Uint64() == b.Uint64() {
			same++
		}
	}
	if same > 1 {
		t.Fatalf("different seeds produced %d equal draws", same)
	}
}

func TestRNGIntRange(t *testing.T) {
	r := NewRand(7)
	for i := 0; i < 10_000; i++ {
		v := r.IntRange(3, 9)
		if v < 3 || v > 9 {
			t.Fatalf("value %d outside [3,9]", v)
		}
	}
	for i := 0; i < 10_000; i++ {
		v := r.Int64Range(-5, 5)
		if v < -5 || v > 5 {
			t.Fatalf("value %d outside [-5,5]", v)
		}
	}
}

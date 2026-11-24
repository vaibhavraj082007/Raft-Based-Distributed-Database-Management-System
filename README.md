# raftkv — deterministic Raft KV lab

A 5-node replicated key-value database in Go with a from-scratch Raft
implementation, running entirely inside a deterministic simulation tester in
the style of FoundationDB and TigerBeetle.

The simulator owns the clock, network, disks, node lifecycle, and randomness.
There is exactly one seeded random number generator per simulation. If a bug
appears on seed `N`, the same seed must replay the same execution and the same
failure.

## Status

**Stage 0 — scaffolding and Stage 1 plan. No implementation code yet.**

Stage 1 is specified in [PLAN.md](PLAN.md) and is waiting for approval.
The Go toolchain is not yet installed in the development environment.

## Non-negotiable rules

1. Go only; standard library except Porcupine for linearizability checks.
2. No `time.Now`, real timers, real goroutines, channels, or real networking
   inside simulated code. Time, scheduling, I/O, and randomness come from the
   simulator.
3. Exactly one seeded PRNG per simulation. All draws go through it in a
   deterministic order.
4. Every simulation run records its seed, scenario, and config so a failure can
   be replayed with one command.
5. Invariants are checked after every simulation step, not only at the end.
6. Tests are written for each stage before moving to the next stage.
7. Committed state is persisted before any RPC reply that depends on it.
8. Same seed + same binary + same config = byte-identical trace and final state.

## Target architecture

| Stage | Layer | Package | Status |
|------:|-------|---------|--------|
| 1 | Deterministic simulator (clock, network, disk, RNG, trace, invariants) | `internal/sim` | Planned |
| 2 | Raft (election and replication, Raft paper Figure 2) | `internal/raft` | Not started |
| 3 | Storage engine (WAL, then LSM-tree) | `internal/storage` | Not started |
| 4 | Linearizable KV API with client request ID deduplication | `internal/kv` | Not started |
| 5 | Sharding, multi-shard transactions | TBD | Not started |
| 6 | Tiny SQL layer, React + WebSocket dashboard | TBD | Not started |

Later layers may import `internal/sim`; `internal/sim` must never import them.
Simulation code is single-threaded and event-driven.

## Repository layout

```text
.
├── PLAN.md                 # Stage 1 specification (current artifact)
├── README.md
├── Makefile
├── go.mod
├── cmd/
│   └── simctl/             # Stage 1 CLI: run a scenario by seed, print trace
├── internal/
│   ├── sim/                # clock, event queue, RNG, network, disk, trace
│   ├── raft/               # later
│   ├── storage/            # later
│   ├── kv/                 # later
│   └── scenarios/          # deterministic scenarios and chaos schedules
└── testdata/               # golden traces and state digests
```

## Determinism rules for contributors

- Use only `internal/sim`'s `Rand` via the simulation environment. Never import
  `math/rand`, `math/rand/v2`, or `crypto/rand` in simulated packages.
- Use logical `sim.Tick` values. No wall-clock APIs, timers, or sleeps.
- No `go` statements, channels, or `select` in simulated packages. Nodes are
  actors driven by events from the single simulator loop.
- Never iterate a Go map when order can affect behavior, RNG consumption, or
  output. Sort keys first.
- Never put a map, pointer, goroutine ID, or wall-clock timestamp into a trace.
  Format trace fields explicitly.
- All node-visible state must be reachable from the event loop. No background
  work may mutate state.
- If behavior changes in a way that changes RNG draws or event ordering, bump
  the simulation version recorded in traces and note it in the bug log. Old
  seeds replay exactly on the same version; reproducing across versions means
  checking out the recorded commit.

A static test enforces the API bans above for simulation packages.

## Running

Requires Go 1.24 or newer.

```sh
make test          # go test ./...
make test-race     # go test -race ./...
make vet           # go vet ./...
make sim           # go run ./cmd/simctl -seed 1 -scenario ping
```

Planned replay command for a failing seed:

```sh
go run ./cmd/simctl -seed <N> -scenario <name> -trace
```

## Bug log

Every bug found by the simulator must be recorded here before its fix is
accepted. The seed, scenario, and regression test are mandatory.

| ID | Stage | Seed | Symptom | Root cause | Fix | Regression test |
|----|------:|-----:|---------|------------|-----|-----------------|
| —  | —     | —    | No bugs logged yet | — | — | — |

## Stage gates

A stage is complete only when:

1. Its plan is approved.
2. Implementation and tests are complete.
3. `go test ./...` and `go vet ./...` pass.
4. Test output has been shown and the user has approved moving on.

No code for a later stage is written early.

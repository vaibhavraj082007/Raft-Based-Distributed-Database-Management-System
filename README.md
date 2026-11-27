# raftkv — deterministic Raft KV lab

A 5-node replicated key-value database in Go with a from-scratch Raft
implementation, running entirely inside a deterministic simulation tester in
the style of FoundationDB and TigerBeetle.

The simulator owns the clock, network, disks, node lifecycle, and randomness.
There is exactly one seeded random number generator per simulation. If a bug
appears on seed `N`, the same seed must replay the same execution and the same
failure.

## Status

**Stage 1 — deterministic simulator — Complete (approved 2026-11-25, commit `0c7d5fb`).**

**Stage 2 — Raft — 2A approved; 2B approved; 2C implemented, awaiting gate approval.**

The Stage 1 simulator, chaos scenario, CLI, golden trace, and full test suite are
approved and pushed. Stage 1 is specified in [PLAN.md](PLAN.md); Stage 2 is
specified in [PLAN_STAGE2.md](PLAN_STAGE2.md).

Sub-phase 2A implements actor-based leader election, empty `AppendEntries`
heartbeats, crash-safe `currentTerm`/`votedFor` persistence, and the per-step
Election Safety invariant in `internal/raft`. Sub-phase 2B adds the leader
no-op, durable `raft.log` append/truncate batches, `AppendEntries` replication
with conflict backtracking, majority commit with the Figure 8 current-term
rule, in-order exactly-once apply, restart replay, and leader/follower client
request handling with exported client codecs. Sub-phase 2C adds the 5-node
Troublemaker scenario, a non-voting client workload, crashes/restarts,
drops/delays/partitions, the Log Matching, State Machine Safety, and Commit
Durability invariants, the `raft` CLI target, and the reviewed golden trace.

Verified commands:

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/raft -count=5
go test ./internal/scenarios -run TestSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestRaftSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestRaftMultiSeed -count=1
make stress-raft SEEDS=10000
go run ./cmd/simctl -seed 42 -scenario ping -ticks 500 -trace
go run ./cmd/simctl -seed 42 -scenario raft -ticks 400 -trace
```

The Stage 2 verification matrix and gate commands are in
[PLAN_STAGE2.md](PLAN_STAGE2.md).

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
| 1 | Deterministic simulator (clock, network, disk, RNG, trace, invariants) | `internal/sim` | Complete (approved) |
| 2 | Raft (election and replication, Raft paper Figure 2) | `internal/raft` | 2A and 2B approved; 2C implemented (awaiting gate) |
| 3 | Storage engine (WAL, then LSM-tree) | `internal/storage` | Not started |
| 4 | Linearizable KV API with client request ID deduplication | `internal/kv` | Not started |
| 5 | Sharding, multi-shard transactions | TBD | Not started |
| 6 | Tiny SQL layer, React + WebSocket dashboard | TBD | Not started |

Later layers may import `internal/sim`; `internal/sim` must never import them.
Simulation code is single-threaded and event-driven.

## Repository layout

```text
.
├── PLAN.md                 # Stage 1 specification
├── PLAN_STAGE2.md          # Stage 2 Raft specification
├── README.md
├── Makefile
├── go.mod
├── cmd/
│   └── simctl/             # run a scenario by seed, print trace/hashes
├── internal/
│   ├── sim/                # event queue, RNG, network, disk, trace, invariants
│   ├── raft/               # Stage 2 Raft (2A election, 2B replication, 2C invariants)
│   ├── storage/            # later
│   ├── kv/                 # later
│   └── scenarios/          # ping workload + raft Troublemaker scenario
├── testdata/
│   ├── stage1/
│   │   └── ping-seed-42.trace
│   └── stage2/
│       └── raft-seed-42.trace
```

Stage 1 includes a frozen SplitMix64 PRNG, stable `(tick, sequence)` event
ordering, network drops/delays/partitions, an asynchronous disk where only
`Sync` survives crashes, epoch-filtered messages/timers/I/O, crash-safe fresh
node factories, canonical traces and state hashes, per-step invariants, a
100-seed smoke test, and a static API guard against non-deterministic APIs.

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
make sim           # go run ./cmd/simctl -seed 1 -scenario ping -trace
make sim-raft      # go run ./cmd/simctl -seed 1 -scenario raft -ticks 400 -trace
make stress-raft SEEDS=10000   # explicit 10,000-seed Raft stress target

# Replay a failing seed (the failure report prints the exact command):
go run ./cmd/simctl -seed <N> -scenario ping -ticks <T> -trace
go run ./cmd/simctl -seed <N> -scenario raft -ticks <T> -trace
```

Regenerate the reviewed golden traces after an intentional behavior change:

```sh
go test ./internal/scenarios -run TestGoldenTraceSeed42 -update
go test ./internal/scenarios -run TestRaftGoldenTraceSeed42 -update
```

## Stage 2 plan (2A and 2B approved, 2C implemented)

Stage 2 is split into three independently testable gates, specified in
[PLAN_STAGE2.md](PLAN_STAGE2.md):

- **2A — Election and durable vote state (approved):** roles, randomized
  timeouts, `RequestVote`, empty heartbeats, crash-safe
  `currentTerm`/`votedFor`, and the Election Safety invariant.
- **2B — Replication and commit safety (approved):** leader no-op and log
  persistence, `AppendEntries` replication, conflict backtracking, durable
  append/truncate, commit/apply, client request handling, and the Figure 8
  current-term commit rule.
- **2C — Troublemaker (implemented, awaiting gate):** the 5-node `raft`
  scenario, client actor and workload, crashes/restarts,
  drops/delays/partitions, the Log Matching, State Machine Safety, and Commit
  Durability invariants, golden trace, and the 10,000-seed stress target.

2C is implemented and verified locally; Stage 3 work begins only after the 2C
review gate is approved.

## Bug log

Every bug found by the simulator must be recorded here before its fix is
accepted. The seed, scenario, and regression test are mandatory. Stage 2 bugs
are appended as `S2-00N` with the same fields; the Stage 1 entry below remains
as the first record.

| ID | Stage | Scenario | Seed | Symptom | Root cause | Fix | Regression test |
|----|------:|----------|-----:|---------|------------|-----|-----------------|
| S1-001 | 1 | ping | 5 (forced restarts) | Recovery invariant would falsely report that a node recovered more records than it synced after two or more restarts | The ledger tracked only the current incarnation's synced count, while the journal is cumulative across incarnations | Track cumulative synced records across all incarnations, keyed by an incarnation counter | `TestPingInvariantAcrossMultipleRestarts` |
| S2-001 | 2 | raft | 42 | Commit Durability falsely rejected a stale term-1 leader that was partitioned from the term-2 majority (`leader node 1 ... has last log index 1 below committed index 2`, observed at tick 147 during 2C development) | The invariant required every current leader to contain every observed committed entry, but Leader Completeness only requires entries committed at or before the leader's own term; an old leader may legally be unaware of a newer majority's commits | Record the first observed commit term for each index and qualify the leader check to entries committed at or before the leader's term; keep the unqualified committed-loss check on restart | `TestRaftCommitDurabilityAllowsStaleLeader` |

## Stage gates

A stage is complete only when:

1. Its plan is approved.
2. Implementation and tests are complete.
3. `go test ./...` and `go vet ./...` pass.
4. Test output has been shown and the user has approved moving on.

For Stage 2, each of 2A, 2B, and 2C has its own gate and verification commands
in [PLAN_STAGE2.md](PLAN_STAGE2.md). The full 10,000-seed stress run is required
before Stage 2 is presented for final approval.

No code for a later stage is written early.

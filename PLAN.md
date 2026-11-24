# Stage 1 — Deterministic Simulator

**Status:** Implemented, awaiting approval
**Date:** 2026-11-24
**Scope:** Build the deterministic substrate only. No Raft, storage engine, KV,
sharding, SQL, or dashboard code in this stage.

## 1. Goal

Build a single-threaded discrete-event simulator that every later layer runs
inside:

- A logical clock that advances only when the simulator processes an event.
- A fake network with configurable latency, drops, and partitions.
- A fake asynchronous disk per node with real crash durability semantics.
- Exactly one seeded PRNG per simulation.
- A canonical event trace and trace hash for replay verification.
- An invariant hook that runs after every simulation step.
- A deterministic chaos scenario that exercises clocks, network, disks, node
  crashes, restarts, and partitions.
- Tests proving that the same seed produces identical output, including across
  independent simulator instances and separate test processes.

Stage 1 is successful when the above is demonstrated, not when Raft works.

### Definition of done

1. `go test ./...` and `go vet ./...` pass.
2. `TestSameSeedProducesIdenticalTrace` runs the chaos scenario multiple times
   from a fresh simulator and compares full traces and state hashes.
3. `TestInterleavedSimsDoNotShareState` proves two simulators can be advanced
   alternately without affecting each other.
4. A golden trace test replays a fixed seed in a separate process and byte
   compares the trace.
5. The chaos scenario completes 100 seeds without a structural invariant
   violation, in under 10 seconds.
6. The static API guard rejects real time, goroutines, channels, real
   networking, and global randomness in simulation packages.
7. No code for Stage 2 or later exists.

## 2. Design principles

1. **One event loop.** Every simulated action is an event processed by `Sim`.
   There are no goroutines, channels, or background mutation.
2. **One source of truth for time.** `Sim.Now()` is a logical `Tick`. Nothing
   else can obtain time.
3. **One RNG.** `Sim` owns one instance of an in-repo, frozen PRNG. Network,
   disk, scenarios, and nodes draw only through it.
4. **Deterministic ordering.** Events are ordered by `(at, sequence)`, where
   `sequence` is assigned at scheduling time. Never by pointer, map order, or
   scheduler behavior.
5. **Deterministic output.** Traces are canonical text with explicit fields.
   No map printing, pointer printing, floats, or wall-clock timestamps.
6. **Reproducibility over realism.** Crash and partition semantics are simple
   and explicitly documented. Complexity is added only when a later layer
   needs it.
7. **Failures are artifacts.** An invariant violation reports seed, scenario,
   config, step, simulated time, the triggering event, and a replay command.

## 3. Package layout

```text
internal/sim/
  sim.go          # Config, Sim, run loop, node registry, lifecycle
  event.go        # Event types, stable priority queue
  rand.go         # frozen SplitMix64 implementation + helpers
  env.go          # per-node capability interface
  network.go      # async message delivery, drops, partitions
  disk.go         # per-node asynchronous crash-safe file device
  trace.go        # canonical trace writer, hashing, state digest
  invariant.go    # invariant interface and runner
  version.go      # simulation semantic version
  *_test.go

internal/scenarios/
  ping.go         # Stage 1 toy scenario + chaos driver
  ping_test.go

cmd/simctl/
  main.go         # flags, run, trace output, failure report

testdata/stage1/
  ping-seed-42.trace
```

`internal/sim` has no project-internal dependencies. Scenarios and the CLI depend
on it. Later layers will depend on it in one direction only.

## 4. Core model

### 4.1 Logical time

- `type Tick int64`, measured in **simulated milliseconds** since start.
- `Now()` is the timestamp of the event currently being dispatched.
- Time jumps directly to the next scheduled event. It never advances on its own.
- `RunUntil(t)` processes all events with `at <= t`, then sets now to `t` if the
  queue is empty earlier. `RunSteps(n)` and `RunUntilIdle()` are also provided.
- Scheduling an event in the past is a programmer error and returns/panics with
  a deterministic diagnostic.
- Tick overflow is checked. A run that reaches the maximum time or step count
  fails with a replayable error instead of hanging.
- Default network latency: 1–4 ticks. Default disk latency: 1 tick. Default Raft
  tick/heartbeat and election timeouts are chosen in Stage 2; nothing in Stage 1
  hard-codes wall-clock units.

### 4.2 Event loop and ordering

Each scheduled event carries an absolute `at` tick and a monotonically
increasing `seq`. The priority queue orders by `(at, seq)`, so:

- Events at the same tick execute in scheduling order.
- An event scheduled during dispatch at the same tick runs after the current
  event.
- Tie-breaking is stable across runs and machines.

A simulation **step** is one pop-and-dispatch. After dispatch, the simulator:

1. checks structural invariants,
2. runs every registered invariant in registration order,
3. appends canonical trace records,
4. updates the rolling trace hash.

Scenario actions (crash, restart, partition, heal, drop-rate change) are
themselves scheduled events, so they interleave deterministically with node work
and RNG consumption.

### 4.3 Randomness

Use a small in-repo **SplitMix64** PRNG rather than `math/rand`:

- Frozen by golden-vector tests, so replay does not depend on a Go release.
- One instance per `Sim`, created from the run seed.
- Only exposed through `Sim` and per-node `Env` methods:
  `Uint64()`, `Intn(n)`, `IntRange(min, maxInclusive)`, `Chance(permille)`.
- `Intn` uses unbiased rejection sampling.
- Randomness is consumed only inside event dispatch, except simulation setup
  explicitly documented as deterministic. Node constructors must not draw.
- One stream means adding a new draw changes subsequent behavior. That is
  intentional. The simulation version is recorded in every trace; exact replay
  is guaranteed for the same binary/config/seed. Cross-version replay requires
  the recorded commit.

CLI seed is an `int64`; negative values are rejected before converting to
`uint64`, so there is one canonical representation per seed.

### 4.4 Fake network

`Send(from, to, kind, payload)` is asynchronous and returns immediately.

On send:

1. Validate that both nodes are registered.
2. Drop if the sender or receiver is down, the directed link is blocked, or the
   configured per-mille drop chance fires.
3. Sample latency uniformly from `[min, max]` inclusive.
4. Clone the payload bytes.
5. Schedule a delivery event tagged with the destination's boot epoch.

On delivery:

1. Drop if the destination's epoch changed (node crashed/restarted), the node is
   down, or the directed link is now blocked.
2. Deliver to the node's `HandleMessage`.

Consequences:

- Messages in flight when a partition forms are dropped.
- Messages sent before a crash can never leak into the restarted node.
- Payload mutation by the sender after `Send` cannot affect the receiver.
- Variable latency produces reordering without needing an explicit reorder knob.

Partition controls:

- `Block(from, to)` and `Unblock(from, to)` for directed links.
- `Partition(groups ...[]NodeID)` blocks every cross-group directed link.
- `Isolate(node)` blocks every link into and out of the node.
- `HealAll()` clears all blocked links.

Blocked links are a set; the controls compose. Duplicated messages and
corruption are deliberately out of scope for Stage 1.

### 4.5 Fake disk

One disk per node. Each file has two byte views:

- **visible** — what reads see; includes completed but unsynced appends,
- **durable** — survives crash; advanced only by a completed `Sync`.

Operations are asynchronous and processed serially per node in FIFO order, each
taking the configured disk latency:

```go
type Device interface {
    Read(tag uint64, name string, off int64, n int64)
    Append(tag uint64, name string, p []byte)
    Replace(tag uint64, name string, p []byte)
    Sync(tag uint64)
    List(tag uint64)
}
```

Completions arrive as `IOEvent{Tag, Data, Err}` through `HandleIO`, as simulator
events. A node must not reply to an RPC that depends on persisted state until
the corresponding `Sync` completion has arrived.

Crash semantics:

- All pending operations are canceled and their completion events are discarded.
- Visible data reverts to durable data.
- Durable data remains for restart.
- Old-epoch completion events are ignored even if they were already queued.
- No torn writes or bit rot in Stage 1; each completed append is atomic.

Read-your-writes holds before sync. Unsynced data is lost on crash. Synced data
survives restart. A `Sync` atomically makes all preceding appends durable. `List`
and file sizes are supported so a WAL can recover its files.

### 4.6 Nodes and lifecycle

```go
type Node interface {
    HandleMessage(Message)
    HandleTimer(TimerEvent)
    HandleIO(IOEvent)
}

type NodeFactory func(Env) Node
```

`Env` exposes the node ID, `Now`, `Send`, `After(delay, tag)`, RNG helpers, and
its `Device`. The simulator owns the `Env`; nodes cannot spoof `From` or reach
outside the simulation.

Lifecycle rules:

- A node is added with a factory and an initial boot event at tick 0.
- Crash: mark down, increment the node's boot epoch, cancel timers, cancel disk
  operations, discard volatile disk data. The node object is abandoned.
- Restart: call the factory again to construct a **fresh** node with a new
  `Env`; only durable disk state carries over.
- Messages, timers, and I/O completions tagged with an old epoch are ignored.
- Node code receives no `OnCrash` callback. It cannot rely on in-memory state
  surviving a crash.
- Panics in node code are caught at the dispatch boundary and reported as
  deterministic simulation failures with seed and replay information.

Stage 2 may add a boot delay if Raft needs one; Stage 1 marks a restarted node
up immediately after construction.

### 4.7 Trace and replay

The trace is canonical ASCII/UTF-8 text, one record per line, for example:

```text
sim v=1 seed=42 scenario=ping nodes=5
t=0 step=1 boot node=1
t=0 step=2 rng kind=network.delay value=3
t=1 step=3 send from=1 to=3 kind=ping seq=1 bytes=8
...
t=120 step=57 crash node=4
...
t=400 step=210 summary events=209 messages=180 dropped=23 invariants=ok
trace-hash=8f14e45fceea167a
state-hash=9e107d9d372bb682
```

Rules:

- Time is simulated ticks. Step is the dispatch count.
- Records name every decision that can affect behavior, including drop reasons,
  partition changes, crashes, restarts, boot, and I/O completions. RNG draw
  labels are included at `-trace=decisions` verbosity for debugging.
- Payloads are summarized by kind and length; raw bytes are optional hex at
  verbose level.
- Nodes may provide a canonical `Snapshot() []byte`; the simulator hashes those
  plus its own state (node liveness/epochs, sorted blocked links, sorted file
  contents and sync offsets). Hash: FNV-1a 64.
- The rolling trace hash is computed over the exact bytes written. Separate
  processes must produce the same bytes and hash.
- On failure, output includes: seed, scenario, simulation version, config, step,
  time, triggering event, failed invariant, last N trace lines, stack trace for
  panics, and the exact replay command.
- Full traces are written to an injected writer. Tests use an in-memory buffer;
  long CLI runs may use a bounded ring buffer plus the hash, but the replay mode
  always writes the full trace.
- No maps, pointers, float formatting, environment variables, or wall-clock
  timestamps may influence output.

### 4.8 Invariants

```go
type Invariant interface {
    Name() string
    Check(*Sim) error
}
```

- Registered in deterministic order and checked after every dispatched event
  and once at the end of a run.
- Stateful checkers are allowed (needed later for "committed entries never
  disappear"); they may keep internal history but may not mutate simulation
  state.
- Structural checks are always enabled: non-decreasing time, no past events, no
  delivery to a crashed epoch, queue integrity, step/time limits.
- Stage 1 tests use toy invariants to prove the framework runs exactly once per
  step and reports replays correctly. Raft and bank invariants arrive in their
  stages:
  - one leader per term,
  - no two nodes disagree on a committed entry,
  - committed entries never disappear,
  - bank total = 100,000 at all times.

### 4.9 API sketch

These signatures are directional; small adjustments are allowed during
implementation as long as the semantics above hold.

```go
type Config struct {
    Seed        uint64
    Scenario    string
    Nodes       []NodeID
    Network     NetworkConfig
    Disk        DiskConfig
    MaxSteps    uint64
    MaxTick     Tick
    Trace       io.Writer
    TraceLevel  TraceLevel
}

func New(cfg Config) (*Sim, error)
func (s *Sim) AddNode(id NodeID, f NodeFactory) error
func (s *Sim) Start() error
func (s *Sim) RunUntil(t Tick) error
func (s *Sim) RunSteps(n uint64) error
func (s *Sim) RunUntilIdle() error
func (s *Sim) RegisterInvariant(inv Invariant)
func (s *Sim) Crash(id NodeID) error
func (s *Sim) Restart(id NodeID) error
func (s *Sim) TraceHash() uint64
func (s *Sim) StateHash() uint64
```

`Crash`, `Restart`, `Block`, and similar controls called from test code are
immediate only during setup; inside a run they must be invoked from a scenario
event so behavior remains event-ordered.

## 5. Determinism contract

A run is reproducible iff all of the following hold:

1. Same simulation version, seed, scenario name, config, and binary.
2. All random draws come from the single `Sim` PRNG in event order.
3. All ordering is by `(tick, sequence)` or explicitly sorted keys.
4. No wall-clock, OS scheduling, environment, filesystem, or real network
   influences simulated behavior.
5. All trace and state encodings are canonical.
6. Node constructors do not draw randomness; all randomness is consumed during
   dispatch.
7. Invariant checks do not mutate state or draw randomness.

The test suite verifies 1–3 and 5 directly. The static guard verifies 4 and the
absence of goroutines/channels. Code review and trace-diff tests cover map
iteration and formatting hazards.

## 6. Stage 1 scenario: `ping`

A deliberately simple but end-to-end scenario, not a Raft placeholder:

- 5 nodes, each with a toy `PingNode` containing a monotonic sequence counter.
- Every 10–20 simulated ms, a node sends `PING{seq}` to a random peer.
- A receiver replies `PONG{seq}` to the sender.
- Each node appends a small record to an on-disk journal and periodically syncs.
- On restart, the node reads its journal and reports how many durable records it
  recovered.
- A chaos driver schedules actions every 50–150 ms:
  - crash a live node,
  - restart a crashed node,
  - create a partition,
  - heal all links,
  - change drop rate,
  - change latency range.
- Scenario invariants check structural simulator validity and that recovered
  durable record counts never exceed what was actually synced.
- At the end, the scenario heals the network and restarts all nodes so a final
  summary is meaningful. Liveness is not required at every intermediate point.

This scenario exercises every Stage 1 subsystem and the RNG consumption order
without implementing any Stage 2 logic.

## 7. Test plan

### Clock and event queue

| Test | Assertion |
|------|-----------|
| `TestRunUntilAdvancesIdleClock` | time advances to target with an empty queue |
| `TestSameTickOrdering` | same-tick events run in scheduling order |
| `TestSchedulePastRejected` | past scheduling fails deterministically |
| `TestTimersFireOnce` | canceled/fired timers do not fire twice |
| `TestMaxStepsGuard` | a non-terminating scenario fails with a replayable error |

### RNG

| Test | Assertion |
|------|-----------|
| `TestRNGGoldenVectors` | fixed seed produces frozen known outputs |
| `TestRNGSeedReplay` | two instances with the same seed produce identical streams |
| `TestRNGIntRange` | bounds hold over many draws |
| `TestRNGRejectsNegativeSeedCLI` | invalid seed is rejected without conversion ambiguity |

### Network

| Test | Assertion |
|------|-----------|
| `TestNetworkLatencyWithinBounds` | every delivery latency is in `[min, max]` |
| `TestNetworkDropRate` | observed drops over a large seeded sample match tolerance |
| `TestNetworkPartitionBothDirections` | no delivery across a partition |
| `TestNetworkHeal` | delivery resumes after healing |
| `TestInFlightMessageDroppedWhenPartitionForms` | delivery re-check drops in-flight messages |
| `TestMessagePayloadIsolation` | sender mutation does not alter delivered bytes |
| `TestCrashDropsInFlightMessages` | epoch change prevents delivery after restart |

### Disk

| Test | Assertion |
|------|-----------|
| `TestDiskReadYourWritesBeforeSync` | reads see completed unsynced appends |
| `TestDiskUnsyncedDataLostOnCrash` | visible data reverts to last durable state |
| `TestDiskSyncedDataSurvivesRestart` | synced bytes remain after crash/restart |
| `TestDiskPendingOpsCanceledOnCrash` | no completion for in-flight ops across crash |
| `TestDiskFIFO` | completions occur in issue order |
| `TestDiskSyncCoversPriorAppends` | one sync persists all preceding writes |
| `TestDiskListAndRecovery` | file list and contents support WAL-style recovery |

### Lifecycle and invariants

| Test | Assertion |
|------|-----------|
| `TestFreshNodeFactoryOnRestart` | restart constructs a new node, not the old object |
| `TestTimerAndIOEpochFiltering` | stale timers and IO results are ignored |
| `TestInvariantRunsEveryStep` | check count equals dispatched events + final check |
| `TestInvariantFailureReport` | error contains seed, step, event, and replay command |
| `TestPanicIsCaptured` | node panic becomes a replayable failure, not a crash |
| `TestCrashDuringDiskIO` | durable state is consistent at every restart |

### Determinism

| Test | Assertion |
|------|-----------|
| `TestSameSeedProducesIdenticalTrace` | 3 fresh runs: identical trace bytes, trace hash, state hash |
| `TestGoldenTraceSeed42` | fixed-seed trace matches checked-in golden file byte for byte |
| `TestInterleavedSimsDoNotShareState` | alternating steps of two sims changes neither trace |
| `TestSameSeedAcrossProcesses` | `simctl` run twice in subprocesses produces identical output |
| `Test100SeedSmoke` | 100 seeds complete with invariants intact, under 10 s |
| `TestDifferentSeedsDiffer` | known seeds produce different trace hashes |
| `TestNoForbiddenAPIs` | AST scan of simulation packages enforces the bans below |

The golden test uses an explicit `-update` test flag or checked-in regeneration
command; reviewers can inspect the small trace in `testdata/stage1/`.

## 8. Static API guard

`TestNoForbiddenAPIs` parses Go source with `go/parser` and fails on:

- `go` statements, `select` statements, channel types, and send/receive
  operations in `internal/sim`, `internal/raft`, `internal/storage`,
  `internal/kv`, and `internal/scenarios`;
- imports or calls of `time.Now`, `time.Sleep`, `time.After`, `time.Tick`,
  timer/ticker constructors, and `time.AfterFunc`;
- imports of `math/rand`, `math/rand/v2`, `crypto/rand`, and `net`;
- filesystem or network access through `os`, `net`, or `syscall` in simulated
  code (test-only setup may use `os` for golden files, but not during a run);
- environment reads (`os.Getenv`) in simulated code.

The CLI package is excluded from the wall-clock ban only for process-level
concerns, never for simulation time.

## 9. Acceptance checklist

- [ ] Package layout and `go.mod` build with Go 1.24+.
- [ ] All table-driven tests above pass.
- [ ] Same-seed determinism test passes with `-count=5`.
- [ ] Interleaved-simulator isolation test passes.
- [ ] Golden trace is byte-identical across separate processes.
- [ ] 100-seed smoke test passes within 10 seconds.
- [ ] Static API guard passes.
- [ ] Failure reports include seed, scenario, version, step, event, and repro.
- [ ] README bug log updated (including "no bugs yet" if none).
- [ ] No Stage 2+ implementation files.

## 10. Implementation order within Stage 1

1. Toolchain check, module, directory structure.
2. RNG + event queue + clock + structural invariants; tests.
3. Trace writer, hashing, failure reports; tests.
4. Invariant registry and panic capture; tests.
5. Fake network; tests.
6. Fake disk; tests.
7. Node registry, epochs, crash/restart, factory; tests.
8. `ping` scenario and chaos driver.
9. Determinism, interleaving, golden, and 100-seed tests.
10. Static guard, CLI, README bug-log entry, full verification.
11. Stop, present results, wait for approval.

## 11. Out of scope for Stage 1

- Raft election, terms, logs, AppendEntries, snapshots, membership changes.
- WAL or LSM-tree implementation (Stage 1 only provides the fake raw device).
- KV commands, client IDs, deduplication, linearizability checking.
- Sharding, transactions, SQL, dashboard.
- Torn writes, bit rot, fsync corruption, message duplication/corruption.
- Multi-process or real-time execution.
- Performance optimization beyond the 100-seed smoke budget.

## 12. Decisions made (please flag disagreement)

1. **In-repo SplitMix64** instead of `math/rand` for replay stability across Go
   versions; one stream, golden-tested.
2. **`Tick` = milliseconds**; logical time is `int64`.
3. **Asynchronous fake disk with durability only on `Sync`**, serial FIFO
   operations, no torn writes in Stage 1. This is what forces Raft to persist
   before replying.
4. **Messages are typed kind + opaque copied bytes**; Raft will encode/decode its
   own payloads later.
5. **Restart creates a fresh node from a factory**; only durable disk survives.
6. **Chaos is event-driven and uses the same single RNG** as everything else.
7. **Module path `raftkv`, Go 1.24+**, standard library only for now.
8. **Stage 1 scenario is a toy ping/journal workload**, not a Raft stub.

## 13. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Hidden map iteration changes behavior | sorted-key helpers; trace-diff tests; code review checklist |
| One RNG stream couples subsystems | accepted; version traces and golden vectors; document replay scope |
| Async disk completions race crash handling | epoch-tagged events; explicit tests for every crash/IO interleaving |
| Flaky determinism tests due to wall clock | no wall-clock APIs; AST guard; simulated time only |
| Trace growth for long runs | injected writer, hash always computed, bounded CLI buffer, full trace in replay |
| Golden files become stale | explicit update command; review diffs; golden trace intentionally tiny |
| Intentional semantic changes break old seeds | simulation version + commit recorded in failure report |
| Invariant checkers accidentally mutate state | read-only API and review; test invariant determinism |

## 14. Hooks needed by later stages

- Node actor interface can carry Raft messages, timers, and disk completions.
- Raw device supports append, replace, read, sync, and list for WAL/LSM.
- Invariant interface can retain commit history and inspect per-node state.
- Trace includes seed, version, scenario, and config for one-command replay.
- State snapshot interface supports hashing Raft and KV state later.
- Controlled RNG prevents nodes from creating independent random streams.

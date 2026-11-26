# Stage 2 — Raft: 5-Node Deterministic Cluster

**Status:** Approved 2026-11-25; 2A implemented 2026-11-26, 2B–2C pending
**Date:** 2026-11-25
**Scope:** A from-scratch, actor-based Raft implementation in `internal/raft`,
a deterministic `raft` scenario named **Troublemaker**, per-step safety
invariants, golden replay, and targeted Figure 8 and crash-durability tests.
**Depends on:** Stage 1 complete and approved (commit `0c7d5fb`).
**Approval gate:** Approved. Implementation proceeds one sub-phase at a time;
2B and 2C remain gated on acceptance of the preceding sub-phase.

**Implementation status:**

- **2A:** Implemented in `internal/raft`; awaiting gate approval. Includes
  empty `AppendEntries` heartbeats and the `raft.election-safety` invariant.
- **2B:** Not started.
- **2C:** Not started.

---

## 1. Starting point and non-negotiable constraints

Stage 2 builds directly on the approved Stage 1 simulator. The simulator owns
the clock, event queue, network, disks, RNG, trace, hashes, lifecycle, and
invariant checks. Raft is a passive actor driven by simulator callbacks.

The implementation must satisfy all of the following:

1. Raft imports only `internal/sim` (plus the Go standard library). It must
   never import `internal/scenarios`, `internal/storage`, `internal/kv`, or the
   CLI.
2. `internal/sim` must never import `internal/raft`. The existing AST guard in
   `internal/sim/static_test.go` already scans `internal/raft`; it must keep
   passing.
3. No goroutines, channels, `select`, wall clocks, real timers, real
   networking, external RNG, filesystem access, or environment reads in
   simulated code.
4. All randomness comes from `sim.Env` (`Uint64`, `Intn`, `IntRange`,
   `Int64Range`, `Chance`) in event order. Node constructors do not draw.
5. No map iteration may influence behavior, RNG consumption, trace output, or
   state hashes. Peer lists, vote arrays, `nextIndex`, and `matchIndex` are
   slices ordered by peer ID. Any map that must be traversed is keyed and
   sorted first.
6. Every RPC reply that depends on durable state is sent only after the
   corresponding `Sync` completion. A crash before that completion means no
   reply and a restart from the previous durable state.
7. Committed entries are durable on a majority before commit advances, and are
   durable on the leader before it applies or replies that an entry is
   committed.
8. Same seed + same binary + same config = byte-identical trace and final
   state. Stage 2 does not change Stage 1 semantics; `sim.Version` remains `1`.
   Raft wire compatibility is versioned separately in every payload.
9. Stage 2 adds no snapshots, membership changes, log compaction, KV
   semantics, or linearizable client sessions. Those belong to later stages.

### 1.1 Existing simulator surface used by Raft

```go
type Node interface {
	Start()
	HandleMessage(Message)
	HandleTimer(TimerEvent)
	HandleIO(IOEvent)
}

type NodeFactory func(Env) Node

type Env interface {
	ID() NodeID
	Now() Tick
	Send(to NodeID, kind string, payload []byte)
	After(delay Tick, tag uint64) TimerID
	CancelTimer(id TimerID)
	Uint64() uint64
	Intn(n int) int
	IntRange(min, max int) int
	Int64Range(min, max int64) int64
	Chance(permille int) bool
	Read(tag uint64, name string, off, n int64)
	Append(tag uint64, name string, data []byte)
	Replace(tag uint64, name string, data []byte)
	Sync(tag uint64)
	List(tag uint64)
}
```

`Send` is asynchronous. Delivery re-checks the destination epoch, liveness,
partition state, and drop rate. Disk operations are serialized FIFO per node;
`Sync` promotes every visible file to durable state; a crash reverts visible
bytes to durable bytes and discards old-epoch completions. A restart constructs
a fresh `Node` from the factory, so only disk state survives.

### 1.2 Deliberate Stage 2 non-goals

- No `AppendEntries` batching across multiple clients in one log entry.
- No snapshot install, log compaction, or membership changes.
- No read-index, leases, or linearizable reads.
- No client request deduplication; Stage 4 owns that.
- No real-time or multi-process execution.
- No changes to `internal/sim`. If implementation discovers a missing simulator
  primitive, work stops and a Stage 1 amendment is proposed for approval rather
  than silently changing the substrate.

---

## 2. Sub-phases and approval gates

Stage 2 is split into three independently testable sub-phases. Each sub-phase
ends with a test run and a review checkpoint. Work does not proceed to the next
sub-phase until the previous one passes.

### 2A — Election and durable vote state

Deliverables:

- `internal/raft` package skeleton, roles, terms, votes, log sentinel, and
  deterministic RPC encoding.
- Crash-safe metadata persistence for `currentTerm` and `votedFor`.
- Randomized election timeout from `sim.Env.IntRange`.
- `RequestVote` request/response handling and all Follower/Candidate/Leader
  transitions that do not require log replication.
- No-op leader entry on election, persisted before any heartbeat. 2A does not
  send `AppendEntries` yet, so leadership is not stable across ticks until 2B;
  the 2A smoke test observes the election and durable no-op, not a quiescent
  cluster.
- Recovery of term, vote, and log from disk on every boot.
- Unit tests for vote rules, stale terms, log up-to-date checks, and
  crash-before-sync.

> **2A implementation note (2026-11-26):** The approved 2A scope was extended
> to include empty `AppendEntries` heartbeats, so leadership stability is tested
> within 2A. To keep log replication out of 2A, the leader no-op entry and all
> log persistence are deferred to 2B; 2A persists only `raft.meta`
> (`currentTerm`/`votedFor`), and boot recovery restores only those fields.

Gate 2A:

```sh
go test ./internal/raft
go vet ./...
```

The 2A election smoke test lives in `internal/raft` and drives a small
`sim.Sim` cluster through `raft.NewFactory`; the full Troublemaker scenario is
not required until 2C.

Acceptance: one leader per term in fault-free runs; no vote reply is sent
before the metadata sync completion; a crash during the vote sync loses the
vote and sends no reply; restart recovers the last durable term/vote.

### 2B — Replication and commit safety

Deliverables:

- `AppendEntries` heartbeats and log replication.
- `nextIndex`/`matchIndex`, conflict backtracking, and idempotent duplicate
  handling.
- Durable append/truncate batches before success replies and before
  `matchIndex` advancement.
- Leader commit advancement using a majority and the Figure 8 current-term
  rule.
- Apply hook in index order, exactly once per incarnation.
- Stale/delayed RPC rejection and defensive checks against truncating
  committed entries.
- Targeted Figure 8 unit and integration tests.

Gate 2B:

```sh
go test ./internal/raft
go test -race ./...
go vet ./...
```

The 2B replication smoke test also lives in `internal/raft`; the scenario-level
Troublemaker tests arrive in 2C.

Acceptance: a leader never commits an entry from a previous term merely because
it is present on a majority; followers never truncate committed entries;
success replies and `matchIndex` advancement happen only after the log batch is
durable; delayed duplicates are idempotent.

### 2C — Troublemaker scenario and per-step invariants

Deliverables:

- `internal/scenarios/raft.go` with a 5-node cluster, a deterministic client
  actor, chaos driver, final quiescence, and registered invariants.
- `cmd/simctl` support for `-scenario raft`.
- Election Safety, Log Matching, State Machine Safety, and Commit Durability
  invariants checked after every simulator step.
- `testdata/stage2/raft-seed-42.trace` golden trace.
- Fast default multi-seed suite and configurable 10,000+ seed stress target.
- README status, Stage 2 plan link, commands, and bug-log instructions.

Gate 2C:

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/scenarios -run TestRaftSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestRaftGoldenTraceSeed42
make stress-raft SEEDS=10000
go run ./cmd/simctl -seed 42 -scenario raft -ticks 400 -trace
```

Acceptance: deterministic replay, golden trace byte equality, all invariants
hold at every step for the default seed range, the 10,000-seed target completes
without a structural or safety violation, and final quiescence elects a leader
and commits client commands.

---

## 3. Package layout

```text
internal/raft/
  node.go          # Node actor, inbox/pump, lifecycle, role transitions
  state.go         # Role, Entry, Config, persistent/volatile state
  rpc.go           # RPC structs, deterministic binary encode/decode
  log.go           # sentinel log, term/index lookup, conflict search
  storage.go       # meta/log file formats, recovery, persistence state machine
  election.go      # election timer, RequestVote, vote counting
  replication.go   # AppendEntries, nextIndex/matchIndex, commit, apply
  client.go        # client request/response payloads and leader handling
  view.go          # read-only View for scenario invariants and Snapshot
  *_test.go
  export_test.go   # test-only state injection and fault hooks

internal/scenarios/
  raft.go          # Troublemaker cluster, client actor, chaos, invariants
  raft_test.go     # determinism, golden, multi-seed, final-state tests

cmd/simctl/
  main.go          # add raft scenario dispatch

testdata/stage2/
  raft-seed-42.trace
```

`internal/raft` does not import `internal/scenarios`. The scenario imports both
`internal/sim` and `internal/raft`, and reads live nodes through the exported
read-only `View`. The CLI imports only `internal/scenarios` and `internal/sim`.

---

## 4. Core state and exact types

### 4.1 Roles and entries

```go
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string // "follower", "candidate", "leader"

type EntryKind uint8

const (
	EntryNoop    EntryKind = 0
	EntryCommand EntryKind = 1
)

type Entry struct {
	Index uint64
	Term  uint64
	Kind  EntryKind
	Data  []byte // nil for no-op; opaque client bytes for commands
}
```

The log is a slice with a sentinel at index 0:

```go
// log[0] = Entry{Index: 0, Term: 0, Kind: EntryNoop}
// log[i] is Raft index i for i >= 1.
```

Terms start at 0 on a blank node. The first election increments to term 1.
`votedFor == 0` means “no vote”. Node IDs are non-zero (`sim` enforces this).

### 4.2 Configuration and factory

```go
type Config struct {
	ID                 sim.NodeID
	Peers              []sim.NodeID // excludes ID, sorted, unique
	HeartbeatInterval  sim.Tick     // default 3
	ElectionTimeoutMin sim.Tick     // default 12
	ElectionTimeoutMax sim.Tick     // default 24
	MaxAppendEntries   int          // default 64
	Apply              func(Entry)  // optional; must be deterministic
}

func NewFactory(cfg Config) sim.NodeFactory
```

Validation at factory construction:

- `ID != 0`, peers non-zero, sorted, unique, and do not contain `ID`.
- `HeartbeatInterval >= 1`.
- `ElectionTimeoutMin >= 3*HeartbeatInterval`.
- `ElectionTimeoutMax >= ElectionTimeoutMin`.
- `MaxAppendEntries >= 1`.
- `Apply` may be nil; if non-nil it must not call Raft methods, mutate Raft
  state, or consume RNG. The scenario's hook only records applied entries.

The factory captures a copy of the config and an incarnation counter. Each boot
increments the incarnation and returns a fresh `*Node`. The constructor draws no
randomness; the first draw happens after recovery completes.

### 4.3 Node state

```go
type Node struct {
	env         sim.Env
	cfg         Config
	id          sim.NodeID
	peers       []sim.NodeID
	incarnation uint64

	// Persistent (durable copies tracked separately).
	currentTerm uint64
	votedFor    sim.NodeID
	log         []Entry // sentinel at 0
	logBytes    uint64  // encoded bytes in raft.log, including obsolete batches

	// Volatile.
	role        Role
	commitIndex uint64
	lastApplied uint64
	leaderID    sim.NodeID

	// Leader replication state, aligned with peers.
	nextIndex  []uint64
	matchIndex []uint64

	// Candidate votes, aligned with peers; self is tracked separately.
	votesGranted []bool
	selfVoted    bool

	// Timers. Timer IDs are unique; stale delivered events are ignored.
	electionTimer  sim.TimerID
	heartbeatTimer sim.TimerID

	// Durable bookkeeping, for reply gating and invariants.
	durableTerm      uint64
	durableVotedFor  sim.NodeID
	durableLastIndex uint64

	// Boot recovery.
	recovering       bool
	metaRecovered    bool
	logRecovered     bool
	recoveredMeta    []byte
	recoveredLog     []byte

	// Serialized persistence: at most one batch in flight.
	busy    bool
	pending *pendingPersist
	inbox   []inbound

	// Leader-side client notifications; volatile by design.
	pendingClients map[uint64]clientRef

	nextIO uint64
}

type clientRef struct {
	ClientID sim.NodeID
	Seq      uint64
}
```

`inbound` holds only messages and timers. Disk completions are handled directly
because they are the persistence continuation.

```go
type inboundKind uint8

const (
	inboundMessage inboundKind = iota
	inboundTimer
)

type inbound struct {
	kind  inboundKind
	msg   sim.Message
	timer sim.TimerEvent
}
```

### 4.4 Read-only view for invariants

```go
type View struct {
	ID               sim.NodeID
	Incarnation      uint64
	Recovering       bool
	Role             Role
	Term             uint64
	DurableTerm      uint64
	VotedFor         sim.NodeID
	DurableVotedFor  sim.NodeID
	LeaderID         sim.NodeID
	CommitIndex      uint64
	LastApplied      uint64
	LastLogIndex     uint64
	LastLogTerm      uint64
	DurableLastIndex uint64
	Busy             bool
	Log              []Entry // read-only; valid only during the call
}

func (n *Node) View() View
```

`View` does not copy the log and does not consume RNG or mutate state. Invariants
must not retain or modify `Log`. The sentinel is included at position 0 so that
`View.Log[i]` is Raft index `i`.

`Recovering` is true from `Start()` until both boot reads have been decoded.
Invariants skip a live node while `Recovering` is true; otherwise a restart
would look like a temporary loss of durable term/vote/log before the disk reads
complete. The first observation after recovery establishes the new
incarnation's durable baseline.

`Node` implements `sim.Snapshotter`:

```go
func (n *Node) Snapshot() []byte
```

`Snapshot` is a canonical binary encoding of all behavior-affecting state:
ID, configuration timeouts and append cap, incarnation, role, term/vote and
their durable copies, `durableLastIndex`, commit/applied indexes, leader ID,
log entries, peer order, `nextIndex`, `matchIndex`, `votesGranted`, `selfVoted`,
`busy`, `logBytes`, `nextIO`, the recovery flags and any partially recovered
meta/log bytes, the pending persistence kind/action/phase/tags, its captured
`metaTerm`, `metaVote`, `grant`, `resetElection`, `requestSummary`, and
`logBatch`, the current
`electionTimer` and `heartbeatTimer`, pending client references sorted by log
index, and the ordered `inbox` (message kind/from/payload and timer ID/tag). It
must not encode pointers, map order, or wall-clock data. The `after` closure is
not encoded because its behavior is fully determined by the action code and
captured fields. The optional `Apply` callback is fixed scenario configuration,
not per-node state; it is not encoded, and the scenario supplies the same hook
for every run of a given binary. Including the inbox and the current timer IDs matters because a
message or timer delivered to a busy or recovering node has left the simulator
queue but still affects the node's future behavior.

### 4.5 Wire version and message kinds

```go
const wireVersion uint16 = 1

const (
	KindVoteRequest    = "raft.vote-req"
	KindVoteResponse   = "raft.vote-resp"
	KindAppendRequest  = "raft.append-req"
	KindAppendResponse = "raft.append-resp"
	KindClientRequest  = "raft.client-req"
	KindClientResponse = "raft.client-resp"
)
```

Payloads are deterministic big-endian binary. Every payload begins with
`wireVersion` as a `uint16`. Decoding checks length before every read. A
malformed payload from our own code is a bug and panics with a deterministic
diagnostic; unknown message kinds are ignored. There is no JSON, reflection, or
map encoding.

The scenario client needs exactly two codecs, so `internal/raft` exports:

```go
func EncodeClientRequest(req ClientRequest) []byte
func DecodeClientResponse(payload []byte) (ClientResponse, error)
```

Both use the same wire version and length checks as the internal RPC codecs.
All vote and append codecs remain unexported; the client actor does not depend
on Raft internals beyond these two helpers and the payload structs.

### 4.6 RPC structs

```go
type VoteRequest struct {
	Term         uint64
	CandidateID  sim.NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendRequest struct {
	Term         uint64
	LeaderID     sim.NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendResponse struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64 // last index proven to match on success
	ConflictIndex uint64 // first index of the conflicting term, or next index
	ConflictTerm  uint64 // 0 when the follower log is too short
}

type ClientRequest struct {
	ClientID sim.NodeID
	Seq      uint64
	Data     []byte
}

type ClientResponse struct {
	Term      uint64
	Accepted  bool
	Committed bool
	LeaderID  sim.NodeID
	Index     uint64
}
```

`AppendRequest.Entries` is capped at `MaxAppendEntries`. Entry data is capped at
`MaxEntryBytes = 4096`; client data is capped at `MaxClientBytes = 1024`.
Decoders reject larger lengths and malformed counts. A request with zero entries
is a heartbeat.

`AppendResponse.MatchIndex` is `PrevLogIndex + len(Entries)` on success, not the
follower's full last index. This proves exactly the prefix covered by the
request and forces the leader to discover any extra follower suffix with a
subsequent request.

### 4.7 Actor execution model

`Node` is a passive actor. It never blocks, never runs in the background, and
never calls itself recursively.

- `HandleMessage` and `HandleTimer` append an `inbound` record to `inbox` and
  call `pump()`.
- `pump()` returns immediately while `recovering` or `busy`. Otherwise it pops
  the oldest `inbound` record, dispatches it, and repeats. A handler that starts
  a persistence batch sets `busy`; `pump()` then stops with the remaining
  messages still queued. This is the serialization point that makes “durable
  before reply” exact.
- `HandleIO` is never enqueued. It is the continuation for recovery or for the
  one in-flight persistence batch. It validates the tag, operation, and phase,
  performs the durable bookkeeping, clears `busy`, runs the captured `after`
  continuation, and finally calls `pump()`.
- The `after` continuation must not call `pump()` itself. “Drain the inbox” in
  the protocol sections means “return to `HandleIO`, which pumps”.
- During recovery, `HandleIO` accepts only the two recovery read tags. When both
  reads are present it decodes them, clears `recovering`, schedules the first
  election timer, and pumps. Any runtime write/sync completion, unexpected tag,
  wrong operation, or wrong phase is a deterministic panic: the simulator only
  delivers completions for operations this incarnation issued.
- A message or timer that arrives while `busy` or `recovering` stays in `inbox`
  in delivery order. No handler may bypass the queue by acting on a message
  directly.
- Timer handlers compare the delivered `TimerID` with the node's current timer
  field and ignore canceled or superseded events. Because the inbox preserves
  delivery order, this check is deterministic and does not depend on map
  iteration.
- `inbox` is bounded only by the simulator run length. The 10,000-seed stress
  target is the check that it does not grow without bound under the configured
  chaos.

---

## 5. Persistence design

### 5.1 Files

Each Raft node owns two files on its simulator disk:

```text
raft.meta   fixed-size term/vote record
raft.log    append-only framed batches of log mutations
```

No other files are used in Stage 2. `List` is not required because filenames are
fixed. Recovery uses tags 1 and 2; `nextIO` starts at `firstRuntimeTag-1` and
each runtime write/sync takes the next tag from 100 upward. Both files are read
during boot with bounded sizes:

```go
const (
	metaFile        = "raft.meta"
	logFile         = "raft.log"
	metaSize        = 28
	maxMetaFileSize = 64
	maxLogFileSize  = 4 << 20
	maxEntryBytes   = 4096
	maxClientBytes  = 1024
	tagRecoverMeta  = uint64(1)
	tagRecoverLog   = uint64(2)
	firstRuntimeTag = uint64(100)
)
```

### 5.2 Meta format

`raft.meta` is exactly 28 bytes:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | magic `R2MT` |
| 4 | 2 | format version `1` |
| 6 | 2 | flags, must be `0` |
| 8 | 8 | `currentTerm` |
| 16 | 8 | `votedFor` (`0` = none) |
| 24 | 4 | CRC-32 (IEEE) over bytes 0..23 |

A missing or zero-length file means term `0`, vote `0`. Any other length, bad
magic, bad version, nonzero flags, or CRC mismatch is a deterministic boot
failure. The simulator does not corrupt bytes; a mismatch means an
implementation bug, not a fault to tolerate.

### 5.3 Log format

`raft.log` is a sequence of framed batch records. Each record is one atomic
`Append`:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | magic `R2LG` |
| 4 | 2 | format version `1` |
| 6 | 1 | record kind `1` (log batch) |
| 7 | 1 | reserved, must be `0` |
| 8 | 8 | `baseIndex` (first entry index in this batch) |
| 16 | 4 | entry count |
| 20 | 4 | payload byte length |
| 24 | N | payload |
| 24+N | 4 | CRC-32 (IEEE) over header + payload |

Each payload entry is:

| Size | Field |
|-----:|-------|
| 8 | `index` |
| 8 | `term` |
| 1 | `kind` |
| 1 | reserved, must be `0` |
| 4 | data length |
| N | data bytes |

Recovery applies records in file order:

1. `baseIndex` must be in `[1, len(log)]`, where `len(log)` is the number of
   entries including the sentinel.
2. If `baseIndex < len(log)`, truncate the in-memory log to `baseIndex-1`.
3. Entries must be contiguous, start at `baseIndex`, have non-decreasing terms,
   fit the data cap, and the first appended term must be at least the term of
   the entry at `baseIndex-1` (the sentinel has term 0). Append them.
4. A batch with `count == 0` is invalid. A batch whose first index is
   `len(log)` is a pure append; a batch whose first index is smaller is a
   truncate-and-replace.
5. A trailing partial record is a deterministic boot failure. Stage 1 has no
   torn writes, so this can only indicate a bug.

The log is never rewritten in Stage 2. Truncation is represented by a later
batch record; recovery replays the records to reconstruct the current log.
Obsolete bytes remain in the file until Stage 3 adds compaction. The node
tracks the encoded byte count (`logBytes`): recovery initializes it from the
read length, and every successful log sync adds the batch size. Before issuing
a batch that would exceed `maxLogFileSize`, the node panics deterministically
with a message that Stage 2 does not compact; this is a test-size guard, not a
silent truncation. A recovery read that ends mid-record also panics.

### 5.4 Boot recovery

`Start()` does no RNG draws and no messaging. It sets `recovering = true` and
issues:

```go
env.Read(tagRecoverMeta, metaFile, 0, maxMetaFileSize+1)
env.Read(tagRecoverLog, logFile, 0, maxLogFileSize+1)
```

Reading one byte past the cap makes an oversized file detectable instead of
silently parsing a valid prefix. If either read returns more than its cap, the
node panics deterministically. Disk FIFO ordering means the meta read completes
first, then the log read. Both completions are handled by `HandleIO`. When both
are present:

1. Decode meta; set `currentTerm`, `votedFor`, `durableTerm`,
   `durableVotedFor`.
2. Decode the log; set `log`, `durableLastIndex = lastLogIndex`, and
   `logBytes = len(recoveredLog)`.
3. Set `commitIndex = 0`, `lastApplied = 0`, `leaderID = 0`, `role = Follower`.
   Clear `nextIndex`, `matchIndex`, `votesGranted`, `selfVoted`,
   `pendingClients`, both timer IDs, `busy`, and `pending`. Set
   `nextIO = firstRuntimeTag - 1` (99). The factory assigns a new
   `incarnation`; runtime IO tags restart at 100 because the simulator drops
   completions from the old incarnation.
4. Clear `recovering`, schedule the first randomized election timer, and drain
   any messages that arrived while recovering.

`commitIndex` is intentionally volatile per Raft Figure 2. A restarted node
keeps its durable log but learns the commit index again from a leader. The apply
hook is replayed from index 1 as commit advances, so the scenario state machine
reconstructs itself.

Messages and timers that arrive during recovery are appended to `inbox` and not
processed until recovery finishes. No election timer exists before recovery, so
a restart cannot campaign on partially recovered state.

### 5.5 Serialized persistence state machine

There is at most one persistence batch in flight per node. This makes reply
gating and crash reasoning exact:

```go
type persistKind uint8

const (
	persistMeta persistKind = iota
	persistLog
)

type pendingAction uint8

const (
	actionNone pendingAction = iota
	actionElectionSelfVote
	actionVoteReply
	actionStepDown
	actionAppendAfterTerm
	actionLeaderNoop
	actionLeaderClientAppend
	actionFollowerAppendReply
)

type pendingPersist struct {
	kind          persistKind
	action        pendingAction
	phase         uint8 // 1 = append/replace issued, 2 = sync issued
	tagWrite      uint64
	tagSync       uint64
	metaTerm      uint64
	metaVote      sim.NodeID
	grant         bool // captured vote decision for actionVoteReply
	resetElection bool // captured timer decision for actionVoteReply/actionStepDown
	request       requestSummary
	batch         logBatch
	after         func()
}

type requestSummary struct {
	peer         sim.NodeID
	term         uint64
	prevIndex    uint64
	prevTerm     uint64
	leaderCommit uint64
	entries      []Entry
	clientID     sim.NodeID
	clientSeq    uint64
}

type logBatch struct {
	baseIndex uint64 // first entry index
	entries   []Entry
}
```

`pendingAction` and the captured `grant`/`request` fields exist so `Snapshot`
can distinguish two nodes with the same in-memory term/log but different
pending continuations. The `after` closure itself is never encoded. It must
capture only value copies that are also represented in `pending`; it must not
read mutable node state that is absent from the snapshot, or two behaviorally
different pending nodes could share a state hash.

`beginMetaPersist(term, vote, after)`:

1. Assert `!busy`.
2. Allocate `tagWrite` and `tagSync` from `nextIO` (tags 100, 101, ...).
3. Encode the meta record.
4. `busy = true`, `pending = ...`, `phase = 1`.
5. `env.Replace(tagWrite, metaFile, encoded)`.

`beginLogPersist(batch, after)`:

1. Assert `!busy`, `batch.entries` non-empty and contiguous.
2. Allocate `tagWrite` and `tagSync` from `nextIO`.
3. Encode one framed batch record; record its encoded length. If
   `logBytes + encodedLength > maxLogFileSize`, panic before issuing the write.
4. `busy = true`, `pending = ...`, `phase = 1`.
5. `env.Append(tagWrite, logFile, encoded)`.

On the write completion (`IOAppend` or `IOReplace`):

1. Verify tag, op, and `phase == 1`; otherwise panic.
2. `phase = 2`; `env.Sync(tagSync)`.

On the sync completion (`IOSync`):

1. Verify tag, op, and `phase == 2`; otherwise panic.
2. For meta: `durableTerm = metaTerm`, `durableVotedFor = metaVote`.
3. For log: `durableLastIndex = batch.entries[len-1].Index` and
   `logBytes += encoded batch length`.
4. Clear `busy` and `pending`.
5. Run `after()` exactly once.
6. `pump()` the inbox.

An `IOEvent.Err` from a write or sync is an unrecoverable persistence failure
and panics with the operation, tag, and error. File names are fixed and offsets
are valid, so this can only indicate a simulator or implementation bug.

`durableLastIndex` is the last index of the most recently synced log batch. It
can be greater than the current in-memory `lastLogIndex()` while a
truncate-and-append batch is pending: the old durable suffix is still on disk
until that batch's sync completes. It must never be less than `commitIndex`;
the defensive truncation checks enforce that. After the sync, it becomes the
new batch's last index (which may be smaller than the previous value).

### 5.6 Reply and send gating table

| Outbound action | Must wait for |
|---|---|
| Send `RequestVote` after starting an election | Meta sync covering the incremented term and self-vote |
| Send `VoteResponse` granting a vote | Meta sync covering term and vote |
| Send `VoteResponse` denying a vote | Meta sync if the request raised the term; otherwise immediate |
| Send `AppendEntries` carrying a newly appended leader entry | Leader log sync covering that entry |
| Send `AppendEntries` heartbeat | No new persistence; term must already be durable |
| Send `AppendEntries` success response | Log sync if the follower changed its log; meta sync if the term changed; otherwise immediate |
| Send `AppendEntries` failure response | Meta sync if the term changed; otherwise immediate |
| Advance leader `matchIndex[self]` | Leader log sync |
| Advance leader `commitIndex` | Majority durable match, including leader durable log |
| Apply committed entries | Commit index advanced and entries durable on this node |
| Send client “accepted” response | Leader log sync |
| Send client “committed” response | Entry committed and applied |

A crash before the required sync completion produces no reply. A crash after the
sync completion but before the continuation cannot occur as a separate
simulator event: the durable update, `after()`, and any reply happen inside the
same `HandleIO` dispatch. A crash event at the same tick either runs before that
dispatch (no reply, old durable state) or after it (reply already sent, new
durable state).

### 5.7 Stale epochs and stale completions

The simulator drops messages, timers, and disk completions tagged with an old
node epoch. A restarted node therefore never sees a pre-crash `Sync` completion.
The Raft node additionally treats any unexpected IO tag/phase as a bug and
panics. There is no code path that waits for an old-epoch completion after a
restart.

---

## 6. Election protocol (Stage 2A)

### 6.1 Timers and constants

```go
const (
	defaultHeartbeatInterval  = sim.Tick(3)
	defaultElectionTimeoutMin = sim.Tick(12)
	defaultElectionTimeoutMax = sim.Tick(24)
	timerTagElection          = uint64(1)
	timerTagHeartbeat         = uint64(2)
)
```

- Followers and candidates run one election timer. Leaders run one heartbeat
  timer.
- `resetElectionTimer()` cancels the current timer, draws
  `env.IntRange(min, max)`, and schedules a new one. The draw is the only
  randomness in Raft and happens in event order.
- Timer events carry a unique `TimerID`. A handler ignores an event whose ID is
  not the node's current timer ID. This makes canceled or already-fired timers
  harmless without relying on map order.
- `resetHeartbeatTimer()` schedules `HeartbeatInterval`; it does not draw.
- On becoming leader, the node cancels its election timer and starts a heartbeat
  timer after its no-op entry is durable.
- On stepping down, the node cancels its heartbeat timer and starts an election
  timer after the new term is durable.

### 6.2 Starting an election

`startElection()` runs only on a follower or candidate election-timeout event:

1. Set `role = Candidate`.
2. `currentTerm++`.
3. `votedFor = id`, `leaderID = 0`.
4. Clear `votesGranted`, set `selfVoted = false`.
5. Cancel any heartbeat timer.
6. Persist `(currentTerm, id)` with `beginMetaPersist`. The continuation:
   - asserts `role == Candidate`, `currentTerm` and `votedFor` match the
     persisted values;
   - sets `selfVoted = true`;
   - if the node is a single-node cluster, calls `becomeLeader()`;
   - otherwise sends one `RequestVote` to every peer in ascending peer order,
     with `LastLogIndex` and `LastLogTerm` from the durable log;
   - resets the election timer;
   - drains the inbox.

No `RequestVote` is sent before the meta sync. If the node crashes during the
sync, it sends nothing, and restart recovers the previous term/vote. A candidate
that times out again repeats the process with a higher term and a fresh vote.

### 6.3 RequestVote handling

`handleVoteRequest(from, req)`:

1. Decode and validate: `req.Term >= 1`, `CandidateID != 0`, candidate is a
   configured peer, `from == CandidateID`, `LastLogIndex == 0` implies
   `LastLogTerm == 0`, and `LastLogIndex > 0` implies `LastLogTerm >= 1`.
   Invalid payloads panic; unknown peers are ignored. Record
   `wasLeader := role == Leader` before any state change.
2. If `req.Term < currentTerm`, send `VoteResponse{Term: currentTerm,
   VoteGranted: false}` immediately. No durable state changes.
3. Otherwise compute the next durable term/vote:
   - `newTerm = currentTerm`, `newVote = votedFor`.
   - If `req.Term > currentTerm`: `newTerm = req.Term`, `newVote = 0`,
     `leaderID = 0`; if the node is not already a follower, become a follower,
     clear `pendingClients`, and cancel the heartbeat timer.
4. Compute log up-to-date:
   ```go
   localTerm, localIndex := lastLogTerm(), lastLogIndex()
   upToDate := req.LastLogTerm > localTerm ||
       (req.LastLogTerm == localTerm && req.LastLogIndex >= localIndex)
   ```
5. Grant iff `(newVote == 0 || newVote == req.CandidateID) && upToDate`.
6. If granting, set `newVote = req.CandidateID`.
7. Compute `restartElectionTimer := grant || (wasLeader && role == Follower)`:
   granting a vote resets the timer per Figure 2, and a former leader that
   actually stepped down has no election timer to keep. A leader that denies a
   same-term request stays leader and must not acquire an election timer.
8. If `newTerm != durableTerm || newVote != durableVotedFor`:
   - set `currentTerm = newTerm`, `votedFor = newVote`;
   - call `beginMetaPersist(newTerm, newVote, after)`, where `after`:
     - if `restartElectionTimer`, resets the election timer; otherwise leaves
       the existing follower/candidate timer running (a denied higher-term
       vote must not delay a legitimate election);
     - sends `VoteResponse{Term: newTerm, VoteGranted: grant}`;
     - drains the inbox.
9. Otherwise the durable state already covers the reply:
   - apply the same `restartElectionTimer` rule;
   - send `VoteResponse{Term: currentTerm, VoteGranted: grant}` immediately.

This single meta write combines a term increase and a vote grant. A request that
raises the term but is denied still persists the higher term before replying, so
a crash cannot resurrect an older term. A duplicate request from the same
candidate at the same term, after the vote is durable, is answered immediately.

### 6.4 Vote response handling

`handleVoteResponse(from, resp)`:

1. Decode and validate `from` is a peer.
2. If `resp.Term < currentTerm`, ignore.
3. If `resp.Term > currentTerm`, record `wasLeader := role == Leader`, step
   down to follower at `resp.Term` with `votedFor = 0`; persist the new term;
   cancel heartbeat; if `wasLeader`, start an election timer after the sync,
   otherwise leave the existing candidate/follower timer running. Ignore the
   vote.
4. If `role != Candidate` or `resp.Term != currentTerm`, ignore.
5. If `!resp.VoteGranted`, ignore.
6. Find the peer's vote slot. If already true, ignore (duplicate response).
7. Set it true. If `1 + count(votesGranted) >= majority`, call
   `becomeLeader()`.

`majority = (len(peers)+1)/2 + 1`. Self is counted only after `selfVoted` is set
by the election persistence continuation.

### 6.5 Becoming leader

`becomeLeader()` asserts:

- `role == Candidate`;
- `currentTerm == durableTerm`;
- `votedFor == id && durableVotedFor == id`;
- `selfVoted`;
- `1 + count(votesGranted) >= majority`.

Then:

1. `role = Leader`, `leaderID = id`.
2. Cancel the election timer.
3. Initialize `nextIndex[f] = lastLogIndex()+1` and `matchIndex[f] = 0` for every
   peer. The leader's own match is `durableLastIndex`.
4. Append a no-op entry at `currentTerm` and `lastLogIndex()+1`.
5. Persist that entry with `beginLogPersist`. The continuation:
   - asserts the no-op is durable;
   - calls `advanceCommit()` (this immediately commits the no-op in a
     single-node cluster; in a larger cluster it has no effect until a majority
     matches);
   - broadcasts `AppendEntries` to all peers;
   - schedules the heartbeat timer;
   - drains the inbox.

The no-op gives the new leader a current-term entry that can be committed,
which is what allows entries from earlier terms to become committed safely
under the Figure 8 rule. No heartbeat or replication is sent before the no-op is
durable.

**2A boundary.** In Stage 2A, the no-op continuation stops after
`advanceCommit()` and the inbox drain: it does not broadcast `AppendEntries` or
schedule a heartbeat. Stage 2B adds steps 5–6. This keeps the 2A gate testable
without replication while still proving the durable no-op path.

### 6.6 Exact role transitions

| From | Trigger | To | Required durable action |
|---|---|---|---|
| Follower | election timer | Candidate | Persist `term+1`, self-vote before sending |
| Candidate | election timer | Candidate | Persist new higher term and self-vote |
| Candidate | majority of votes in current term | Leader | Persist no-op before heartbeat |
| Candidate | `AppendEntries` term >= current term | Follower | Persist higher term if raised |
| Candidate | `RequestVote` higher term | Follower | Persist higher term and vote decision |
| Leader | any RPC with higher term | Follower | Persist higher term and clear vote |
| Leader | valid `AppendEntries` same term from another leader | Follower | Defensive; Election Safety invariant must fail |
| Follower | valid `AppendEntries` same/higher term | Follower | Reset election timer; persist term if raised |
| Follower | grant vote | Follower | Persist vote before reply; reset timer |

No other transitions exist. A leader never becomes a candidate without first
stepping down through a higher-term event or an election timeout after losing
leadership. Every transition away from Leader clears `pendingClients` by
replacing it with an empty map, cancels the heartbeat timer, and leaves
`commitIndex`/`lastApplied` intact for the entries that were already committed.

### 6.7 Role transition helpers

Three small helpers keep every call site identical:

```go
func (n *Node) becomeFollower(term uint64, leader sim.NodeID)
func (n *Node) cancelElectionTimer()
func (n *Node) cancelHeartbeatTimer()
```

`becomeFollower(term, leader)`:

1. Assert `term >= n.currentTerm`.
2. If `term > n.currentTerm`, clear `votedFor = 0` (a new term starts with no
   vote). If `term == n.currentTerm`, preserve the existing `votedFor`; a
   same-term demotion must not erase a vote that is already durable.
3. `role = Follower`, `currentTerm = term`, `leaderID = leader`.
4. `pendingClients = map[uint64]clientRef{}`.
5. `cancelHeartbeatTimer()`; clear `votesGranted`, `selfVoted = false`.
6. Do **not** reset the election timer here. The caller resets it after the
   required meta sync, or immediately when no durable term change was needed.
   This prevents a timer from firing while the new term is not yet durable.

Callers that raise the term must persist before sending any message or
resetting the timer. Callers that only demote at the same term (a candidate
receiving a valid current-term `AppendEntries`) may reset the timer immediately
because no durable state changes.

`cancelElectionTimer()` and `cancelHeartbeatTimer()` cancel the simulator timer
if the stored ID is non-zero and then set the field to 0. They never draw RNG.
A canceled timer may still be delivered; the handler's ID check ignores it.

---

## 7. Log replication and commit (Stage 2B)

### 7.1 Leader append path

All leader log mutations go through one helper:

```go
func (n *Node) appendLocal(kind EntryKind, data []byte) Entry
```

It creates `Entry{Index: lastLogIndex()+1, Term: currentTerm, Kind: kind,
Data: clone(data)}`, appends it to `log`, and returns it. It does not mark it
durable and does not send anything.

Two paths append entries:

- **No-op on election**: described in 6.5.
- **Client command**: `handleClientRequest` appends `EntryCommand` with the
  encoded `ClientRequest` as data.

Both paths call `beginLogPersist` with `baseIndex = entry.Index` and one entry.
The continuation runs only after the leader's log sync:

1. Assert `durableLastIndex >= entry.Index`.
2. Record the client reference in `pendingClients[entry.Index]` if applicable.
3. Send the “accepted” client response if applicable.
4. Call `advanceCommit()` (a single-node cluster commits immediately; a larger
   cluster waits for a majority).
5. Broadcast `AppendEntries` to all peers.
6. Schedule or keep the heartbeat timer.
7. Drain the inbox.

A leader never sends a newly appended entry, advances its own match, or replies
“accepted” before that entry is durable on its own disk.

### 7.2 Sending AppendEntries

`sendAppendEntries(peerIndex)`:

1. `prev := nextIndex[peerIndex] - 1`; clamp `nextIndex` to
   `[matchIndex[peerIndex]+1, lastLogIndex()+1]`.
2. `prevTerm := log[prev].Term` (the sentinel has term 0).
3. Collect up to `MaxAppendEntries` entries starting at `nextIndex`.
4. Encode:
   ```go
   AppendRequest{
       Term:         currentTerm,
       LeaderID:     id,
       PrevLogIndex: prev,
       PrevLogTerm:  prevTerm,
       Entries:      entries,
       LeaderCommit: commitIndex,
   }
   ```
5. `env.Send(peer, KindAppendRequest, payload)`.

`broadcastAppendEntries()` calls this for every peer in ascending peer order.
There is no in-flight gate: multiple AppendEntries may be outstanding, which is
safe because responses are idempotent and `matchIndex` is monotonic. A delayed
older request from the same term only re-sends a prefix the follower already
has; it cannot truncate newer matching entries. A delayed request from an older
term is rejected by the term check.

Heartbeats use the same function with no entries. The leader sends heartbeats
every `HeartbeatInterval` while it remains leader. A heartbeat does not persist
anything; the leader's term/vote are already durable.

### 7.3 Follower AppendEntries handling

`handleAppendRequest(from, req)`:

1. Decode and validate: `req.Term >= 1`, `LeaderID != 0`, leader is a
   configured peer, `from == LeaderID`, entry count and data caps hold,
   `PrevLogIndex == 0` implies `PrevLogTerm == 0`, and `PrevLogIndex > 0`
   implies `PrevLogTerm >= 1`. Entry indices must be contiguous and start at
   `PrevLogIndex+1`.
2. If `req.Term < currentTerm`, reply
   `AppendResponse{Term: currentTerm, Success: false, MatchIndex: 0,
   ConflictIndex: lastLogIndex()+1, ConflictTerm: 0}` immediately. No state
   change.
3. If `req.Term > currentTerm`:
   - set `currentTerm = req.Term`, `votedFor = 0`, `leaderID = req.LeaderID`;
   - become follower; cancel heartbeat; clear `pendingClients`;
   - persist the new term with `beginMetaPersist`; the continuation resets the
     election timer (a valid current-term leader exists) and then calls
     `continueAppendRequest(req)` after the sync.
4. If `req.Term == currentTerm`:
   - set `leaderID = req.LeaderID`;
   - if candidate or leader, become follower, cancel heartbeat, and clear
     `pendingClients`;
   - reset the election timer immediately (no durable term change);
   - call `continueAppendRequest(req)`.

`continueAppendRequest(req)`:

1. If `req.PrevLogIndex > lastLogIndex()`, reply failure with
   `ConflictIndex = lastLogIndex()+1`, `ConflictTerm = 0`. No log change.
2. If `req.PrevLogIndex >= 1` and `log[req.PrevLogIndex].Term !=
   req.PrevLogTerm`, reply failure with:
   - `ConflictTerm = log[req.PrevLogIndex].Term`;
   - `ConflictIndex = first index in the local log with that term`.
   No log change.
3. Let `base := req.PrevLogIndex + 1`.
4. Walk the request entries:
   - If `base+i <= lastLogIndex()` and the local entry has the same term:
     - if kind or data differ, panic: same index and term with different
       content violates Log Matching and cannot be produced by correct code;
     - otherwise continue (idempotent duplicate).
   - If `base+i <= lastLogIndex()` and the term differs: this is the first
     conflict. Truncate the in-memory log to `base+i-1`, append the remaining
     request entries, and stop walking.
   - If `base+i > lastLogIndex()`: append the remaining entries and stop.
5. If no entries were appended or truncated, this is a heartbeat or a duplicate:
   - update `commitIndex` from `LeaderCommit` (see 7.5);
   - apply;
   - reply success with `MatchIndex = req.PrevLogIndex + len(req.Entries)`.
6. If the log changed:
   - build `logBatch{baseIndex: firstChangedIndex, entries:
     entriesFromFirstChange}`;
   - persist with `beginLogPersist`; the continuation:
     - asserts the batch is durable;
     - updates `commitIndex` from `LeaderCommit` and applies;
     - replies success with `MatchIndex = req.PrevLogIndex + len(req.Entries)`;
     - drains the inbox.

Defensive checks:

- If a conflict or truncation point is `<= commitIndex`, panic with the index,
  local term, request term, and commit index. A correct leader can never ask a
  follower to replace a committed entry.
- If a request entry at an index `<= commitIndex` differs from the local entry,
  panic for the same reason.
- `LeaderCommit` is clamped to `lastNew = PrevLogIndex + len(Entries)` and never
  decreases `commitIndex`.

### 7.4 Conflict backtracking

`handleAppendResponse(from, resp)` first validates `from` is a peer and then:

1. If `resp.Term < currentTerm`, ignore the response.
2. If `resp.Term > currentTerm`, record `wasLeader := role == Leader`, step
   down to follower at `resp.Term` with `votedFor = 0`, persist the new term,
   cancel the heartbeat timer, clear `pendingClients`; if `wasLeader`, start an
   election timer after the sync, otherwise leave the existing
   candidate/follower timer running. Ignore the response body.
3. If `role != Leader` or `resp.Term != currentTerm`, ignore.
4. Otherwise process the success/failure body below.

On a failed `AppendResponse`, the leader computes:

```go
next := resp.ConflictIndex
if resp.ConflictTerm != 0 {
    if idx, ok := lastIndexOfTerm(resp.ConflictTerm); ok {
        next = idx + 1
    }
}
if next < matchIndex[f]+1 {
    next = matchIndex[f]+1
}
if next > lastLogIndex()+1 {
    next = lastLogIndex()+1
}
```

If `next < nextIndex[f]`, set `nextIndex[f] = next` and immediately send the
next `AppendEntries`. If `next >= nextIndex[f]`, the response is stale or gives
no new information; ignore it and wait for the next heartbeat. This guarantees
progress without a retry storm and never regresses below a known-matching
prefix.

On a successful response:

```go
if resp.MatchIndex > lastLogIndex() {
    panic(fmt.Sprintf("append success beyond leader log: match=%d last=%d", resp.MatchIndex, lastLogIndex()))
}
matchIndex[f] = max(matchIndex[f], resp.MatchIndex)
nextIndex[f] = max(nextIndex[f], matchIndex[f]+1)
```

Then call `advanceCommit()`. If `nextIndex[f] <= lastLogIndex()`, immediately
send the next batch to that peer; otherwise wait for the heartbeat.

### 7.5 Commit advancement and apply

`advanceCommit()` runs only on a leader:

1. Build a slice of length `N = len(peers)+1`:
   - the leader's own value is `durableLastIndex`;
   - each peer's value is `matchIndex[f]`.
2. Sort the slice ascending.
3. `majority := N/2 + 1`.
4. `candidate := sorted[N-majority]`. This is the highest index present on at
   least a majority.
5. If `candidate > commitIndex` and `log[candidate].Term == currentTerm`:
   - set `commitIndex = candidate`;
   - apply entries in order;
   - immediately broadcast a heartbeat carrying the new `LeaderCommit` so
     followers learn the commit index without waiting for the next tick.

If `candidate > commitIndex` but its term is older than `currentTerm`, do not
commit it. The leader's no-op entry (or a later client entry) from the current
term will eventually be on a majority; committing that entry commits all
preceding entries transitively. This is the Figure 8 rule. A leader never
commits by counting replicas of a previous-term entry alone.

`apply()`:

```go
for lastApplied < commitIndex {
    lastApplied++
    entry := log[lastApplied]
    if cfg.Apply != nil {
        cfg.Apply(entry)
    }
    if role == Leader {
        if ref, ok := pendingClients[entry.Index]; ok {
            sendCommittedResponse(ref, entry)
            delete(pendingClients, entry.Index)
        }
    }
}
```

`Apply` is called in index order, exactly once per incarnation. On restart,
`lastApplied` resets to 0 and the hook replays durable committed entries as the
new commit index advances. The hook must be deterministic and must not call
Raft, send messages, draw RNG, mutate Raft state, or mutate `Entry.Data`.

Follower commit update:

```go
lastNew := req.PrevLogIndex + uint64(len(req.Entries))
if req.LeaderCommit > commitIndex {
    newCommit := min(req.LeaderCommit, lastNew)
    if newCommit > commitIndex {
        commitIndex = newCommit
        apply()
    }
}
```

The clamp is to `lastNew`, not the follower's full `lastLogIndex()`. A follower
may have an unconfirmed suffix beyond the entries the leader just proved; that
suffix must not be committed merely because the leader's commit index is ahead.
A heartbeat proves only `PrevLogIndex`. The follower performs this update only
after any log change is durable. A follower trusts the leader's commit index for
the prefix the leader proved; the leader is responsible for the Figure 8
current-term rule.

### 7.6 Stale, delayed, and duplicate RPC handling

| Situation | Handling |
|---|---|
| RequestVote from an older term | Reply false with current term; no state change |
| RequestVote from a newer term | Persist new term and vote decision before reply |
| AppendEntries from an older term | Reply failure with current term; no log change |
| AppendEntries from a newer term | Persist new term, then process; reply after any required log sync |
| AppendEntries with `PrevLogIndex` beyond local log | Failure with `ConflictIndex = lastLogIndex+1`, `ConflictTerm = 0` |
| AppendEntries with mismatched `PrevLogTerm` | Failure with the conflicting term and its first index |
| Duplicate AppendEntries already in the log | No truncation, success after commit update |
| Delayed success response | `matchIndex` max and `nextIndex` max absorb it |
| Delayed failure response | Backtracking clamps to `matchIndex+1`; stale higher next is ignored |
| Response from an old term | Ignored unless it raises the node's term |
| Message to a crashed/restarted recipient | Dropped by `internal/sim` before delivery (destination epoch check) |
| Response to a message sent by a now-crashed sender | May be delivered to the restarted incarnation; term/role checks ignore it, and the durable term/vote/log are unchanged by the crash |
| Same index and term with different content | Panic: Log Matching violation and an implementation bug |
| Truncation at or below `commitIndex` | Panic: committed-entry safety violation |

### 7.7 Client command handling

The Raft node handles `KindClientRequest` as follows:

- If not leader: reply immediately with `ClientResponse{Term: currentTerm,
  Accepted: false, LeaderID: leaderID}`. No persistence. The leader ID lets a
  client that guessed a follower retarget without waiting for another election.
- If leader:
  1. Validate `ClientID != 0`, `Seq != 0`, `from == ClientID`, and data length
     cap. Invalid payloads panic as internal bugs; the scenario client always
     sends valid requests.
  2. Append `EntryCommand` whose data is the deterministic encoding of the
     client request.
  3. Persist the entry; the continuation records
     `pendingClients[index] = {ClientID, Seq}`, sends
     `ClientResponse{Term: currentTerm, Accepted: true, LeaderID: id,
     Index: index}`, and broadcasts.
  4. When the entry is applied and the node is still leader, send
     `ClientResponse{Term: currentTerm, Committed: true, LeaderID: id,
     Index: index}` and delete the pending entry.

“Accepted” means the entry is durable in the leader's log, not committed.
“Committed” means it is committed and applied. If leadership is lost before
commit, `pendingClients` is replaced with an empty map on step-down and no
committed response is sent; the client may retry. Replacing the map avoids any
iteration-order dependence. Stage 2 intentionally has no request deduplication,
so a retry can produce a second log entry. Stage 4 adds client IDs and
exactly-once semantics.

---

## 8. Troublemaker scenario (Stage 2C)

### 8.1 Constants and topology

```go
const (
	RaftScenario     = "raft"
	RaftChaosProfile = "troublemaker"
	RaftNodeCount    = 5
	RaftChaosEnd     = sim.Tick(200)
	RaftClientStop   = sim.Tick(380)
	DefaultRaftEnd   = sim.Tick(400)
)

func NewRaft(cfg sim.Config, nodeCount int, end sim.Tick) (*sim.Sim, error)
```

- Raft node IDs are `1..nodeCount` in ascending order. The default scenario
  uses exactly 5.
- `RaftChaosProfile = "troublemaker"` is the human-readable profile label used
  by docs and tests; the simulator scenario name remains `raft`, so replay
  commands stay `-scenario raft`.
- One client actor is added as node `nodeCount+1` (ID 6 for the default
  cluster). It is not a Raft peer and never votes.
- `RaftChaosEnd` and `RaftClientStop` are **fixed** ticks, independent of
  `end`. The chaos driver runs from tick 30 to 200, the finalizer runs at 200,
  and the client sends its last request before 380. The caller decides how far
  to run with `end`; the interval after 200 is the fault-free settle window.
- `end` is the total run end. It may be smaller than `RaftChaosEnd`; the
  finalizer is then scheduled but never reached. This is what makes the
  simulator's replay command (`-ticks <failure-tick>`) reproduce the same prefix
  for an early failure: neither the chaos schedule nor the client's stop time
  depends on the run target. On the common prefix, the client's behavior is
  identical for `end = 100`, `end = 400`, or `end = 1000`.
- `NewRaft` rejects only negative `end` and a `nodeCount` smaller than 3. A
  normal full run uses `end > RaftChaosEnd`; the default is 400. `end` is
  accepted for the `simctl`/`NewPing` API shape and validated, but the scenario
  schedule is fixed; the caller passes the same value to `RunUntil`.
- The scenario sets `cfg.Scenario = "raft"` when empty and leaves the Stage 1
  network/disk defaults in place unless the caller overrides them.

### 8.2 Cluster construction

```go
s, err := sim.New(cfg)
if err != nil {
	return nil, err
}
ids := make([]sim.NodeID, nodeCount)
for i := range ids {
	ids[i] = sim.NodeID(i + 1)
}
clientID := sim.NodeID(nodeCount + 1)
recorder := newApplyRecorder(ids)

for _, id := range ids {
	peers := all IDs except id, ascending
	factory := raft.NewFactory(raft.Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  3,
		ElectionTimeoutMin: 12,
		ElectionTimeoutMax: 24,
		MaxAppendEntries:   64,
		Apply: func(e raft.Entry) {
			recorder.Apply(id, e)
		},
	})
	if err := s.AddNode(id, factory); err != nil {
		return nil, err
	}
}

if err := s.AddNode(clientID, newClientFactory(clientID, ids)); err != nil {
	return nil, err
}
s.RegisterInvariant(NewElectionSafety(ids))
s.RegisterInvariant(NewLogMatching(ids))
s.RegisterInvariant(NewStateMachineSafety(recorder))
s.RegisterInvariant(NewCommitDurability(ids, recorder))
if err := s.Start(); err != nil {
	return nil, err
}
if err := ScheduleRaftChaos(s, RaftChaosEnd); err != nil {
	return nil, err
}
```

The `Apply` closure captures the node ID by value; there is no shared mutable
Raft state. The recorder is only written by apply callbacks and read by
invariants, all on the simulator goroutine.

### 8.3 Client actor

`clientNode` implements `sim.Node` and `sim.Snapshotter`:

```go
type clientNode struct {
	env       sim.Env
	id        sim.NodeID
	peers     []sim.NodeID
	seq       uint64
	sent      uint64
	accepted  uint64
	committed uint64
	leaderID  sim.NodeID
	term      uint64
}
```

- `Start`: schedule the first request at `IntRange(5, 15)`.
- `HandleIO` is a no-op; the client never reads or writes its disk.
- Request timer:
  1. If `env.Now() >= RaftClientStop`, stop and send nothing. The last 20 ticks
     of the default 400-tick run are reserved for replication, commit, and
     apply of commands already accepted.
  2. `seq++`.
  3. Choose a target: if `leaderID != 0` and `Intn(4) != 0`, use `leaderID`;
     otherwise choose `peers[IntRange(0, len(peers)-1)]`.
  4. Send `raft.KindClientRequest` with
     `raft.EncodeClientRequest(raft.ClientRequest{ClientID: id, Seq: seq,
     Data: []byte(fmt.Sprintf("cmd-%d", seq))})`.
  5. `sent++`.
  6. Schedule the next request at `IntRange(15, 35)`, clamped to
     `RaftClientStop`. The clamped timer fires at the stop tick and returns
     without sending, so the client's last request is strictly before
     `RaftClientStop`.
- `HandleMessage` for `raft.KindClientResponse`:
  - Decode with `raft.DecodeClientResponse`; a decode error panics as an
    internal bug.
  - If `resp.Term > term`, update `term` and clear `leaderID` before applying
    the hint below. This prevents a delayed response from an old term from
    overwriting a newer leader hint.
  - If `resp.Term == term` and `resp.LeaderID != 0`, update `leaderID`.
  - If `resp.Accepted`, increment `accepted`.
  - If `resp.Committed`, increment `committed`. A committed response is valid
    even if its term is now old; the entry is durably committed.
- `HandleTimer` ignores any event whose tag is not the client request tag.
- The client does not retry individual commands in Stage 2. A new command is
  sent on the next timer. If a command is accepted but never committed because
  leadership changes, it may be overwritten; Stage 4 adds client sessions and
  deduplication.
- `Snapshot` is a canonical string of `id, peers (ascending), seq, sent,
  accepted, committed, leaderID, term`.
- The client has an unused simulator disk. It never reads or writes it.

### 8.4 Chaos driver

The chaos driver starts at tick 30 (after the first election attempt is
underway) and repeats every `IntRange(25, 60)` ticks until `RaftChaosEnd`. Each
action draws one `Intn(100)` and performs exactly one of:

```go
func ScheduleRaftChaos(s *sim.Sim, chaosEnd sim.Tick) error
```

`ScheduleRaftChaos` schedules the first driver action at tick 30 and the
finalizer at `chaosEnd`. If `chaosEnd <= 30`, it schedules only the finalizer.
The driver never schedules another action at or after `chaosEnd`.

| Roll | Action |
|---:|---|
| 0–24 | Crash a random live Raft node, unless only one Raft node is live; then restart a down node instead |
| 25–44 | Restart a random down Raft node, if any |
| 45–59 | Isolate a random live Raft node |
| 60–74 | Partition the Raft nodes into two non-empty groups at a random cut |
| 75–84 | `HealAll` |
| 85–94 | `SetDropPermille(IntRange(0, 200))` |
| 95–99 | Set latency to `min = IntRange(1, 3)`, `max = min + IntRange(0, 2)` |

Selection is deterministic and always uses ascending ID lists:

- `live` and `down` are built by scanning `1..nodeCount` in ascending order.
- Crash: `live[IntRange(0, len(live)-1)]`, unless `len(live) <= 1`.
- Restart: `down[IntRange(0, len(down)-1)]`, unless `len(down) == 0`.
- Isolate: `live[IntRange(0, len(live)-1)]`.
- Partition: `cut := IntRange(1, nodeCount-1)`; groups are
  `[1..cut]` and `[cut+1..nodeCount]`. The groups include down nodes, so the
  block survives a restart and is cleared by `HealAll`.
- Latency: draw `min` first, then the extra, as shown in the table.

The client is never crashed and is never included in a partition group. An
isolated Raft node also becomes unreachable from the client, and the global drop
rate applies to client-to-Raft messages as well, but the client always has five
targets and retries on its next timer. Only Raft-to-Raft links are partitioned.
This keeps client command generation independent of cluster chaos and avoids a
sixth failure domain in Stage 2.

Variable latency is the delay/reordering mechanism: messages sent at the same
tick can arrive in a different order, and a latency increase delays in-flight
messages. Explicit per-message duplication/corruption is out of scope. Drops
come from the global per-mille setting; partitions come from blocked directed
links. All three fault classes are therefore exercised without changing
`internal/sim`.

### 8.5 Finalization and quiescence

At the fixed `RaftChaosEnd` tick (200), the finalizer:

1. `HealAll()`.
2. `SetDropPermille(0)`.
3. `SetLatency(1, 3)`.
4. Restart every down Raft node in ascending ID order.
5. Stop the chaos driver.

The client continues sending commands until the fixed `RaftClientStop` tick
(380), then stops so the last 20 ticks of the default 400-tick run can drain
in-flight work. Because the stop time does not depend on `end`, replaying with
`-ticks <failure-tick>` preserves the client prefix. In a normal run with
`end = 400`, the last 200 ticks are fault-free, so a leader should be elected,
the no-op committed, and client commands replicated and committed. Tests assert
the final quiescent properties; the scenario itself does not fail on temporary
lack of a leader, because liveness is not required during chaos.

### 8.6 Final-state assertions (tests, not invariants)

For the default seed 42 run at `end = 400`:

- All Raft nodes are up.
- Exactly one node has `Role == Leader`.
- Every live node has the same `Term` as the leader.
- Every live node has the same `CommitIndex` as the leader, and for every index
  `1..CommitIndex` its log entry matches the leader's. Uncommitted suffixes may
  differ in length: a follower can retain entries from an old leader that the
  current leader never overwrites because it only truncates on a conflicting
  `AppendEntries`. Raft does not require uncommitted logs to be identical.
- `LastApplied == CommitIndex` on every node.
- The client has `sent > 0` and `accepted > 0`.
- The committed log contains at least one `EntryCommand`, and every node has
  applied it. (The client's `committed` counter is a bonus observation; a
  command can be committed by a new leader after the accepting leader stepped
  down, so the test does not require a committed response for every command.)
- The apply recorder has no conflict and every node's applied prefix matches
  the committed log.

Other seeds assert the safety invariants and final quiescence where the
fault-free window is long enough; the multi-seed test treats a missing final
leader as a failure after the full 200-tick settle.

---

## 9. Invariants

All invariants are registered in a fixed order, run after every simulator step,
and run once at the end. They must not draw RNG, send messages, mutate Raft or
simulator state, or depend on map iteration order. They may maintain their own
history across steps. Live nodes are read through `Node.View()`; down nodes are
skipped for log comparison, but their previously observed durable commitments
are remembered. A live node with `View.Recovering == true` is also skipped until
its boot reads complete; otherwise a restart would momentarily look like lost
durable state.

Each invariant reads nodes through a small `viewSource` function. Production
passes a function that resolves a `sim.NodeID` through `sim.Sim.Node`, asserts
the exported read-only interface, and calls `View`; invariant unit tests pass a
synthetic map of `View` values. This keeps the safety logic testable without
constructing a broken production node.

```go
type raftViewer interface{ View() raft.View }

func liveRaftView(s *sim.Sim, id sim.NodeID) (raft.View, bool) {
	node, up := s.Node(id)
	if !up {
		return raft.View{}, false
	}
	viewer, ok := node.(raftViewer)
	if !ok {
		panic(fmt.Sprintf("node %d does not implement View()", id))
	}
	return viewer.View(), true
}
```

`internal/sim` never imports `internal/raft`; the assertion lives in
`internal/scenarios`, which imports both packages.

### 9.1 `raft.election-safety`

Maintains:

```go
leadersByTerm   map[uint64]sim.NodeID
lastDurableTerm map[sim.NodeID]uint64
lastDurableVote map[sim.NodeID]sim.NodeID
lastTerm        map[sim.NodeID]uint64
lastIncarnation map[sim.NodeID]uint64
```

For each live Raft node, in ascending ID order:

1. If `Incarnation` changed, reset `lastTerm[id]` to the recovered `Term`
   before checking monotonicity.
2. `Term >= DurableTerm` and `DurableTerm` never decreases across incarnations.
3. Within an incarnation, `Term` never decreases.
4. If `DurableTerm == lastDurableTerm[id]`, then `DurableVotedFor` may only stay
   the same or go from `0` to one candidate; it may never change from one
   non-zero candidate to another. A higher durable term resets the record.
5. If `Role == Leader`:
   - `Term >= 1`.
   - `DurableTerm == Term`.
   - `DurableVotedFor == ID`.
   - `leadersByTerm[Term]` is either unset or `ID`. If it is another node,
     report both node IDs and the term.

The map is never cleared, so a leader that later crashes cannot be replaced by
a second leader in the same term. This is stronger than checking only the
current step and directly enforces Election Safety over the whole run.

### 9.2 `raft.log-matching`

For each live Raft node:

1. `View.Log[0]` is the sentinel with index 0 and term 0.
2. For `i >= 1`: `Log[i].Index == i`; terms are non-decreasing; entry kind and
   data caps hold.
3. `CommitIndex <= LastLogIndex`.
4. `CommitIndex <= DurableLastIndex`.
5. `LastApplied <= CommitIndex`.

`DurableLastIndex` may be greater than `LastLogIndex` while a
truncate-and-append batch is pending; that is not a violation, because the old
durable suffix remains on disk until the batch sync completes.

For every pair of live nodes `(A, B)` with `A.ID < B.ID`:

```go
mismatch := false
limit := min(A.LastLogIndex, B.LastLogIndex)
for i := 1; i <= limit; i++ {
    a, b := A.Log[i], B.Log[i]
    sameTerm := a.Term == b.Term
    sameEntry := sameTerm && a.Kind == b.Kind && bytes.Equal(a.Data, b.Data)
    if sameTerm && (mismatch || !sameEntry) {
        return fmt.Errorf("Log Matching violated: nodes %d and %d share term %d at index %d but prefixes differ", ...)
    }
    if !sameEntry {
        mismatch = true
    }
}
```

The `mismatch` flag implements the “same index and term implies identical
preceding log” property without an O(L²) inner loop. The check is
O(N² · L) per step; Stage 2 logs are small. If profiling shows this dominates
the 10,000-seed target, the implementation may add an incremental cache keyed by
`(node, incarnation, lastLogIndex)`, but the semantics above are mandatory.

### 9.3 `raft.state-machine-safety`

The apply recorder stores the first applied identity for each index:

```go
type entryIdentity struct {
	Term uint64
	Kind raft.EntryKind
	Data string // hex-encoded; canonical
}

type applyRecorder struct {
	applied map[uint64]entryIdentity
	err     error
}

func (r *applyRecorder) Apply(node sim.NodeID, e raft.Entry) {
	id := identityOf(e)
	if prev, ok := r.applied[e.Index]; ok {
		if prev != id && r.err == nil {
			r.err = fmt.Errorf("State Machine Safety violated at index %d: node %d applied %v, previously %v", ...)
		}
		return
	}
	r.applied[e.Index] = id
}
```

The invariant:

1. Returns `recorder.err` if set.
2. For each live node, requires `LastApplied <= CommitIndex`.
3. Requires that every index in `1..LastApplied` has a recorded identity. The
   Raft apply loop is contiguous, so a gap indicates a bug.

Reapplying the same entry after a restart is allowed and idempotent. Applying a
different entry at the same index is the safety violation.

### 9.4 `raft.commit-durability`

Maintains:

```go
committed          map[uint64]entryIdentity // first observed committed entry
maxCommitted       uint64
nodeMaxCommit      map[sim.NodeID]uint64
nodeCommitSeen     map[sim.NodeID]uint64
nodeIncarnation    map[sim.NodeID]uint64
leaderVerified     map[sim.NodeID]leaderCheck
```

For each live Raft node, in ascending ID order:

1. If the incarnation changed since the last check, reset
   `nodeCommitSeen[id] = 0` and require `LastLogIndex >= nodeMaxCommit[id]`; for
   every index `1..nodeMaxCommit[id]`, the node's log entry must equal
   `committed[index]`. This catches a restart that lost a previously committed
   entry and makes `nodeCommitSeen` a per-incarnation high-water mark for the
   monotonicity check.
2. For each index `i` from `nodeCommitSeen[id]+1` to `CommitIndex`:
   - require `i <= LastLogIndex`;
   - if `committed[i]` is unset, set it from `Log[i]`;
   - if set and different, report a committed-entry conflict.
   - update `nodeCommitSeen[id]` and `nodeMaxCommit[id]`.
3. `CommitIndex` never decreases within an incarnation.
4. If the node is a leader, require `LastLogIndex >= maxCommitted` and every
   index `1..maxCommitted` matches `committed`. Cache the check by
   `(incarnation, term, LastLogIndex, maxCommitted)` so it is not repeated every
   step.

This enforces both “committed entries never disappear” and “every future leader
contains every committed entry” (Leader Completeness). Combined with Log
Matching and State Machine Safety, it covers the Stage 2 safety requirement.

### 9.5 Invariant failure quality

An invariant error includes:

- invariant name;
- node ID(s) and incarnation(s);
- term, commit index, last log index;
- the conflicting index, local term/kind/data, and other term/kind/data;
- the simulator failure wrapper adds seed, scenario, version, step, tick,
  triggering event, trace tail, and replay command.

No invariant prints a Go map or pointer. All lists are sorted by node ID and
index before formatting.

---

## 10. Determinism and replay

### 10.1 Sources of determinism

- The simulator owns the only RNG. Raft draws only for election timeouts, in
  event order.
- Peer lists, vote arrays, `nextIndex`, `matchIndex`, and invariant node lists
  are slices in ascending peer/ID order.
- `pendingClients` is a map keyed by log index. It is only used for keyed
  lookup; on step-down it is replaced with an empty map, never iterated. Its
  contents are encoded in `Snapshot` in ascending index order.
- RPC payloads are fixed big-endian encodings. No JSON, reflection, or map
  iteration.
- `Snapshot` encodes explicit fields with length prefixes and no pointers.
- Trace output is produced only by the simulator: boots, sends, drops, disk
  operations, crashes, restarts, partitions, and scenario events. Raft message
  payloads are summarized by kind and byte length, so raw pointers or map
  order cannot leak.
- The client command string is deterministic (`cmd-<seq>`); no wall clock,
  hostname, or process ID enters the simulation.

### 10.2 Golden trace

- Scenario: `raft`, seed `42`, total end `400` ticks.
- Chaos ends at `200`; the remaining 200 ticks are fault-free. The client sends
  until `RaftClientStop = 380`, and the final 20 ticks drain in-flight work.
- File: `testdata/stage2/raft-seed-42.trace`.
- The trace header is `sim v=1 seed=42 scenario=raft level=0`.
- The test runs the scenario in-process and byte-compares the trace.
- A separate-process test builds `simctl` once and runs it twice with identical
  flags, comparing stdout and the printed `trace-hash`/`state-hash`.
- Regeneration:
  ```sh
  go test ./internal/scenarios -run TestRaftGoldenTraceSeed42 -update
  ```
- The golden file is reviewed as part of the Stage 2 change. It must not contain
  absolute paths, timestamps, or host-specific data.

### 10.3 Multi-seed suite

- `TestRaftMultiSeed` runs seeds `1..N` with the default 400-tick scenario.
- Default `N = 50`, intended to finish well under 15 seconds and run in
  `make test`.
- The test flag `-raft-seeds=N` overrides `N`.
- `make stress-raft SEEDS=10000` runs the same test with `N = 10000` and a
  longer Go test timeout. It is not part of the default suite.
- A failure reports the seed and the exact `simctl` replay command through the
  simulator's existing `Failure` wrapper.

---

## 11. Test plan

### 11.1 Unit tests (`internal/raft`)

| Test | Assertion |
|---|---|
| `TestLogSentinelAndIndexing` | Sentinel at 0; append/lookup/term helpers are contiguous |
| `TestVoteGrantRules` | Grants to an up-to-date candidate; rejects stale log, lower term, and a second candidate in the same term |
| `TestVotePersistsBeforeReply` | Trace order shows meta sync before `vote-resp`; a crash before sync sends no reply |
| `TestVoteSurvivesCrashAfterSync` | Durable term/vote recovered; duplicate request answered consistently |
| `TestTermIncreasePersistsBeforeDenial` | Higher-term request that is denied still persists the term before replying |
| `TestElectionTimeoutDrawsFromEnv` | Timeout in `[min,max]`; repeated resets draw new values; no constructor draws |
| `TestSingleNodeElection` | A one-node cluster elects itself only after its meta sync |
| `TestElectionSmoke` | A small `sim.Sim` cluster elects a leader; at that observation, no other node is leader in the same term and the leader's no-op is durable (2A has no heartbeats, so the cluster need not stay quiescent) |
| `TestReplicationSmoke` | A small `sim.Sim` cluster commits and applies a command on every node |
| `TestCandidateStepsDownOnHigherTerm` | Role, term, vote, and durable state update correctly |
| `TestLeaderNoopPersistedBeforeHeartbeat` | No `append-req` send before the no-op log sync |
| `TestAppendEntriesPrevLogChecks` | Too-short and term-mismatch requests fail with correct conflict hints |
| `TestAppendEntriesAppendsAndPersists` | New entries are durable before success reply |
| `TestAppendEntriesTruncatesConflict` | Conflicting suffix is replaced; truncation and append are one durable batch |
| `TestAppendEntriesDuplicateIsIdempotent` | Duplicate request does not duplicate or truncate entries |
| `TestAppendEntriesNeverTruncatesCommitted` | Defensive panic/failure on a request that would replace a committed entry |
| `TestConflictBacktrackingByTerm` | Leader skips a whole conflicting term and clamps to `matchIndex+1` |
| `TestStaleAppendResponseIgnored` | Old success/failure cannot regress `nextIndex` below `matchIndex+1` |
| `TestCommitRequiresMajority` | Minority leader does not advance commit; majority does |
| `TestCommitOnlyCurrentTerm` | Figure 8: a prior-term entry replicated on a majority is not committed by replica count alone |
| `TestFigure8Integration` | Crafted term/log history cannot produce two applied entries at the same index |
| `TestApplyInOrder` | Apply hook sees every index once per incarnation, in order |
| `TestRecoveryReplaysDurableLog` | Restart reconstructs term/vote/log; commitIndex starts at 0; apply replays as commit advances |
| `TestSnapshotCanonical` | Same logical state produces identical snapshot bytes; no map order |

**Figure 8 construction.** `export_test.go` seeds a term-4 leader whose log has
a term-1 entry at index 1 and a term-2 entry at index 2, with `matchIndex`
showing index 2 on a majority. The focused test calls `advanceCommit` and
asserts that the term-2 entry is not committed by replica count alone: its term
is older than `currentTerm`. It then appends a term-4 no-op at index 3, makes it
durable on a majority, and asserts that `advanceCommit` commits index 3 and,
transitively, indexes 1 and 2 in order. The integration variant scripts the full
Figure 8 (a)–(d) history through `export_test.go` state injection and a
deterministic message sequence, then checks that no two different entries are
ever applied at the same index. This is the specific trap: committing an entry
from an earlier term because it is present on a majority.

### 11.2 Crash and stale-epoch tests

| Test | Fault injection |
|---|---|
| `TestCrashBeforeMetaSync` | Schedule a crash after the meta replace completes but before sync; assert no vote reply and old term/vote after restart |
| `TestCrashBeforeLogSync` | Schedule a crash after the log append completes but before sync; assert no success reply, no `matchIndex` advancement, and old log after restart |
| `TestCrashAfterSyncBeforeNextEvent` | Crash at the next tick after sync; assert the reply was already sent and the durable state is the new one |
| `TestStaleIOCompletionDropped` | Crash while a sync completion is queued; assert the restarted node never sees it |
| `TestStaleMessageFromOldEpoch` | Crash/restart the recipient between send and delivery; assert no delivery to the new incarnation |
| `TestDelayedAppendEntriesAfterNewLeader` | Old-term request arrives after a new election; rejected without truncation |
| `TestDelayedVoteResponse` | Response from an old term/candidate is ignored |

### 11.3 Scenario tests (`internal/scenarios`)

| Test | Assertion |
|---|---|
| `TestRaftElectionSmoke` | A fault-free 5-node cluster built directly with `raft.NewFactory` (no chaos) elects one leader and all live nodes converge to its term |
| `TestRaftReplicationSmoke` | The same fault-free cluster commits client commands and applies them on all nodes |
| `TestRaftSameSeedProducesIdenticalTrace` | Three fresh runs produce identical trace bytes, trace hash, and state hash |
| `TestRaftInterleavedSimsDoNotShareState` | Two sims advanced alternately match the solo run |
| `TestRaftGoldenTraceSeed42` | Byte-exact golden trace |
| `TestRaftSameSeedAcrossProcesses` | Two `simctl` subprocesses produce identical output |
| `TestRaftDifferentSeedsDiffer` | Different seeds produce different hashes |
| `TestRaftMultiSeed` | Seeds `1..N` complete with all invariants intact |
| `TestRaftFinalLeaderAndCommit` | Seed 42 ends with one leader, a common committed prefix on all nodes, and a committed client command applied everywhere |
| `TestRaftCrashRestartConvergence` | Repeated crash/restart of a follower and leader converges after healing |
| `TestRaftInvariantCatchesElectionViolation` | Test-only fake view with two leaders in one term is rejected |
| `TestRaftInvariantCatchesLogMatchingViolation` | Test-only fake views with same index/term and different prefixes are rejected |
| `TestRaftInvariantCatchesStateMachineViolation` | Recorder rejects two different entries at one index |
| `TestRaftInvariantCatchesCommitLoss` | Test-only restart view missing a committed entry is rejected |

The invariant tests may use a small test-only adapter that feeds synthetic
`View` values; they do not require a broken production node.

### 11.4 Test budgets

- Default `go test ./...`: under 20 seconds on the development machine.
- `TestRaftMultiSeed` with 50 seeds: under 15 seconds.
- 10,000-seed stress: allowed several minutes; documented as an explicit
  target, not part of the default suite.
- Golden trace: under 1 second.
- `go test -race ./...`: under 60 seconds.

---

## 12. CLI, Makefile, and documentation changes

### 12.1 `cmd/simctl`

- Accept `-scenario ping` and `-scenario raft`; reject anything else with a
  message listing both.
- Dispatch:
  ```go
  switch *scenario {
  case scenarios.PingScenario:
      s, err = scenarios.NewPing(cfg, 5, sim.Tick(*ticks))
  case scenarios.RaftScenario:
      s, err = scenarios.NewRaft(cfg, scenarios.RaftNodeCount, sim.Tick(*ticks))
  }
  ```
- Keep the existing flags, trace levels, summary, hashes, and failure output.
- `-ticks` is the total run end for both scenarios. For Raft, the chaos schedule
  is fixed at ticks 30..200, so a replay with `-ticks <failure-tick>` reproduces
  the same prefix even when the failure happened before the finalizer.
- Replay commands printed by `sim.Failure` already include seed, scenario, and
  ticks, so they work unchanged for `raft`.

### 12.2 `Makefile`

```make
SEEDS ?= 10000

.PHONY: test test-race vet fmt sim sim-raft stress-raft golden-stage2

sim-raft:
	$(GO) run ./cmd/simctl -seed 1 -scenario raft -ticks 400 -trace

stress-raft:
	$(GO) test ./internal/scenarios -run TestRaftMultiSeed -count=1 \
		-timeout 60m -args -raft-seeds=$(SEEDS)

golden-stage2:
	$(GO) test ./internal/scenarios -run TestRaftGoldenTraceSeed42 -update
```

`make test` remains `go test ./...` and therefore runs the default 50-seed
Raft suite. `make stress-raft` is the explicit 10,000+ target.

### 12.3 `README.md`

- Stage 1 status: `Complete (approved 2026-11-25, commit 0c7d5fb)`.
- Stage 2 status: `Plan under review`; link to `PLAN_STAGE2.md`.
- Architecture table: Stage 1 complete; Stage 2 plan under review; later stages
  unchanged.
- Repository layout: add `PLAN_STAGE2.md`, `internal/scenarios/raft.go`,
  `testdata/stage2/raft-seed-42.trace`; mark `internal/raft` as Stage 2.
- Running: add `make sim-raft`, `make stress-raft`, and the Raft replay command.
- Stage 2 sub-phase summary and gate commands.
- Bug log: append Stage 2 entries as `S2-00N` with seed, scenario, symptom, root
  cause, fix, and regression test. The existing `S1-001` entry stays.
- Stage gates: keep the rule that implementation starts only after plan
  approval and that test output is shown before moving on.

### 12.4 `PLAN.md`

Update the Stage 1 status line to `Complete (approved 2026-11-25)` and add a
one-line pointer to `PLAN_STAGE2.md` for Stage 2. Do not rewrite the approved
Stage 1 design.

---

## 13. Acceptance checklist

- [ ] This plan is approved before any `internal/raft` code is written.
- [ ] 2A gate passes: election, durable vote, crash-before-sync tests.
- [ ] 2B gate passes: replication, conflict backtracking, Figure 8 tests.
- [ ] 2C gate passes: Troublemaker scenario, invariants, golden trace.
- [ ] No changes to `internal/sim`; static API guard passes.
- [ ] No forbidden APIs in `internal/raft` or `internal/scenarios`.
- [ ] Every RPC reply that depends on durable state follows a sync completion.
- [ ] A crash before sync produces no reply and restart recovers the old state.
- [ ] Figure 8 current-term commit rule has a focused unit test and an
      integration test.
- [ ] `testdata/stage2/raft-seed-42.trace` is checked in and byte-exact.
- [ ] `go test ./...`, `go test -race ./...`, and `go vet ./...` pass.
- [ ] Default 50-seed Raft suite passes within budget.
- [ ] `make stress-raft SEEDS=10000` passes.
- [ ] README status, layout, commands, and bug log are updated.
- [ ] Results are presented and approval is requested before Stage 3.

---

## 14. Implementation order

1. **2A foundation**: package skeleton, roles, entries, RPC encoding, log
   helpers, meta/log file formats, recovery, serialized persistence, election
   timer, RequestVote, no-op leader entry. Write unit tests and the election
   smoke test.
2. **2A durability**: crash-before-meta-sync, crash-after-sync, stale epoch,
   duplicate vote, and term monotonicity tests. Run the 2A gate.
3. **2B replication**: AppendEntries, conflict backtracking, follower truncation,
   success gating, leader `nextIndex`/`matchIndex`, commit advancement, apply
   hook, client request handling.
4. **2B safety**: Figure 8 unit/integration tests, committed-truncation
   defenses, stale/delayed response tests, majority commit tests. Run the 2B
   gate.
5. **2C scenario**: client actor, chaos driver, finalizer, apply recorder,
   invariants, CLI dispatch, Makefile targets.
6. **2C verification**: determinism, interleaving, golden trace, cross-process,
   default multi-seed, 10,000-seed stress.
7. **Documentation and review**: README/PLAN updates, bug log, full command
   output, stop for approval.

---

## 15. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Serialized inbox/persistence reduces liveness under chaos | Election timeout 4–8× heartbeat; 200-tick quiescence; tests assert final convergence, not mid-chaos liveness |
| Log file grows without compaction | 4 MiB cap with deterministic failure; Stage 2 runs are short; Stage 3 adds snapshots/compaction |
| Invariant cost dominates 10,000 seeds | O(N²·L) with small logs; optional incremental caches; stress target is explicit, not default |
| Figure 8 test is hard to construct | Test-only `export_test.go` state injection plus a focused `advanceCommit` unit test |
| Crash points are hard to hit | Disk latency 1–2 ticks; schedule crashes between append and sync; assert trace order; no separate reply gap exists |
| Map iteration changes behavior | Slices for peers/votes/indexes; maps only keyed lookups; sorted iteration on clear/report |
| Golden trace becomes stale | Explicit `-update` target; review diff; trace is small and deterministic |
| A later bug invalidates a seed | Append `S2-00N` to README bug log with seed, root cause, fix, regression test |
| Simulator primitive turns out to be missing | Stop and propose a Stage 1 amendment for approval; do not silently change the substrate |

---

## 16. Decisions requiring explicit approval

1. **Serialized actor inbox and one persistence batch at a time.** This makes
   “durable before reply” exact and crash reasoning simple, at the cost of
   blocking message processing during a 2-operation disk batch. Election
   timeouts are set well above that latency.
2. **Separate `raft.meta` and append-only `raft.log`.** Meta is fixed-size
   replace+sync; log batches are append+sync with truncation records. No log
   compaction in Stage 2.
3. **`commitIndex` is volatile.** It is not persisted; a restarted node learns
   it from a leader. Durable log entries are what matter for safety.
4. **No-op entry on election.** The leader appends and persists a current-term
   no-op before heartbeating, enabling safe commit of prior-term entries under
   Figure 8.
5. **No changes to `internal/sim`.** The scenario uses existing crash, restart,
   partition, drop, latency, disk, and invariant APIs. If a primitive is
   missing, work stops for a Stage 1 amendment.
6. **Client actor outside the Raft peer set.** It exercises commands and
   redirects without becoming a voting member or a sixth failure domain.
7. **Invariants inspect live nodes through `View` and remember history.** This
   avoids shared mutable Raft state and still catches safety violations across
   crashes and restarts.
8. **Golden trace at seed 42, end tick 400, chaos end 200.** The final 200 ticks
   are fault-free and should produce a stable, reviewable trace; the client
   stops at 380 so the last 20 ticks drain.
9. **Default 50-seed suite; 10,000-seed target is explicit.** This keeps
   `make test` fast while preserving the requested stress capability.

---

## 17. Verification commands for the approval review

After implementation, the complete Stage 2 verification is:

```sh
gofmt -l internal/raft internal/scenarios cmd/simctl
go test ./...
go test -race ./...
go vet ./...
go test ./internal/scenarios -run TestRaftSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestRaftGoldenTraceSeed42
go test ./internal/scenarios -run TestRaftSameSeedAcrossProcesses
go test ./internal/scenarios -run TestRaftMultiSeed -count=1
make stress-raft SEEDS=10000
go run ./cmd/simctl -seed 42 -scenario raft -ticks 400 -trace
go run ./cmd/simctl -seed 42 -scenario raft -ticks 400
```

Expected at approval time:

- `gofmt -l` prints nothing.
- All tests pass; the default multi-seed suite meets its time budget.
- The 10,000-seed stress target completes without a safety or structural
  invariant failure.
- The seed-42 run prints the checked-in `trace-hash` and `state-hash`.
- The golden trace is byte-identical.
- No `internal/raft` code exists before this plan is approved.

Stage 2 is complete only after these results are shown and the user approves
moving to Stage 3.

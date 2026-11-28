# Stage 3 — Deterministic WAL + LSM Storage Engine

**Status:** Proposed 2026-11-28, awaiting approval.
**Date:** 2026-11-28.
**Scope:** A deterministic, crash-safe write-ahead log and LSM-tree storage
engine in `internal/storage`, a `storage` chaos scenario, per-step oracle
invariants, a reviewed golden trace, and a 10,000-seed stress target.
**Depends on:** Stage 2 complete and approved (commit `c8bac13`).
**Approval gate:** No `internal/storage` implementation code is written until
this plan is approved. Implementation then proceeds one sub-phase at a time:
3A, gate; 3B, gate; 3C, gate.

---

## 1. Starting point and non-negotiable constraints

Stage 3 builds directly on the approved Stage 1 simulator. The simulator owns
the clock, event queue, network, per-node disks, RNG, trace, hashes, lifecycle,
and invariant checks. The storage engine is a passive actor driven by simulator
callbacks. It never performs real I/O.

The implementation must satisfy all of the following:

1. `internal/storage` imports only `internal/sim` and the Go standard library.
   It must never import `internal/raft`, `internal/scenarios`, `internal/kv`,
   `cmd/simctl`, `os`, `syscall`, `time`, `math/rand`, `crypto/rand`, or `net`.
2. `internal/sim` must never import `internal/storage`. The existing AST guard
   in `internal/sim/static_test.go` already scans `internal/storage`; it must
   keep passing.
3. No goroutines, channels, `select`, wall clocks, real timers, real
   networking, external RNG, environment reads, or real filesystem access in
   simulated code.
4. All randomness comes from `sim.Env` in event order. The storage engine
   itself draws no randomness; the scenario client and chaos driver draw it.
5. No map iteration may influence behavior, encoding, trace output, or state
   hashes. Every map traversal that affects output is sorted first.
6. A write is acknowledged only after the asynchronous WAL `Sync` completion
   has been handled. A crash before that completion means no acknowledgement
   and no durable write.
7. The manifest is the commit point for flush and compaction. New SSTables are
   durable before a manifest references them. Old SSTables and WAL records are
   logically retired only after the new manifest is durable.
8. Same seed + same binary + same config = byte-identical trace and final
   state. Stage 3 does not change Stage 1 or Stage 2 semantics; `sim.Version`
   remains `1`. Storage formats carry their own version fields.
9. Stage 3 adds no Raft integration, KV client sessions, request
   deduplication, snapshots, or SQL. Stage 4 owns those.

### 1.1 Existing simulator surface used by storage

```go
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

The relevant simulator guarantees are:

- Each node has a private flat disk namespace. File names are opaque strings
  with no tabs or newlines. There are no directories.
- Disk operations from one node are serialized FIFO by `busyUntil`; completion
  is delivered through `HandleIO(IOEvent{Tag, Op, Data, Err})`.
- `Read` returns a prefix of the *visible* bytes. `Append` extends visible
  bytes. `Replace` atomically replaces visible bytes. `Sync` copies every
  visible file to durable bytes. `List` returns sorted `name\tlength\n` lines
  for visible files.
- A crash reverts every visible file to its durable bytes and increments the
  node epoch. Completions from an old epoch are dropped without a callback.
- A restart constructs a fresh `Node` from its factory. Only durable disk bytes
  survive.
- There is no delete or truncate operation. A file can be shrunk with
  `Replace`, but it cannot be removed from the namespace.

### 1.2 Design decisions requiring approval

These choices keep Stage 3 deterministic and focused. They are part of the
plan and may be changed before approval.

1. **Reads use an in-memory table cache.** Live SSTables are decoded into
   memory at recovery and after each flush/compaction. `Get` is a synchronous
   function of active memtable, immutable memtable, then cached SSTables
   newest-to-oldest. Stage 3 does not issue a disk `Read` per `Get`. The
   on-disk SSTable format, checksums, and parse path are still exercised on
   every recovery and by every flush/compaction test. Block-cache and
   disk-resident reads are deferred.
2. **One in-flight disk pipeline per storage node.** WAL append/sync, flush,
   compaction, and manifest writes are serialized. Writes that arrive while the
   pipeline is busy are queued FIFO. Reads are served from memory and do not
   queue behind disk work. This makes crash points explicit and testable.
3. **Full compaction only.** Compaction merges *all* live SSTables into one
   table (or into no table if every key is deleted). Tombstones may be dropped
   only because every older table is included. Partial/leveled compaction is a
   later optimization.
4. **Single manifest file.** `MANIFEST` is rewritten with `Replace` and made
   durable with `Sync`. The simulator's `Replace` is atomic and a crash before
   `Sync` reverts to the previous durable manifest, so no A/B fallback is
   needed. The manifest carries a generation and CRC. A corrupt manifest is a
   hard recovery error, not a silent fallback.
5. **Logical retirement, no physical deletion.** The manifest's live-table set
   and `lastFlushedSeq` define what is live. Old SSTables and WAL segments are
   ignored after the manifest commit; they are not physically deleted because
   the simulator has no delete API. This is safe and keeps recovery simple.
   WAL tail truncation is the one exception: a corrupt/torn tail is rewritten
   with `Replace(segment, validPrefix)` so future appends remain replayable.
6. **No client request deduplication.** The scenario client sends one request
   at a time and resolves an in-flight write after a crash by probing its keys.
   Duplicate request IDs and idempotence belong to Stage 4.
7. **Applied index is carried in the WAL batch.** Each batch may carry a
   Stage 4 `appliedIndex`. The WAL record makes the data and index atomic. The
   manifest records a durable lower bound. Recovery restores
   `max(manifest.appliedIndex, replayed appliedIndex)`. This is the Stage 4
   integration hook; Stage 3 does not implement Raft apply.

### 1.3 Deliberate Stage 3 non-goals

- No Raft integration, leader apply, linearizable reads, or client sessions.
- No request deduplication, sequence-number caches, or transaction IDs.
- No snapshots, log compaction, membership changes, or sharding.
- No partial/leveled compaction, bloom filters, compression, or block cache.
- No real filesystem, real time, goroutines, or multi-process storage.
- No changes to `internal/sim`. If a missing simulator primitive is
  discovered, work stops and a Stage 1 amendment is proposed for approval.

---

## 2. Sub-phases and approval gates

Stage 3 is split into three independently testable sub-phases. Each ends with
a test run and a review checkpoint. Work does not proceed to the next
sub-phase until the previous one passes.

### 3A — WAL and crash recovery

Deliverables:

- `internal/storage` package skeleton: `Op`, `Batch`, `Config`, `Phase`,
  `View`, request/response codec, and the actor lifecycle.
- Canonical binary WAL record framing with magic, version, flags, payload
  length, batch sequence, optional applied index, ordered `Put`/`Delete` ops,
  and CRC32.
- WAL-only engine state: an in-memory sorted key/value state rebuilt from the
  WAL. The full memtable module lands in 3B; 3A does not write SSTables or a
  manifest.
- One-in-flight WAL append/sync pipeline. A write batch is applied to memory
  and acknowledged only in the `Sync` completion callback.
- Restart replay: scan WAL segments in ascending ID, apply valid records in
  order, reject malformed records, and truncate a torn/corrupt/unsynced tail.
- Applied-index recovery and the `OnDurable` hook.
- Tests for framing, CRC, batch atomicity, crash before/after append and sync,
  stale epoch callbacks, truncation, and deterministic replay.

> 3A implements the WAL-only subset of the final recovery pipeline: `List`, WAL
> scan/truncation, and replay. The manifest and SSTable steps in section 7.2
> are added in 3B.

Acceptance:

- No acknowledgement is sent before the WAL sync completion.
- A crash before the sync completion loses the batch entirely; a crash after
  the sync completion preserves it atomically.
- Replay reconstructs exactly the acknowledged state; duplicate replay is
  idempotent.
- A torn or corrupt WAL tail is truncated at the last valid record, and the
  valid prefix is rewritten durably before new writes are accepted.
- A stale-epoch completion is never applied to the new incarnation.

Gate 3A:

```sh
go test ./internal/storage -count=5
go test ./...
go vet ./...
```

### 3B — Memtable, SSTables, tombstones, and compaction

Deliverables:

- Deterministic sorted memtable with upsert, binary-search lookup, ordered
  iteration, byte/entry accounting, and tombstone support.
- Active and immutable memtables; a deterministic flush threshold.
- Immutable SSTable format: sorted unique entries, data blocks, an index block,
  per-block CRC32, header CRC, content hash, and min/max sequence metadata.
- `MANIFEST` format: generation, `lastFlushedSeq`, `appliedIndex`, live table
  metadata, next file IDs, and CRC.
- Flush pipeline: freeze, write SSTable, sync, write manifest, sync, install.
- `Get` path: active memtable, immutable memtable, then live SSTables
  newest-to-oldest, stopping at the first tombstone or value.
- Full compaction that resolves shadowed keys and drops tombstones only when
  every older table is included.
- Recovery that loads the manifest, validates every live SSTable, replays only
  WAL records with `batchSeq > lastFlushedSeq`, and rebuilds the logical state.
- Crash tests at every flush/compaction/manifest phase.

Acceptance:

- Every acknowledged write survives any crash before, during, or after flush
  and compaction.
- A crash at any tick leaves a state recoverable from the last durable
  manifest plus WAL; no acknowledged data is lost and no state is corrupted.
- Tombstones prevent deleted keys from resurrecting after flush or compaction.
- A crash after SSTable sync but before manifest sync leaves an orphan table
  that recovery ignores; the WAL still replays the data.
- A crash after manifest sync but before the in-memory install loads the new
  manifest and skips the flushed WAL records.
- Manifest generation, `lastFlushedSeq`, and `appliedIndex` never go backwards.

Gate 3B:

```sh
go test ./internal/storage -count=5
go test -race ./...
go vet ./...
```

### 3C — Storage chaos scenario, oracle invariants, and stress

Deliverables:

- `internal/scenarios/storage.go`: one storage node, one non-voting client
  actor, a deterministic mixed `Put`/`Get`/`Delete` workload, a phase-aware
  crash/restart driver, and a finalizer.
- `storage` scenario wired into `cmd/simctl` as
  `-scenario storage -seed <N> -ticks <T> -trace`.
- Oracle in `internal/storage/oracle.go` and per-step invariants in
  `internal/storage/invariant.go`.
- `testdata/stage3/storage-seed-42.trace` golden trace.
- Deterministic replay tests (`-count=5`), cross-process determinism, a fast
  multi-seed test in the default `go test ./...`, and a 10,000-seed stress
  target.
- Targeted negative tests for each invariant.
- Makefile targets `sim-storage`, `stress-storage SEEDS=10000`, and
  `golden-stage3`.
- README status, commands, and Stage 3 bug-log instructions.

Acceptance:

- Every acknowledged `Put`/`Delete` survives all later flushes, compactions,
  and crashes.
- An unacknowledged in-flight batch across a crash appears atomically in full
  or not at all.
- Every `Get` response matches the oracle state at response time.
- Tombstones never resurrect a deleted key after compaction.
- On-disk checksums and manifests remain consistent across arbitrary restarts.
- The default multi-seed suite passes quickly; the 10,000-seed target completes
  without a safety or structural violation.

Gate 3C:

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/storage -count=5
go test ./internal/scenarios -run TestStorageSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestStorageGoldenTraceSeed42
go test ./internal/scenarios -run TestStorageSameSeedAcrossProcesses
go test ./internal/scenarios -run TestStorageMultiSeed -count=1
make stress-storage SEEDS=10000
go run ./cmd/simctl -seed 42 -scenario storage -ticks 400 -trace
```

---

## 3. Package layout

```text
internal/storage/
  doc.go            # package overview, format versions, invariants
  state.go          # Op, Batch, Config, Phase, View, Entry, TableInfo
  codec.go          # client request/response binary encode/decode
  wal.go            # WAL record framing, CRC, segment scan, truncation
  memtable.go       # sorted memtable, upsert, lookup, iteration, accounting
  sstable.go        # SSTable encode/decode, data blocks, index, checksums
  manifest.go       # MANIFEST encode/decode, validation, generation
  node.go           # actor lifecycle, messages, write/read paths, tags
  recovery.go       # List -> manifest -> tables -> WAL replay pipeline
  flush.go          # freeze, SSTable write, manifest commit, install
  compact.go        # full compaction, tombstone dropping, manifest commit
  oracle.go         # Stage 3C oracle and possible-world checks
  invariant.go      # Stage 3C per-step invariants and ViewSource
  *_test.go
  export_test.go    # test-only phase helpers and fault-injection hooks

internal/scenarios/
  storage.go        # storage node, client actor, chaos driver, finalizer
  storage_test.go   # determinism, golden, cross-process, multi-seed, coverage

cmd/simctl/
  main.go           # add the storage scenario dispatch

testdata/stage3/
  storage-seed-42.trace
```

`internal/storage` does not import `internal/scenarios`. The scenario imports
both `internal/sim` and `internal/storage`, and reads live nodes through the
exported `View`. The CLI imports only `internal/scenarios` and `internal/sim`.
`internal/sim` imports neither.

---

## 4. Disk layout and file lifecycle

### 4.1 File names

The storage node uses a flat namespace on its private simulated disk:

| Name | Purpose | Mutability |
|------|---------|------------|
| `MANIFEST` | Current committed metadata: generation, flushed sequence, applied index, live tables, next IDs | Rewritten atomically by `Replace`; synced |
| `wal-%06d.log` | Append-only WAL segment, IDs start at 1 | Appended; a torn tail may be rewritten with its valid prefix |
| `sst-%06d.sst` | Immutable SSTable, IDs start at 1 | Written once; never modified after its sync |

Unknown files are ignored and recorded in `View.UnknownFiles` (sorted). A
malformed `wal-` or `sst-` name is a recovery error. The engine never uses a
user-controlled path.

### 4.2 Manifest as the commit point

The manifest is the single source of truth for durable LSM state:

- `lastFlushedSeq` is the highest batch sequence represented in the live
  SSTables. WAL records with `batchSeq <= lastFlushedSeq` are skipped on
  replay.
- `liveTables` lists the SSTables that make up the current LSM state, sorted by
  table ID ascending (oldest to newest).
- `appliedIndex` is a durable lower bound for the Stage 4 applied index.
- `nextSSTID` and `nextWALID` allocate new file IDs without relying on the
  directory listing. At boot they are reconciled with the largest IDs actually
  listed.

A manifest update is committed by:

1. Writing the new manifest bytes with `Replace("MANIFEST", bytes)`.
2. Waiting for the `Replace` completion.
3. Issuing `Sync` and waiting for its completion.

A crash before step 3 completes reverts the manifest to its previous durable
bytes. A crash after step 3 leaves the new manifest durable. The engine never
retires an old table or WAL segment before this commit.

### 4.3 Logical retirement and physical cleanup

Because the simulator has no delete or truncate operation:

- A WAL segment is *logically retired* when every valid record in it has
  `batchSeq <= lastFlushedSeq`. It remains on disk but is skipped on replay.
- An SSTable is *logically retired* when a durable manifest no longer lists it.
  It remains on disk but is ignored by recovery.
- Orphan SSTables from a crash between table sync and manifest sync are ignored.
- The engine does not physically zero retired files in Stage 3. Disk usage grows
  with the number of flushes and compactions, which is acceptable for the
  simulation. A future stage can add a safe cleanup pass; correctness never
  depends on physical deletion.
- The final WAL segment with an invalid tail is the one file that is rewritten:
  the engine replaces it with its valid prefix and syncs before accepting new
  writes. This keeps future appends replayable without needing a delete. An
  invalid tail in a non-final segment is a hard recovery error, not a rewrite.

---

## 5. Binary formats

### 5.1 Conventions

- All integers are big-endian, matching Stage 2.
- `u8`, `u16`, `u32`, `u64` are unsigned. Lengths are `u32` unless stated.
- Byte strings are length-prefixed with `u32`. Keys are compared
  lexicographically by byte value.
- CRC32 is IEEE (`hash/crc32.ChecksumIEEE`) and is stored as `u32`.
- Magic values are four ASCII bytes.
- Limits:

| Limit | Value |
|-------|------:|
| Key length | 1..256 bytes |
| Value length | 0..64 KiB |
| Batch ops | 1..64 |
| Batch payload | 1 MiB |
| WAL segment target | 256 KiB (configurable) |
| WAL segment maximum | 4 MiB |
| SSTable maximum | 8 MiB |
| Manifest maximum | 1 MiB |
| SSTable data block target | 4 KiB |

An encoder rejects out-of-range values. A decoder rejects truncated fields,
unknown versions, unknown flags, and bad CRCs.

### 5.2 WAL record

A WAL record is a self-contained batch:

```text
offset  size          field
0       4             magic "S3WL"
4       2             version = 1
6       2             flags (bit 0 = batch; all other bits zero)
8       4             payloadLen
12      payloadLen    payload
12+len  4             crc32 over bytes [0, 12+payloadLen)
```

Payload:

```text
offset  size          field
0       8             batchSeq (> 0, strictly increasing across replay order)
8       8             appliedIndex (0 = none; otherwise strictly increasing)
16      4             opCount (1..64)
20      ...           ops in application order
```

Op:

```text
offset  size          field
0       1             kind (1 = put, 2 = delete)
1       4             keyLen
5       keyLen        key
5+keyLen 4            valLen (put only; delete must have valLen = 0)
...     valLen        value (put only)
```

Rules:

- A record is atomic: recovery applies all of its ops or none.
- `batchSeq` identifies the batch and is used for flush/compaction shadowing.
- `appliedIndex` is optional. If nonzero, it must be greater than the previous
  nonzero applied index seen during replay. Stage 4 uses this to make its apply
  index atomic with the data.
- A record with `opCount == 0`, an unknown op kind, a zero-length key, an
  oversized key/value, a delete with a value, or a bad CRC is invalid.
- The maximum encoded record is `16 + 1 MiB`.

### 5.3 WAL segments and scanning

- Segment names are `wal-%06d.log`, IDs starting at 1.
- The first append to a new segment creates it. The engine appends one whole
  record per `Append` call, then waits for the append completion, then issues
  `Sync`.
- Before appending, if the current segment is non-empty and
  `currentSize + recordLen > WALSegmentBytes`, the engine starts a new segment
  with `nextWALID++`. A single oversized record is allowed to exceed the target
  in a fresh segment.
- Recovery reads every `wal-*.log` in ascending ID. It scans records in file
  order until EOF or the first invalid record.
- `batchSeq` must strictly increase across the whole replay order (segments
  ascending, records in file order). A decrease is a recovery error.
- A valid record with `batchSeq <= manifest.lastFlushedSeq` is skipped for
  state reconstruction but still validated.
- A valid record with `batchSeq > lastFlushedSeq` is applied to the active
  memtable in order.
- The scanner returns the valid prefix length, the last valid batch sequence,
  and a reason for the tail (`unexpected EOF`, bad magic, bad version, bad
  flags, bad length, bad CRC, bad payload). For the final segment, the engine
  rewrites it to the valid prefix with `Replace` and `Sync` before going live.
- An invalid record in the **final** segment is a torn or unsynced tail: the
  engine truncates it to the valid prefix and syncs before going live.
- An invalid record in a **non-final** segment cannot be produced by a normal
  crash, because a new segment is only started after the previous segment's
  records were synced. It is treated as a hard recovery error. Silently
  discarding later segments could lose acknowledged data, so the engine fails
  the run with a clear diagnostic instead. A dedicated fault-injection test
  asserts this behavior.
- Normal crashes can therefore only leave an unsynced tail in the final
  segment. The recovery note records every truncation and its reason.
- After recovery, new writes start in a fresh segment with
  `nextWALID = max(listedWALID, manifest.nextWALID) + 1`. Old segments are never
  appended to again.

### 5.4 SSTable

An SSTable is immutable and non-empty. It contains sorted, unique keys. A
tombstone is an entry with `kind = 2` and an empty value.

```text
Header prefix (fixed 56 bytes, followed by variable-length keys and index
metadata):
offset  size    field
0       4       magic "S3ST"
4       2       version = 1
6       2       flags = 0
8       8       tableID
16      8       minSeq
24      8       maxSeq
32      8       contentHash (FNV-1a 64 over canonical entries)
40      4       entryCount
44      4       blockCount
48      4       smallestKeyLen
52      4       largestKeyLen
56      ...     smallestKey
...     ...     largestKey
...     8       indexOffset (absolute file offset)
...     4       indexLen
...     4       headerCRC over all header bytes from offset 0 to here
```

Data block:

```text
offset  size    field
0       4       entryCount
4       ...     entries
...     4       crc32 over the block bytes before this field
```

Data block entry:

```text
offset  size    field
0       4       keyLen
4       keyLen  key
...     1       kind (1 = put, 2 = delete)
...     8       seq (batchSeq that wrote the entry)
...     4       valLen (put only; delete must have valLen = 0)
...     valLen  value
```

Index block:

```text
offset  size    field
0       4       blockCount
4       ...     index entries
...     4       crc32 over the index bytes before this field
```

Index entry:

```text
offset  size    field
0       4       firstKeyLen
4       firstKeyLen  firstKey
...     8       blockOffset (absolute)
...     4       blockLen (including its CRC)
...     4       entryCount
```

Encoding rules:

- Entries are sorted by key ascending and unique within the table.
- Data blocks are filled in order until adding the next entry would exceed the
  4 KiB target; an entry larger than the target occupies its own block.
- The index lists every data block in file order.
- `minSeq`/`maxSeq` are the minimum/maximum entry `seq`.
- `smallestKey`/`largestKey` are the first/last entry keys.
- `contentHash` is FNV-1a 64 over the canonical entry encoding: for each entry,
  write `keyLen`, key, `kind`, `seq`, `valLen`, value. It is recomputed by the
  decoder and compared with the header.
- The decoder validates all offsets and lengths against the file size, checks
  every block CRC and the header CRC, verifies sorted unique keys, verifies the
  entry count and key range, and verifies `contentHash`.
- A table with zero entries is not written. An empty compaction result is
  represented by an empty live-table list in the manifest.

### 5.5 Manifest

`MANIFEST` is a single canonical file:

```text
offset  size    field
0       4       magic "S3MF"
4       2       version = 1
6       2       flags = 0
8       8       generation (starts at 1, strictly increases)
16      8       lastFlushedSeq
24      8       appliedIndex
32      8       nextSSTID
40      8       nextWALID
48      4       liveTableCount
52      ...     live table entries, sorted by tableID ascending
...     4       crc32 over all preceding bytes
```

Live table entry:

```text
offset  size    field
0       8       tableID
8       8       minSeq
16      8       maxSeq
24      4       entryCount
28      4       tombstoneCount
32      8       fileSize
40      8       contentHash
48      4       smallestKeyLen
52      smallestKeyLen  smallestKey
...     4       largestKeyLen
...     largestKeyLen  largestKey
```

Validation:

- `generation >= 1`, `lastFlushedSeq >= 0`, `appliedIndex >= 0`,
  `nextSSTID >= 1`, `nextWALID >= 1`.
- Live tables are sorted by ID, unique, and reference existing files with the
  recorded size.
- `lastFlushedSeq` is at least every live table's `maxSeq`.
- The CRC matches.
- A missing or zero-length `MANIFEST` means "fresh engine" only when no
  `sst-*.sst` file exists; otherwise it is a recovery error. A corrupt manifest
  is always a hard recovery error.
- `nextSSTID`/`nextWALID` are reconciled at boot:
  `next = max(manifest.next, maxListedID+1, 1)`.

### 5.6 Client protocol

The scenario client and the storage node exchange deterministic binary
messages. The storage engine does not depend on this codec for durability; it
is the Stage 3 test/scenario API.

Request:

```text
offset  size    field
0       4       magic "S3RQ"
4       2       version = 1
6       1       kind (1 = get, 2 = write)
7       1       flags = 0
8       8       requestID (client-assigned)
16      ...     get: keyLen u32 + key
                write: appliedIndex u64, opCount u32, ops in order
...     4       crc32 over all preceding bytes
```

Write op encoding is the same as the WAL op encoding.

Response:

```text
offset  size    field
0       4       magic "S3RS"
4       2       version = 1
6       1       kind (1 = get, 2 = write, 3 = error)
7       1       flags = 0
8       8       requestID
16      1       found (0/1)
17      8       batchSeq (write) or entry seq (get)
25      8       appliedIndex (engine's durable applied index)
33      4       valueLen
37      valueLen  value (if found)
...     4       errLen
...     errLen  error text (empty on success)
...     4       crc32
```

The decoder validates magic, version, lengths, and CRC. A malformed request
receives a `kind = 3` error response; it never panics the storage node.

---

## 6. Engine state and behavior

### 6.1 Types and configuration

```go
type OpKind uint8

const (
    OpPut    OpKind = 1
    OpDelete OpKind = 2
)

type Op struct {
    Kind  OpKind
    Key   []byte
    Value []byte
}

type Batch struct {
    AppliedIndex uint64
    Ops          []Op
}

type Entry struct {
    Value []byte // nil for a tombstone; non-nil empty slice for a live empty value
    Seq   uint64
}

type Config struct {
    FlushBytes          int
    FlushEntries        int
    WALSegmentBytes     int
    CompactionThreshold int
    MaxBatchOps         int
    MaxKeyBytes         int
    MaxValueBytes       int
    OnDurable           func(Batch)
}

type TableInfo struct {
    ID             uint64
    MinSeq         uint64
    MaxSeq         uint64
    EntryCount     uint32
    TombstoneCount uint32
    FileSize       uint64
    ContentHash    uint64
    SmallestKey    []byte
    LargestKey     []byte
}

type WALInfo struct {
    ID          uint64
    Size        uint64
    MaxBatchSeq uint64
    ValidEnd    uint64
    Truncated   bool
    Reason      string
}
```

Defaults: `FlushBytes = 64 KiB`, `FlushEntries = 1024`,
`WALSegmentBytes = 256 KiB`, `CompactionThreshold = 4`, `MaxBatchOps = 64`,
`MaxKeyBytes = 256`, `MaxValueBytes = 64 KiB`. The scenario overrides the
thresholds to small values so that seed 42 exercises many flushes and
compactions inside 400 ticks.

Config validation rejects non-positive thresholds, `WALSegmentBytes` above the
4 MiB segment maximum, `MaxBatchOps` above 64, `MaxKeyBytes` above 256, and
`MaxValueBytes` above 64 KiB. A nil `OnDurable` is allowed.

`OnDurable` is optional. It is called exactly once per batch, in batch order,
after the WAL sync completion and the memtable apply, before any response is
sent. It must not call back into the storage node or perform I/O. Stage 4 uses
it to advance its own applied state.

An empty value is a live key. `Entry.Value == nil` is reserved for tombstones;
a put with a zero-length value is stored as a non-nil zero-length slice.
Decoders preserve this distinction (allocate an empty slice rather than
returning `nil` for a live empty value). `View.KV` contains only live keys, so
map membership itself is the presence test.

### 6.2 Memtable

The memtable is a sorted slice of `(key, Entry)`:

- `Put` upserts the key with the batch's sequence and value.
- `Delete` upserts a tombstone with the batch's sequence and a nil value.
- Lookup is a binary search by key.
- Iteration is in ascending key order.
- Size accounting is `len(key) + len(value) + 32` bytes per entry. Entry count
  and tombstone count are tracked incrementally.
- Freezing moves the active memtable to `immutable`, creates a new empty active
  memtable, and records the frozen maximum batch sequence.
- A flush writes the immutable memtable to an SSTable, including tombstones.
- The active memtable accepts no new writes while the pipeline is flushing;
  those requests queue. The immutable remains readable during the flush.

### 6.3 Get path

`Get(key)` is synchronous and deterministic:

1. If the node is recovering, queue the request.
2. Search the active memtable. If found:
   - tombstone -> not found;
   - value -> return it.
3. Search the immutable memtable, if present. Same tombstone rule.
4. Search live SSTables from newest to oldest (descending table ID):
   - skip tables whose `[smallestKey, largestKey]` does not contain the key;
   - binary-search the cached entries;
   - first hit wins: tombstone -> not found; value -> return it.
5. Not found.

The response includes `Found`, `Value`, the entry's `Seq`, and the engine's
current durable `AppliedIndex`. A read never waits for the disk pipeline.

### 6.4 Write path

A write request carries a `Batch` of one or more ops. The engine processes one
batch at a time:

1. If recovering or the pipeline is busy, enqueue the request FIFO.
2. Validate the batch. Invalid batches receive an error response.
3. Assign `batchSeq = nextBatchSeq` and increment `nextBatchSeq`.
4. Encode the WAL record. Choose the current WAL segment, rotating first if the
   record would exceed `WALSegmentBytes`.
5. `Append(segment, record)`. Wait for completion.
6. `Sync`. Wait for completion.
7. On sync completion:
   - apply the batch to the active memtable in op order;
   - update `lastAppliedSeq`, `lastDurableSeq`, and `appliedIndex` (if the
     batch carries a nonzero index);
   - update the logical live map;
   - call `OnDurable(batch)` if configured;
   - send the response for the request;
   - if the active memtable crossed a flush threshold, freeze it and start a
     flush;
   - otherwise go idle and process the next queued request.
8. If the append or sync fails, panic with a clear internal error; the
   simulator reports a run failure.

A batch is acknowledged only in step 7. A crash before step 7 means no
acknowledgement. The batch may still be durable if the sync completion ran but
the response did not reach the client; the oracle treats that as an allowed
unacknowledged outcome.

### 6.5 Flush

Triggered after a write batch when the active memtable has at least
`FlushBytes` bytes or `FlushEntries` entries.

1. Freeze the active memtable into `immutable`; create a new active memtable.
2. Encode the immutable entries into an SSTable with a fresh table ID.
3. `Replace(sstName, bytes)`. Wait.
4. `Sync`. Wait. The table is now durable but not yet referenced.
5. Build a new manifest with `generation = committed generation + 1`:
   - add the table to `liveTables`;
   - set the new `lastFlushedSeq = immutable.maxBatchSeq` (the highest batch
     applied to the frozen memtable; this is at least the table's maximum entry
     sequence);
   - set `appliedIndex = max(manifest.appliedIndex, immutable.maxAppliedIndex)`;
   - set `nextSSTID` to the next unused ID.
   Do not advance the in-memory committed generation or `lastFlushedSeq` yet.
6. `Replace("MANIFEST", bytes)`. Wait.
7. `Sync`. Wait. This is the flush commit point.
8. Install the table in the in-memory cache, clear the immutable memtable,
   commit the new generation and `lastFlushedSeq`, update counters, and either
   start a due compaction or process the next queued request.

Crash behavior:

- Before step 4 completes: the table is not durable. The manifest is unchanged.
  Recovery replays the WAL and may re-flush with a new table ID.
- After step 4 but before step 7: the table is a durable orphan. The manifest is
  unchanged. Recovery ignores the orphan and replays the WAL.
- After step 7: the manifest is durable and references the table. Recovery
  loads it and skips WAL records with `batchSeq <= lastFlushedSeq`.
- The engine never acknowledges a write because of a flush; writes are already
  durable in the WAL before they enter the memtable.

### 6.6 Compaction

Triggered after a flush completes when `len(liveTables) >= CompactionThreshold`.
Full compaction merges every live table:

1. Merge all entries by key. For each key, keep the entry with the highest
   `seq`. If two entries have the same `seq`, the entry from the higher table ID
   wins; this should never happen, but the tie-break is deterministic. If the
   winner is a tombstone, drop it. Because every older table is included,
   dropping tombstones cannot resurrect a value. The merge uses sorted table
   iterators or a sorted key slice; it never iterates a Go map.
2. If the result is non-empty, encode a new SSTable with a fresh table ID.
3. `Replace(sstName, bytes)`, `Sync`.
4. Build a new manifest with `generation = committed generation + 1` whose
   `liveTables` is the single new table (or empty), `lastFlushedSeq` unchanged,
   `appliedIndex` unchanged, and `nextSSTID` advanced if a table was written.
   Do not advance the committed generation yet.
5. `Replace("MANIFEST", bytes)`, `Sync`. This is the compaction commit point.
6. Install the new table cache, drop the old tables from memory, commit the new
   generation, and process the next queued request.

Crash behavior mirrors flush: before the new table sync, the old manifest and
tables remain authoritative; between the table sync and manifest sync, the new
table is an orphan; after the manifest sync, the new manifest is authoritative
and old tables are ignored. Compaction never changes the logical live state, so
the oracle must observe the same key/value map before and after.

### 6.7 Applied index and Stage 4 hook

- A `Batch` may carry `AppliedIndex > 0`. The WAL record stores it alongside
  the ops, so data and index are atomic.
- The engine rejects a batch whose nonzero index is not greater than the
  current durable applied index.
- After a WAL sync, `appliedIndex` advances to the batch's index.
- A manifest update records `max(manifest.appliedIndex, flushed applied index)`
  as a durable lower bound.
- Recovery sets `appliedIndex = max(manifest.appliedIndex, max replayed
  appliedIndex)`.
- `View.AppliedIndex` is the recovered/durable value. Stage 4 can read it after
  a restart and resume applying at the next index. The `OnDurable` callback
  provides the in-process signal.

### 6.8 View and Snapshot

`View` is the read-only surface used by invariants and tests:

```go
type View struct {
    ID                 sim.NodeID
    Incarnation        uint64
    Recovering         bool
    Phase              Phase
    Busy               bool
    NextBatchSeq       uint64
    LastAppliedSeq     uint64
    LastDurableSeq     uint64
    LastFlushedSeq     uint64
    AppliedIndex       uint64
    ManifestGeneration uint64
    NextSSTID          uint64
    NextWALID          uint64
    ActiveEntries      int
    ActiveBytes        int
    ImmutableEntries   int
    ImmutableBytes     int
    TombstoneCount     int
    Tables             []TableInfo
    WAL                []WALInfo
    KV                 map[string]Entry // live keys only, copied
    PhaseCounts        []uint64
    Flushes            uint64
    Compactions        uint64
    WALTruncations     uint64
    LastRecoveryNote   string
    UnknownFiles       []string
}
```

`KV` contains only live keys; a tombstone is represented by absence. It is
maintained incrementally on writes and rebuilt from live tables plus WAL during
recovery; flush and compaction do not change it. `TombstoneCount` is the total
number of tombstone entries in the active and immutable memtables plus the live
tables. `Tables` is sorted by table ID ascending. `WAL` is sorted by segment ID
ascending. `PhaseCounts` is indexed by `Phase`. `ManifestGeneration` and
`LastFlushedSeq` are committed (durable) values; pending flush/compaction values
are not exposed and are discarded on a crash. `LastDurableSeq` is the highest
batch whose WAL sync completed (or that is covered by `lastFlushedSeq`);
`LastAppliedSeq` is the highest batch applied to the active memtable. Both are
initialized to `max(lastFlushedSeq, replayed max)` on recovery.

`Snapshot()` returns a canonical binary encoding of the same state, including:

- the storage format version, node ID, incarnation, phase, busy flag, tags,
  sequence counters, manifest generation, and recovery note;
- active and immutable memtable entries in key order (key, kind, seq, value);
- live tables in ID order (metadata and content hash);
- WAL segments in ID order (name, size, max sequence, valid end, truncation);
- queued request IDs and batches in FIFO order;
- a canonical digest of the logical live map.

The encoding writes lengths explicitly and never iterates a map without sorting
keys first. It does not include pointers, goroutine IDs, or wall-clock values.

The node implements `View() View` and `Snapshot() []byte`, matching the Stage 2
pattern. The scenario and invariants resolve live nodes through a `ViewSource`
that type-asserts the node and returns false while it is down or recovering.

---

## 7. Async crash and recovery state machine

### 7.1 One-in-flight I/O pipeline

The storage node has exactly one expected disk completion at a time:

```go
type Phase uint8

const (
    PhaseIdle Phase = iota
    PhaseRecoverList
    PhaseRecoverManifest
    PhaseRecoverTable
    PhaseRecoverWAL
    PhaseRecoverRewrite
    PhaseRecoverInitialManifestWrite
    PhaseRecoverInitialManifestSync
    PhaseWALAppend
    PhaseWALSync
    PhaseFlushWrite
    PhaseFlushSync
    PhaseFlushManifestWrite
    PhaseFlushManifestSync
    PhaseCompactWrite
    PhaseCompactSync
    PhaseCompactManifestWrite
    PhaseCompactManifestSync
    phaseCount
)
```

- `nextTag` increments for every disk operation. `expectedTag` is the tag of the
  operation currently in flight.
- `HandleIO` checks `ev.Tag == expectedTag` and `ev.Op` matches the phase. An
  unexpected tag or operation is an internal bug and panics; the simulator
  converts the panic into a deterministic run failure.
- An `IOEvent.Err` is an internal bug (our names and offsets are valid) and also
  panics.
- The simulator drops stale-epoch completions before they reach the new node.
  The tag check is a second line of defense within an incarnation.
- `PhaseCounts` increments when a phase is entered, so tests can assert that
  crash coverage reached every phase. The manifest phases are split by owner:
  `PhaseFlushManifestWrite`/`PhaseFlushManifestSync` commit a flush, and
  `PhaseCompactManifestWrite`/`PhaseCompactManifestSync` commit a compaction.
  `PhaseRecoverInitialManifestWrite`/`PhaseRecoverInitialManifestSync` cover the
  fresh-manifest write during recovery.

### 7.2 Boot recovery pipeline

`Start()` sets `recovering = true` and issues `List`:

1. **List.** Parse `name\tlength` lines. Classify `MANIFEST`, `wal-*.log`,
   `sst-*.sst`, and unknown files. Sort WAL/SSTable IDs ascending.
2. **Manifest.** If `MANIFEST` exists and is non-empty, `Read` it, decode it,
   validate its CRC, version, generation, table list, and file sizes. If it is
   missing or empty and no SSTable exists, start with a fresh in-memory
   manifest (`generation = 0`, `lastFlushedSeq = 0`, `appliedIndex = 0`, no
   tables). A missing manifest with SSTables present is a hard error.
3. **SSTables.** For each live table in ascending ID, `Read` exactly its listed
   size, decode it, validate all checksums and metadata, and build its in-memory
   entry cache. Any mismatch is a hard error. Apply each table's entries to the
   logical live map in oldest-to-newest order (put stores, delete removes).
4. **WAL.** For each segment in ascending ID, `Read` exactly its listed size and
   scan records:
   - validate the frame and payload;
   - track the segment's valid prefix, max `batchSeq`, and max `appliedIndex`;
   - skip records with `batchSeq <= lastFlushedSeq`;
   - apply records with `batchSeq > lastFlushedSeq` to the active memtable in
     file order, and update the logical live map with the same ops;
   - stop at the first invalid record. If the segment is the final segment,
     mark it for truncation. If it is not the final segment, fail recovery with
     a clear error; later segments must not be silently discarded.
   After all segments are scanned, set `lastAppliedSeq = max(lastFlushedSeq,
   max replayed batchSeq)` and `lastDurableSeq = lastAppliedSeq`; set
   `nextBatchSeq = lastAppliedSeq + 1`.
5. **Truncate.** If the final segment has an invalid tail, replace it with its
   valid prefix and `Sync`. Wait for the rewrite to complete. Record a recovery
   note with the original size, valid prefix size, and reason.
6. **Initial manifest.** If no manifest existed, write a fresh generation-1
   manifest with no tables, `lastFlushedSeq = 0`,
   `appliedIndex = max replayed appliedIndex`, and the next IDs derived from the
   listing. `Replace` + `Sync`. This must complete before the node goes live.
   Persisting the replayed applied index here prevents the durable lower bound
   from regressing if the WAL is later truncated or flushed.
7. **Post-recovery.** If the active memtable exceeds the flush threshold,
   start a flush. Otherwise set `recovering = false`, process queued requests,
   and continue.

Recovery is read-only except for WAL truncation and the initial manifest. A
crash during recovery leaves the previous durable state intact; the next boot
repeats the same pipeline. A crash after the initial manifest sync leaves a
valid manifest; the next boot loads it and replays the WAL again (idempotent).

### 7.3 Crash-point matrix

| Crash point | Durable state | Recovery result | Acknowledged data |
|-------------|---------------|-----------------|-------------------|
| Before WAL append completes | Old WAL | Replay old records | Batch absent, never acked |
| After append, before sync | Old WAL (append reverted) | Replay old records | Batch absent, never acked |
| After sync completion, before ack delivery | WAL includes batch | Replay batch | Batch appears; ack may be in flight, so the client probes |
| During flush, before SSTable sync | WAL includes batch; manifest old | Replay WAL | Acked data survives |
| After SSTable sync, before manifest sync | Durable orphan SSTable; manifest old | Ignore orphan; replay WAL | Acked data survives |
| After manifest sync, before install | Manifest references table | Load table; skip flushed WAL records | Acked data survives |
| During compaction, before new table sync | Old manifest and tables | Load old tables | Acked data survives |
| After new table sync, before manifest sync | Orphan new table; old manifest | Ignore orphan; load old tables | Acked data survives |
| After compaction manifest sync | New manifest references new table | Load new table; ignore old tables | Acked data survives |
| During recovery | Previous durable state | Repeat recovery | Acked data survives |
| During initial manifest write | No manifest or old manifest | Fresh/old manifest plus WAL replay | No writes were served yet |

A crash at any tick after a write was acknowledged must leave the acknowledged
batch in the WAL or in a live SSTable. The manifest ordering rules guarantee
this: a table is durable before the manifest references it, and a WAL segment is
never logically retired until a manifest with a covering `lastFlushedSeq` is
durable.

### 7.4 WAL tail truncation

The scanner distinguishes three cases:

1. **Clean EOF.** Every record is valid. No rewrite.
2. **Unsynced tail.** A partial header or payload at the end. This is the
   normal crash shape if the simulator ever exposes a partial append. The
   engine truncates to the last valid record and syncs the prefix.
3. **Corrupt tail.** A complete-looking record with a bad CRC, bad length, or
   bad payload. The engine truncates at the start of that record. If it is the
   final segment, the valid prefix is preserved. If it is a non-final segment,
   recovery fails: a normal crash cannot corrupt a synced segment, and silently
   dropping later segments could lose acknowledged data.

The rewrite is idempotent: a crash during the rewrite reverts to the old bytes,
and the next boot repeats the scan and rewrite. New writes are not accepted
until the rewrite sync completes.

A WAL record is never partially appended by the engine: one `Append` call
carries one complete record. Torn/corrupt tails are therefore primarily
fault-injection cases; the scanner still handles them because the format and
the requirement demand it. The crash model guarantees that an acknowledged
record was synced before the crash and therefore cannot be part of a torn tail;
a bad CRC on a durable record is media corruption, outside the crash model, and
is treated as a recoverable truncation only when it is in the final segment.
The scenario and unit tests assert the crash-model guarantee separately from
the fault-injection truncation tests.

### 7.5 Manifest ordering rules

These rules are mandatory and are asserted by tests:

1. A new SSTable must be durable (`Replace` + `Sync`) before any manifest
   references it.
2. A new manifest must be durable (`Replace` + `Sync`) before the engine treats
   the new table as live in memory or skips the corresponding WAL records.
3. `lastFlushedSeq` may only increase, and only to the maximum batch sequence
   actually captured in a durable table.
4. `appliedIndex` may only increase, and only to an index actually captured in
   a durable WAL batch or table.
5. `generation` strictly increases on every durable manifest write. The
   in-memory committed generation advances only after the manifest `Sync`
   completion; a pending generation is discarded on a crash.
6. Old tables and WAL segments are retired only logically, after the manifest
   commit. Physical cleanup is not part of the correctness contract.
7. A crash between any two steps leaves either the old manifest or the new
   manifest durable, never a mixture.

---

## 8. Storage chaos scenario and oracle

### 8.1 Actors and timeline

The `storage` scenario has two nodes:

- **Node 1 — storage:** `storage.NewFactory` with small flush/compaction
  thresholds so a 400-tick run exercises many WAL rotations, flushes, and
  compactions.
- **Node 2 — client:** a deterministic non-voting actor that sends one request
  at a time and maintains the oracle.

Timeline:

| Tick | Event |
|-----:|-------|
| 0 | Storage boots, recovers, writes/loads the manifest |
| 5 | Client starts its request loop |
| 20 | Chaos driver starts |
| 220 | Chaos driver stops crashing |
| 290 | Client stops issuing new writes |
| 300 | Finalizer restarts storage if needed and drains uncertainty |
| 380 | Client stops all requests |
| 390 | Final verification: storage up, not recovering, idle, oracle matches |
| 400 | Simulation ends (`DefaultStorageEnd`) |

The chaos driver runs every 10–30 ticks. If the storage is down, it restarts
it. If it is up, the driver may crash it:

- with high probability when `View().Phase` is a mutation phase (WAL append,
  WAL sync, flush write, flush sync, manifest write, manifest sync, compaction
  write, compaction sync);
- with lower probability at any other time, including recovery, to test
  repeated crashes.

When the driver crashes the storage, it notifies the client with the current
phase. The client records the crash phase for coverage and, if it has an
in-flight write, marks that batch uncertain. The driver also sends a
`restarted` notification to the client after each successful restart, which
triggers uncertainty probing.

### 8.2 Client workload and uncertainty resolution

The client is single-request-at-a-time:

- It chooses keys from a fixed key space (for example `key-000`..`key-031`) so
  overwrites, deletes, and tombstones are frequent.
- Operation mix: roughly 60% `Put`, 25% `Delete`, 15% `Get`. About 10% of
  writes are multi-op batches of 2–4 ops, including a put and a delete, to
  exercise batch atomicity.
- Values are deterministic byte strings derived from the request sequence, not
  from wall-clock or map iteration.
- Each write batch carries `AppliedIndex = writeSeq`, a deterministic
  monotonically increasing counter. This exercises the Stage 4 applied-index
  path end to end; the oracle does not depend on it.
- After a response, it schedules the next request 5–12 ticks later.
- If a request is in flight, it does not send another one.
- At tick 290 the client stops issuing new writes and drains any uncertainty;
  after that it only probes or reads.

On a storage crash:

- If the in-flight request is a `Get`, it is discarded (it cannot change
  state); after restart the client may resend it with a new request ID.
- If the in-flight request is a write, the client calls
  `oracle.BeginUncertain(batch)`.
- The client keeps the request ID so a late acknowledgement can still resolve
  the batch as applied.

After the storage restarts, the client probes the uncertain batch's keys with
`Get` requests, one at a time:

- if every key matches the pre-batch oracle state, the batch is resolved as not
  applied;
- if every key matches the post-batch state, it is resolved as applied;
- a mixture is an atomicity violation and fails the run;
- a key matching neither state means a later write slipped in before
  resolution, which the client prevents by not sending new writes until the
  uncertain batch is resolved.

If the storage crashes again during probing, the client keeps the same
uncertain batch and restarts probing after the next restart. A late
acknowledgement for the uncertain batch resolves it as applied immediately,
because an acknowledgement is proof of durability. A late acknowledgement that
arrives after the uncertainty was already resolved must agree with the
resolution; a disagreement is a protocol error and fails the run.

### 8.3 Oracle

The oracle is deterministic and lives in `internal/storage/oracle.go`:

```go
type Oracle struct {
    acked     map[string]Entry // live keys only
    uncertain *Batch
}

func (o *Oracle) Ack(b Batch) error
func (o *Oracle) BeginUncertain(b Batch) error
func (o *Oracle) Resolve(applied bool) error
func (o *Oracle) Check(v View) error
func (o *Oracle) DeletedKeys() []string
func (o *Oracle) Snapshot() []byte
```

Semantics:

- `Ack` applies a batch's ops in order to `acked`: a put stores the value, a
  delete removes the key. If the batch is the current uncertain batch, `Ack`
  also clears the uncertainty (an acknowledgement proves it was applied).
- `BeginUncertain` records at most one in-flight write batch. A second call
  without resolution is a client protocol error.
- `Resolve(applied)` applies the uncertain batch to `acked` if `applied`, then
  clears it.
- `Check(v)` compares the engine's logical live map with the oracle:
  - with no uncertain batch, every key in `v.KV` and `acked` must have the same
    presence and value;
  - with an uncertain batch, compute the base world (`acked`) and the applied
    world (`acked` plus the batch). The entire `v.KV` must equal one of those
    worlds; a mixture of the two is an atomicity violation.
  - keys are sorted before iteration so errors and behavior are deterministic.
- `DeletedKeys` returns keys deleted and not re-put, sorted. The tombstone
  invariant uses it to assert they are absent from `v.KV`.
- Oracle entries carry `Seq = 0`; `Check` compares only presence and value.
  Batches are deep-copied on entry so later client mutations cannot change the
  oracle.
- If the pre-batch and post-batch worlds are identical (for example, a put of
  the value already present, or a delete of an absent key), probing cannot
  distinguish the two outcomes. The client resolves the batch deterministically
  as not applied; either choice is observationally correct.
- `Snapshot` canonically encodes `acked` in key order and the uncertain batch
  in op order. It also encodes the in-flight request ID, kind, batch, and
  expected read value, if any. It is included in the client node's snapshot so
  the workload is part of the simulation state hash.

### 8.4 Per-step invariants

All invariants are checked after every simulator event and once at the end of
the run. They never mutate state or consume RNG.

1. **`storage.oracle` — durability and atomicity.**
   `Oracle.Check(view)` must pass whenever the storage node is up and not
   recovering. This covers:
   - every acknowledged `Put`/`Delete` is reflected after every flush,
     compaction, and restart;
   - an uncertain batch is fully applied or fully absent;
   - no extra live key appears that the oracle did not acknowledge;
   - a deleted key does not reappear (tombstone safety).
2. **`storage.tombstones` — no resurrection.**
   For every key in `oracle.DeletedKeys()`, `view.KV` must not contain it. This
   is a focused check on compaction's tombstone dropping.
3. **`storage.metadata` — manifest and sequence monotonicity.**
   Per node, across incarnations:
   - `ManifestGeneration` never decreases;
   - `LastFlushedSeq` never decreases;
   - `AppliedIndex` never decreases;
   - `LastAppliedSeq >= LastFlushedSeq`;
   - `NextBatchSeq > LastAppliedSeq`;
   - when `Recovering` is false, the last recovery note is empty or reports a
     successful parse, and every live table's metadata is valid.
4. **`storage.reads` — Get matches the oracle.**
   The client records the expected value for each `Get` at send time and sets a
   read error if the response differs. The invariant checks that the client's
   error is empty. Reads are only issued when the oracle is exact (no uncertain
   batch). Probe reads used to resolve an uncertain batch are excluded; their
   results are checked against the two possible worlds by the oracle instead.
5. **`storage.checksums` — on-disk consistency.**
   Recovery either succeeds with all live SSTable and manifest checksums valid,
   or fails the run with a clear error. The invariant checks that a
   non-recovering node exposes only committed tables, sorted by ID and unique,
   with `LastFlushedSeq >= max(table.MaxSeq)` and metadata that matches the
   decoded table headers.

Invariants skip checks while the storage node is down or `Recovering`, but the
finalizer asserts that the node is up and recovered before the run ends.

### 8.5 Chaos coverage

The storage engine records `PhaseCounts`, and the client records the phase at
each crash. `TestStorageChaosCoverage` runs the default seed range and asserts
that crashes occurred in at least:

- WAL append and WAL sync;
- flush write, flush sync, flush manifest write, and flush manifest sync;
- compaction write, compaction sync, compaction manifest write, and compaction
  manifest sync.

If a phase is not reached, the chaos driver or the scenario thresholds are
tuned before the 3C gate. Targeted unit tests in `internal/storage` also crash
at each phase directly, so the crash semantics are guaranteed even if the
scenario's random schedule misses a phase for a particular seed.

---

## 9. Testing and verification

### 9.1 Format and unit tests

`internal/storage` unit tests (no simulator unless noted):

- `TestWALRecordRoundTrip`: encode/decode every op shape, including a
  multi-op batch and a delete.
- `TestWALRecordRejectsBadCRC`, `...BadLength`, `...BadOp`, `...OversizeKey`,
  `...OversizeValue`, `...DeleteWithValue`.
- `TestWALScanClean`, `TestWALScanTornHeader`, `TestWALScanTornPayload`,
  `TestWALScanBadCRC`, `TestWALScanStopsAtFirstInvalid`.
- `TestNonFinalWALCorruptionFailsRecovery`: a corrupted synced segment is a
  hard error, never a silent loss of later segments.
- `TestWALTruncateRewritesValidPrefix`.
- `TestMemtableSortedUpsert`, `TestMemtableTombstone`, `TestMemtableThresholds`.
- `TestLogicalMapMatchesGet`: after writes, flushes, and compactions, the
  logical live map and the `Get` path agree for every key.
- `TestSSTableRoundTrip`: sorted entries, tombstones, multiple data blocks,
  index lookup, content hash.
- `TestSSTableRejectsBadHeaderCRC`, `...BadBlockCRC`, `...BadIndexCRC`,
  `...UnsortedKeys`, `...DuplicateKeys`, `...ContentHashMismatch`.
- `TestManifestRoundTrip`, `TestManifestRejectsBadCRC`, `...BadGeneration`,
  `...MissingTable`, `...SizeMismatch`, `...UnsortedTables`.
- `TestClientCodecRoundTrip`, `TestClientCodecRejectsBadCRC`.
- `TestOraclePossibleWorlds`, `TestOracleAtomicBatch`, `TestOracleTombstone`.
- `TestStorageImportsOnlySim`: parse the package's non-test imports and allow
  only `raftkv/internal/sim` and standard-library packages; reject
  `internal/raft`, `internal/scenarios`, `internal/kv`, and `cmd/simctl`.

### 9.2 Crash and recovery tests

These use a small `sim.Sim` with a storage node and a test client. A test
helper runs the simulator one event at a time until `View().Phase` equals a
target, then calls `sim.Crash(1)` before the completion. The node is restarted
and the run continues until idle. Fault-injection tests write a torn or corrupt
tail and `Sync` it before restarting, so the corruption is durable and the
recovery path sees it.

- `TestWALCrashBeforeAppend`: crash before the append completes; no ack, batch
  absent after restart.
- `TestWALCrashBeforeSync`: crash after the append completes but before the
  sync completes; no ack, batch absent after restart.
- `TestWALCrashDuringSync`: crash while the sync is in flight; no ack, batch
  absent after restart.
- `TestWALCrashAfterSync`: crash after the sync completion callback; the batch
  is durable, applied, and acknowledged.
- `TestWALCrashAfterAck`: crash after the client receives the ack; the batch
  survives restart.
- `TestStaleIOAfterRestartIgnored`: an old-epoch completion is never applied.
- `TestFlushCrashAtWrite`, `TestFlushCrashAtSync`,
  `TestFlushCrashAtManifestWrite`, `TestFlushCrashAtManifestSync`.
- `TestCompactionCrashAtWrite`, `TestCompactionCrashAtSync`,
  `TestCompactionCrashAtManifestWrite`, `TestCompactionCrashAtManifestSync`.
- `TestRecoveryCrashDuringList`, `...Manifest`, `...Table`, `...WAL`,
  `...Truncate`, `...InitialManifest`.
- `TestAppliedIndexAtomicWithBatch`: a batch with an applied index is all or
  nothing; the recovered index matches the durable data.
- `TestManifestGenerationMonotonicAcrossRestarts`.
- `TestWALTruncationEndToEnd`: inject a torn/corrupt tail through the test-only
  I/O hook, restart, verify the valid prefix and a clean rewrite.
- `TestSSTableCorruptionFailsRecovery`: corrupt a live table, expect a clear
  failure.
- `TestManifestCorruptionFailsRecovery`: corrupt the manifest, expect a clear
  failure.

### 9.3 Scenario tests

`internal/scenarios/storage_test.go`:

- `TestStorageSameSeedProducesIdenticalTrace`: run seed 42 three times; trace,
  trace hash, and state hash must match.
- `TestStorageInterleavedSimsDoNotShareState`: two sims advanced alternately
  must match their solo traces.
- `TestStorageDifferentSeedsDiffer`.
- `TestStorageGoldenTraceSeed42`: byte equality with
  `testdata/stage3/storage-seed-42.trace`; `-update` regenerates it.
- `TestStorageSameSeedAcrossProcesses`: build `simctl`, run
  `-seed 42 -scenario storage -ticks 400 -trace` twice, compare full output,
  and assert `scenario=storage`, `nodes=2`, `status=ok`, and both hashes.
- `TestStorageReplayPrefixIndependentOfEnd`: a run to tick T has the same trace
  prefix as a longer run to T2.
- `TestStorageFinalState`: storage up, not recovering, not busy, no queued or
  in-flight request, oracle exact, at least one flush and one compaction,
  manifest generation advanced, applied index consistent.
- `TestStorageMultiSeed`: seeds `1..-storage-seeds` (default 50), each runs to
  400 ticks with no invariant failure; progress logged every 1000 seeds.
- `TestStorageChaosCoverage`: aggregate crash phases cover WAL append/sync,
  flush write/sync, flush manifest write/sync, compaction write/sync, and
  compaction manifest write/sync.
- `TestStorageInvariantCatchesLostAck`,
  `...CatchesPartialBatch`, `...CatchesResurrectedTombstone`,
  `...CatchesExtraKey`, `...CatchesMetadataRegression`: inject a fake `View`
  through a test `ViewSource` and assert the relevant invariant fails.

### 9.4 Negative invariant tests

The invariant constructors accept a `ViewSource`, mirroring Stage 2's
`NewElectionSafetyWithSource`:

```go
type ViewSource func(s *sim.Sim, id sim.NodeID) (View, bool)
```

Tests supply a fake source returning a deliberately broken `View`:

- an acknowledged key missing;
- an unacknowledged batch with only one of its keys present;
- a deleted key present again;
- a key the oracle never saw;
- a manifest generation or `lastFlushedSeq` regression;
- an invalid live table metadata set.

Each test asserts the invariant returns a specific error. These tests prove the
oracle can actually fail, not just pass.

### 9.5 Golden trace

- Path: `testdata/stage3/storage-seed-42.trace`.
- Generated with `go test ./internal/scenarios -run
  TestStorageGoldenTraceSeed42 -update`.
- The golden trace is reviewed before the 3C gate, like the Stage 1 and Stage 2
  traces.
- Any intentional behavior change that alters event order or RNG draws updates
  the trace and notes the change in the bug log.

### 9.6 Multi-seed and stress

- The default suite runs `TestStorageMultiSeed` with 50 seeds. It must keep
  `go test ./...` under roughly 20 seconds on the development machine.
- `make stress-storage SEEDS=10000` runs the same test with 10,000 seeds and a
  60-minute timeout. This is an explicit gate, not part of the default suite.
- The stress run logs progress every 1000 seeds and reports the seed, scenario,
  and replay command on failure.

### 9.7 Verification commands

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/storage -count=5
go test ./internal/scenarios -run TestStorageSameSeedProducesIdenticalTrace -count=5
go test ./internal/scenarios -run TestStorageGoldenTraceSeed42
go test ./internal/scenarios -run TestStorageSameSeedAcrossProcesses
go test ./internal/scenarios -run TestStorageMultiSeed -count=1
make stress-storage SEEDS=10000
go run ./cmd/simctl -seed 42 -scenario storage -ticks 400 -trace
go list -deps ./internal/sim   # must not contain internal/raft or internal/storage
```

---

## 10. CLI and Makefile

### 10.1 `cmd/simctl`

Add `StorageScenario = "storage"` in `internal/scenarios/storage.go`, include it
in the accepted scenario list, and dispatch:

```go
case scenarios.StorageScenario:
    s, err = scenarios.NewStorage(cfg, sim.Tick(*ticks))
```

The unknown-scenario error lists ping, raft, and storage.

Usage:

```sh
go run ./cmd/simctl -seed 42 -scenario storage -ticks 400 -trace
```

The output keeps the existing format:

```text
run v=1 seed=42 scenario=storage nodes=2 end-tick=400 status=ok
steps=... events=... sent=... delivered=... dropped=... disk-ops=... boots=... crashes=... restarts=...
trace-hash=... state-hash=...
```

### 10.2 Makefile

Add to `.PHONY` and the targets:

```make
sim-storage:
	$(GO) run ./cmd/simctl -seed 1 -scenario storage -ticks 400 -trace

stress-storage:
	$(GO) test ./internal/scenarios -run TestStorageMultiSeed -count=1 \
		-timeout 60m -args -storage-seeds=$(SEEDS)

golden-stage3:
	$(GO) test ./internal/scenarios -run TestStorageGoldenTraceSeed42 -update
```

`SEEDS` already defaults to `10000`. The default `make test` remains
`go test ./...`.

---

## 11. Documentation and bug log

- README status becomes Stage 3 approved/in progress after approval, then
  Complete after the 3C gate.
- README links this plan, lists the new commands, and describes the
  `internal/storage` package.
- The repository layout adds `PLAN_STAGE3.md`, `internal/storage/`, the
  `storage` scenario, and `testdata/stage3/`.
- Every Stage 3 bug found by the simulator is logged in the README before its
  fix is accepted, starting at `S3-001`, with:
  - seed and scenario;
  - tick/phase and observed symptom;
  - root cause;
  - fix;
  - regression test name.
- A fix without a regression test is not accepted.
- If a behavior change alters RNG draws or event ordering, the golden trace is
  regenerated and the change is noted in the bug log.

---

## 12. Risks and open questions

1. **In-memory SSTable reads.** This is the main simplification. It keeps
   `Get` synchronous and deterministic but does not exercise per-read disk
   latency. If disk-resident reads are required in Stage 3, the plan needs a
   read pipeline, table reference counting, and crash tests for in-flight
   reads; that would expand 3B/3C materially.
2. **Single manifest.** Safe under the simulator's atomic `Replace` plus
   `Sync`. A real disk could tear the manifest; a production design would use
   two alternating manifest slots. If the user wants that realism now, add
   `MANIFEST-A`/`MANIFEST-B` with generation selection and fallback.
3. **No physical deletion.** The simulator has no delete/truncate, so retired
   files remain. This is correct but uses more simulated disk. A future
   cleanup pass can zero retired files after the manifest commit.
4. **Full compaction.** Simple and tombstone-safe, but rewrites all live data.
   Partial compaction would require retaining tombstones when older tables
   remain.
5. **Scenario thresholds.** Small flush/compaction thresholds are needed to
   exercise the LSM within 400 ticks. The exact values are tuned during 3C; the
   acceptance criteria are flush/compaction coverage and the 10,000-seed run.
6. **Applied-index semantics.** Stage 3 treats a nonzero applied index as
   strictly increasing per batch. Stage 4 must submit contiguous indexes and
   must not reuse an index after a crash. The `OnDurable` callback and
   `View.AppliedIndex` are the integration points; Stage 4's exact protocol is
   out of scope.

---

## 13. Approval checklist

Before implementation starts, the user approves:

- [ ] The 3A/3B/3C split and gate commands.
- [ ] The WAL record format, including CRC, batch sequence, and applied index.
- [ ] The SSTable and manifest formats and the manifest-as-commit-point rule.
- [ ] The in-memory SSTable read cache and one-in-flight I/O pipeline.
- [ ] Full compaction and logical (non-physical) retirement.
- [ ] The oracle's possible-world model and the five per-step invariants.
- [ ] The `storage` scenario timeline, chaos driver, and golden trace path.
- [ ] The `sim-storage`, `stress-storage`, and `golden-stage3` targets.
- [ ] The Stage 3 bug-log policy starting at `S3-001`.

After approval, implementation starts with 3A only. No 3B or 3C code is written
until the 3A gate passes.


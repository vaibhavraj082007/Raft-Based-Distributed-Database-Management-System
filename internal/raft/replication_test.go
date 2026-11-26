package raft

import (
	"fmt"
	"reflect"
	"testing"

	"raftkv/internal/sim"
)

func logEntry(index, term uint64, kind EntryKind, data string) Entry {
	return Entry{Index: index, Term: term, Kind: kind, Data: []byte(data)}
}

func cloneU64(in []uint64) []uint64 {
	return append([]uint64(nil), in...)
}

func decodeSingleAppend(t *testing.T, env *testEnv) AppendRequest {
	t.Helper()
	if len(env.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(env.sent))
	}
	if env.sent[0].kind != KindAppendRequest {
		t.Fatalf("sent kind %s, want %s", env.sent[0].kind, KindAppendRequest)
	}
	return decodeAppendRequest(env.sent[0].payload)
}

func TestLogSentinelAndIndexing(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	if len(n.log) != 1 || n.log[0].Index != 0 || n.log[0].Term != 0 || n.log[0].Kind != EntrySentinel {
		t.Fatalf("bad sentinel: %+v", n.log)
	}
	n.currentTerm = 3
	noop := n.appendLocal(EntryNoop, nil)
	command := n.appendLocal(EntryCommand, []byte("x"))
	if noop.Index != 1 || noop.Term != 3 || noop.Kind != EntryNoop || noop.Data != nil {
		t.Fatalf("noop = %+v", noop)
	}
	if command.Index != 2 || command.Term != 3 || command.Kind != EntryCommand || string(command.Data) != "x" {
		t.Fatalf("command = %+v", command)
	}
	if n.lastLogIndex() != 2 || n.lastLogTerm() != 3 {
		t.Fatalf("last = %d/%d, want 2/3", n.lastLogIndex(), n.lastLogTerm())
	}
	if index, ok := n.firstIndexOfTerm(3); !ok || index != 1 {
		t.Fatalf("first index of term 3 = %d/%t, want 1/true", index, ok)
	}
	if index, ok := n.lastIndexOfTerm(3); !ok || index != 2 {
		t.Fatalf("last index of term 3 = %d/%t, want 2/true", index, ok)
	}
	if _, ok := n.firstIndexOfTerm(4); ok {
		t.Fatalf("found absent term 4")
	}
}

func TestLeaderNoopPersistedBeforeHeartbeat(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	env.clearTraffic()

	fireElectionTimer(t, env, n)
	completePersist(t, env, n)
	if n.role != Candidate {
		t.Fatalf("role = %s, want candidate", n.role)
	}
	env.clearTraffic()

	n.HandleMessage(voteResponseMessage(2, 1, true))
	if n.role != Leader {
		t.Fatalf("role = %s, want leader", n.role)
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistLog || n.pending.action != actionLeaderNoop {
		t.Fatalf("leader did not start no-op log persistence: busy=%t pending=%+v", n.busy, n.pending)
	}
	if len(n.log) != 2 || n.log[1].Kind != EntryNoop || n.log[1].Term != 1 {
		t.Fatalf("leader log = %+v, want term-1 no-op at index 1", n.log)
	}
	if len(env.sent) != 0 {
		t.Fatalf("leader sent %d messages before the no-op was durable", len(env.sent))
	}

	completeAppend(t, env, n)
	if len(env.sent) != 0 {
		t.Fatalf("leader sent %d messages before the no-op sync completed", len(env.sent))
	}
	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
	if n.durableLastIndex != 1 {
		t.Fatalf("durable last = %d, want 1", n.durableLastIndex)
	}
	if len(env.sent) != 2 {
		t.Fatalf("leader sent %d messages after no-op sync, want 2", len(env.sent))
	}
	for _, msg := range env.sent {
		if msg.kind != KindAppendRequest {
			t.Fatalf("post-no-op kind = %s, want append request", msg.kind)
		}
		req := decodeAppendRequest(msg.payload)
		if req.Term != 1 || req.PrevLogIndex != 0 || len(req.Entries) != 1 ||
			req.Entries[0].Index != 1 || req.Entries[0].Kind != EntryNoop {
			t.Fatalf("post-no-op request = %+v", req)
		}
	}
	if env.timerCount(timerTagHeartbeat) != 1 {
		t.Fatalf("heartbeat timers = %d, want 1", env.timerCount(timerTagHeartbeat))
	}
}

func TestAppendEntriesPrevLogChecks(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 1, 0)
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 5, PrevLogTerm: 1,
	}))
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || resp.Success || resp.ConflictIndex != 1 || resp.ConflictTerm != 0 {
		t.Fatalf("too-short response = %+v", resp)
	}

	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		{Index: 1, Term: 1, Kind: EntryNoop},
		{Index: 2, Term: 1, Kind: EntryNoop},
		{Index: 3, Term: 2, Kind: EntryNoop},
	}
	n.durableLastIndex = 3
	setFollowerTerm(n, 2, 0)
	env.clearTraffic()
	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2,
	}))
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses, want 1", len(env.sent))
	}
	resp = decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 2 || resp.Success || resp.ConflictIndex != 1 || resp.ConflictTerm != 1 {
		t.Fatalf("term-mismatch response = %+v", resp)
	}
}

func TestAppendEntriesAppendsAndPersists(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 1, 0)
	env.clearTraffic()

	entries := []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "cmd"),
	}
	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries,
	}))
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before log sync")
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistLog {
		t.Fatalf("log batch was not persisted: busy=%t pending=%+v", n.busy, n.pending)
	}
	if n.durableLastIndex != 0 || n.lastLogIndex() != 2 {
		t.Fatalf("durable/last = %d/%d, want 0/2", n.durableLastIndex, n.lastLogIndex())
	}
	completeAppend(t, env, n)
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before log sync completion")
	}
	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses after sync, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("response = %+v, want term 1 success match 2", resp)
	}
	if n.durableLastIndex != 2 || n.lastLogIndex() != 2 {
		t.Fatalf("durable/last = %d/%d, want 2/2", n.durableLastIndex, n.lastLogIndex())
	}
	if !reflect.DeepEqual(n.log[1:], entries) {
		t.Fatalf("log = %+v, want %+v", n.log[1:], entries)
	}
}

func TestAppendEntriesTruncatesConflict(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 2, 0)
	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "old2"),
		logEntry(3, 1, EntryCommand, "old3"),
	}
	n.durableLastIndex = 3
	env.clearTraffic()

	entries := []Entry{
		logEntry(2, 2, EntryCommand, "new2"),
		logEntry(3, 2, EntryCommand, "new3"),
	}
	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 2, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries,
	}))
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before truncate batch sync")
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistLog ||
		n.pending.batch.baseIndex != 2 || len(n.pending.batch.entries) != 2 {
		t.Fatalf("truncate batch = %+v", n.pending)
	}
	if got := n.log[2].Data; string(got) != "new2" {
		t.Fatalf("in-memory index 2 data = %q, want new2", got)
	}
	completeAppend(t, env, n)
	sync := env.lastSync()
	deliverSync(t, env, n, sync.tag)
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 2 || !resp.Success || resp.MatchIndex != 3 {
		t.Fatalf("response = %+v, want term 2 success match 3", resp)
	}
	if n.durableLastIndex != 3 || n.lastLogIndex() != 3 {
		t.Fatalf("durable/last = %d/%d, want 3/3", n.durableLastIndex, n.lastLogIndex())
	}
	if !reflect.DeepEqual(n.log[1:], []Entry{logEntry(1, 1, EntryNoop, ""), entries[0], entries[1]}) {
		t.Fatalf("log = %+v", n.log[1:])
	}
}

func TestAppendEntriesDuplicateIsIdempotent(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 1, 0)
	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "cmd"),
	}
	n.durableLastIndex = 2
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: cloneEntries(n.log[1:]),
	}))
	if n.busy || len(env.appends) != 0 || len(env.replaces) != 0 {
		t.Fatalf("duplicate request persisted: busy=%t appends=%d replaces=%d",
			n.busy, len(env.appends), len(env.replaces))
	}
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("response = %+v, want success match 2", resp)
	}
}

func TestAppendEntriesNeverTruncatesCommitted(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 2, 0)
	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "committed"),
	}
	n.durableLastIndex = 2
	n.commitIndex = 2
	n.lastApplied = 2

	mustPanic(t, "truncate committed entry", func() {
		n.HandleMessage(appendRequestMessage(1, AppendRequest{
			Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2,
		}))
	})
}

func TestConflictBacktrackingByTerm(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	setLeaderState(t, n, 4, []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "a"),
		logEntry(3, 4, EntryNoop, ""),
		logEntry(4, 4, EntryCommand, "b"),
	}, 4, []uint64{1, 0}, []uint64{4, 5}, 0, 0)
	env.clearTraffic()

	n.HandleMessage(appendResponseMessageFull(2, AppendResponse{
		Term: 4, Success: false, ConflictIndex: 2, ConflictTerm: 2,
	}))
	if n.nextIndex[0] != 3 {
		t.Fatalf("nextIndex = %d, want 3", n.nextIndex[0])
	}
	req := decodeSingleAppend(t, env)
	if req.PrevLogIndex != 2 || req.PrevLogTerm != 2 || len(req.Entries) != 2 ||
		req.Entries[0].Index != 3 || req.Entries[1].Index != 4 {
		t.Fatalf("backtracked request = %+v", req)
	}
}

func TestStaleAppendResponseIgnored(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	setLeaderState(t, n, 4, []Entry{
		logEntry(1, 4, EntryNoop, ""),
		logEntry(2, 4, EntryNoop, ""),
		logEntry(3, 4, EntryNoop, ""),
		logEntry(4, 4, EntryNoop, ""),
	}, 4, []uint64{2, 0}, []uint64{3, 5}, 0, 0)
	env.clearTraffic()

	n.HandleMessage(appendResponseMessageFull(2, AppendResponse{
		Term: 4, Success: false, ConflictIndex: 1, ConflictTerm: 0,
	}))
	if n.nextIndex[0] != 3 {
		t.Fatalf("stale failure regressed nextIndex to %d, want 3", n.nextIndex[0])
	}
	if len(env.sent) != 0 {
		t.Fatalf("stale failure triggered %d sends", len(env.sent))
	}

	n.HandleMessage(appendResponseMessageFull(2, AppendResponse{
		Term: 4, Success: true, MatchIndex: 1,
	}))
	if n.matchIndex[0] != 2 || n.nextIndex[0] != 3 {
		t.Fatalf("stale success changed match/next to %d/%d, want 2/3",
			n.matchIndex[0], n.nextIndex[0])
	}
}

func TestCommitRequiresMajority(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3, 4, 5})
	var applied []uint64
	n.cfg.Apply = func(e Entry) { applied = append(applied, e.Index) }
	setLeaderState(t, n, 4, []Entry{
		logEntry(1, 4, EntryNoop, ""),
		logEntry(2, 4, EntryCommand, "a"),
		logEntry(3, 4, EntryCommand, "b"),
	}, 3, []uint64{2, 1, 1, 1}, []uint64{3, 3, 3, 3}, 0, 0)
	env.clearTraffic()

	n.advanceCommit()
	if n.commitIndex != 1 || !reflect.DeepEqual(applied, []uint64{1}) {
		t.Fatalf("minority commit = %d applied=%v, want 1/[1]", n.commitIndex, applied)
	}
	n.matchIndex = []uint64{2, 2, 1, 1}
	n.advanceCommit()
	if n.commitIndex != 2 || !reflect.DeepEqual(applied, []uint64{1, 2}) {
		t.Fatalf("majority commit = %d applied=%v, want 2/[1 2]", n.commitIndex, applied)
	}
	n.matchIndex = []uint64{3, 3, 3, 1}
	n.advanceCommit()
	if n.commitIndex != 3 || !reflect.DeepEqual(applied, []uint64{1, 2, 3}) {
		t.Fatalf("current-term majority commit = %d applied=%v, want 3/[1 2 3]", n.commitIndex, applied)
	}
}

func TestCommitOnlyCurrentTerm(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3, 4, 5})
	var applied []uint64
	n.cfg.Apply = func(e Entry) { applied = append(applied, e.Index) }
	setLeaderState(t, n, 4, []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "old"),
	}, 2, []uint64{2, 2, 1, 1}, []uint64{3, 3, 3, 3}, 0, 0)
	env.clearTraffic()

	n.advanceCommit()
	if n.commitIndex != 0 || len(applied) != 0 {
		t.Fatalf("old-term majority committed: commit=%d applied=%v", n.commitIndex, applied)
	}

	n.log = append(n.log, logEntry(3, 4, EntryNoop, ""))
	n.durableLastIndex = 3
	n.matchIndex = []uint64{3, 3, 3, 1}
	n.nextIndex = []uint64{4, 4, 4, 4}
	n.advanceCommit()
	if n.commitIndex != 3 || !reflect.DeepEqual(applied, []uint64{1, 2, 3}) {
		t.Fatalf("current-term entry did not commit transitively: commit=%d applied=%v",
			n.commitIndex, applied)
	}
}

func TestFigure8Integration(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3, 4, 5})
	applied := map[uint64]Entry{}
	n.cfg.Apply = func(e Entry) {
		if prev, ok := applied[e.Index]; ok {
			if !reflect.DeepEqual(prev, e) {
				t.Fatalf("two entries applied at index %d: %+v and %+v", e.Index, prev, e)
			}
			return
		}
		applied[e.Index] = cloneEntry(e)
	}
	setLeaderState(t, n, 4, []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "old"),
	}, 2, []uint64{2, 2, 1, 1}, []uint64{3, 3, 3, 3}, 0, 0)
	env.clearTraffic()

	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("Figure 8 trap: committed prior-term entry by replica count, commit=%d", n.commitIndex)
	}

	n.log = append(n.log, logEntry(3, 4, EntryNoop, ""))
	n.durableLastIndex = 3
	n.matchIndex = []uint64{3, 3, 3, 1}
	n.nextIndex = []uint64{4, 4, 4, 4}
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("current-term no-op did not commit transitively: commit=%d", n.commitIndex)
	}
	if len(applied) != 3 {
		t.Fatalf("applied %d entries, want 3", len(applied))
	}
	for i := uint64(1); i <= 3; i++ {
		if _, ok := applied[i]; !ok {
			t.Fatalf("index %d was not applied", i)
		}
	}
}

func TestApplyInOrder(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	var applied []uint64
	n.cfg.Apply = func(e Entry) { applied = append(applied, e.Index) }
	setLeaderState(t, n, 1, []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "a"),
		logEntry(3, 1, EntryCommand, "b"),
	}, 3, []uint64{0, 0}, []uint64{4, 4}, 0, 0)
	n.commitIndex = 3
	n.apply()
	if !reflect.DeepEqual(applied, []uint64{1, 2, 3}) {
		t.Fatalf("applied = %v, want [1 2 3]", applied)
	}
	n.apply()
	if !reflect.DeepEqual(applied, []uint64{1, 2, 3}) {
		t.Fatalf("re-apply changed applied = %v", applied)
	}
}

func TestFollowerCommitClampedToProvenPrefix(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 1, 0)
	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "unproven"),
		logEntry(3, 1, EntryCommand, "unproven2"),
	}
	n.durableLastIndex = 3
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3,
	}))
	if n.commitIndex != 1 || n.lastApplied != 1 {
		t.Fatalf("heartbeat committed unproven suffix: commit=%d applied=%d", n.commitIndex, n.lastApplied)
	}

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3,
	}))
	if n.commitIndex != 3 || n.lastApplied != 3 {
		t.Fatalf("proven prefix did not commit: commit=%d applied=%d", n.commitIndex, n.lastApplied)
	}
}

func TestRecoveryReplaysDurableLog(t *testing.T) {
	env := newTestEnv(2)
	entries := []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "cmd"),
	}
	env.durable[metaFile] = encodeMeta(1, 0)
	env.durable[logFile] = encodeLogBatch(logBatch{baseIndex: 1, entries: entries})
	var applied []Entry
	cfg := Config{ID: 2, Peers: []sim.NodeID{1, 3}, Apply: func(e Entry) { applied = append(applied, cloneEntry(e)) }}
	factory := NewFactory(cfg)
	n := factory(env).(*Node)
	n.Start()
	completeBoot(t, env, n)

	if n.currentTerm != 1 || n.durableTerm != 1 || n.votedFor != 0 {
		t.Fatalf("recovered term/vote = %d/%d/%d", n.currentTerm, n.durableTerm, n.votedFor)
	}
	if n.lastLogIndex() != 2 || n.durableLastIndex != 2 || n.logBytes != uint64(len(env.durable[logFile])) {
		t.Fatalf("recovered log last/durable/bytes = %d/%d/%d",
			n.lastLogIndex(), n.durableLastIndex, n.logBytes)
	}
	if n.commitIndex != 0 || n.lastApplied != 0 || len(applied) != 0 {
		t.Fatalf("recovery applied before commit: commit=%d applied=%d hook=%d",
			n.commitIndex, n.lastApplied, len(applied))
	}

	env.clearTraffic()
	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
	}))
	if n.commitIndex != 2 || n.lastApplied != 2 {
		t.Fatalf("commit replay = %d/%d, want 2/2", n.commitIndex, n.lastApplied)
	}
	if len(applied) != 2 || applied[0].Index != 1 || applied[1].Index != 2 {
		t.Fatalf("apply replay = %+v", applied)
	}
}

func TestLogReplayTruncation(t *testing.T) {
	first := logBatch{baseIndex: 1, entries: []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 1, EntryCommand, "old"),
	}}
	second := logBatch{baseIndex: 2, entries: []Entry{
		logEntry(2, 2, EntryCommand, "new"),
		logEntry(3, 2, EntryNoop, ""),
	}}
	data := append(encodeLogBatch(first), encodeLogBatch(second)...)
	log := decodeLogFile(data)
	want := []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "new"),
		logEntry(3, 2, EntryNoop, ""),
	}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("replayed log = %+v, want %+v", log, want)
	}
}

func TestCrashBeforeLogSync(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()
	n.HandleMessage(heartbeatMessage(1, 1))
	completePersist(t, env, n)
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{logEntry(1, 1, EntryNoop, "")},
	}))
	if len(env.sent) != 0 || !n.busy {
		t.Fatalf("append did not wait for sync: sent=%d busy=%t", len(env.sent), n.busy)
	}
	completeAppend(t, env, n)
	env.crash()
	restarted := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	if restarted.currentTerm != 1 || restarted.lastLogIndex() != 0 || restarted.durableLastIndex != 0 {
		t.Fatalf("restart recovered term/log = %d/%d/%d, want 1/0/0",
			restarted.currentTerm, restarted.lastLogIndex(), restarted.durableLastIndex)
	}
	if len(env.sent) != 0 {
		t.Fatalf("append reply leaked across crash: %d", len(env.sent))
	}
}

func TestCrashAfterLogSync(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()
	n.HandleMessage(heartbeatMessage(1, 1))
	completePersist(t, env, n)
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{logEntry(1, 1, EntryNoop, ""), logEntry(2, 1, EntryCommand, "x")},
	}))
	completePersist(t, env, n)
	if len(env.sent) != 1 {
		t.Fatalf("reply count after sync = %d, want 1", len(env.sent))
	}
	env.crash()
	env.clearTraffic()
	restarted := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	if restarted.currentTerm != 1 || restarted.lastLogIndex() != 2 || restarted.durableLastIndex != 2 {
		t.Fatalf("restart recovered term/log = %d/%d/%d, want 1/2/2",
			restarted.currentTerm, restarted.lastLogIndex(), restarted.durableLastIndex)
	}
	if restarted.commitIndex != 0 || restarted.lastApplied != 0 {
		t.Fatalf("restart kept volatile commit/applied = %d/%d", restarted.commitIndex, restarted.lastApplied)
	}
}

func TestDelayedAppendEntriesAfterNewLeader(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 2, 0)
	n.log = []Entry{
		{Index: 0, Term: 0, Kind: EntrySentinel},
		logEntry(1, 2, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "current"),
	}
	n.durableLastIndex = 2
	env.clearTraffic()

	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{logEntry(1, 1, EntryCommand, "stale")},
	}))
	if n.busy || len(env.appends) != 0 || len(env.replaces) != 0 {
		t.Fatalf("stale request changed durable state")
	}
	if n.lastLogIndex() != 2 || string(n.log[2].Data) != "current" {
		t.Fatalf("stale request truncated current log: %+v", n.log)
	}
	if len(env.sent) != 1 {
		t.Fatalf("sent %d responses, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 2 || resp.Success || resp.ConflictIndex != 3 {
		t.Fatalf("stale response = %+v", resp)
	}
}

func TestDelayedVoteResponse(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	fireElectionTimer(t, env, n)
	completePersist(t, env, n)
	env.clearTraffic()

	n.HandleMessage(voteResponseMessage(2, 0, true))
	if n.role != Candidate || n.currentTerm != 1 || len(env.sent) != 0 {
		t.Fatalf("stale vote response changed candidate: role=%s term=%d sent=%d",
			n.role, n.currentTerm, len(env.sent))
	}
}

func TestClientAcceptedAfterLogSync(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, nil)
	fireElectionTimer(t, env, n)
	completeAllPersist(t, env, n)
	if n.role != Leader || n.commitIndex != 1 {
		t.Fatalf("setup leader state role=%s commit=%d", n.role, n.commitIndex)
	}
	env.clearTraffic()

	req := ClientRequest{ClientID: 100, Seq: 1, Data: []byte("cmd")}
	n.HandleMessage(sim.Message{
		From: 100, Kind: KindClientRequest, Payload: EncodeClientRequest(req),
	})
	if len(env.sent) != 0 {
		t.Fatalf("client response before log sync: %d", len(env.sent))
	}
	if !n.busy || n.pending == nil || n.pending.action != actionLeaderClientAppend {
		t.Fatalf("client append was not persisted: %+v", n.pending)
	}
	completeAllPersist(t, env, n)

	var accepted, committed int
	for _, msg := range env.sent {
		if msg.kind != KindClientResponse {
			t.Fatalf("client received kind %s", msg.kind)
		}
		resp, err := DecodeClientResponse(msg.payload)
		if err != nil {
			t.Fatalf("DecodeClientResponse: %v", err)
		}
		if resp.Term != 1 || resp.Index != 2 {
			t.Fatalf("client response = %+v, want term 1 index 2", resp)
		}
		if resp.Accepted {
			accepted++
		}
		if resp.Committed {
			committed++
		}
	}
	if accepted != 1 || committed != 1 {
		t.Fatalf("client responses accepted=%d committed=%d, want 1/1", accepted, committed)
	}
	if n.commitIndex != 2 || n.lastApplied != 2 {
		t.Fatalf("client entry commit/applied = %d/%d, want 2/2", n.commitIndex, n.lastApplied)
	}
}

func TestReplicationSmoke(t *testing.T) {
	var mu = make(map[sim.NodeID]map[uint64]Entry)
	apply := func(id sim.NodeID, e Entry) {
		m := mu[id]
		if m == nil {
			m = make(map[uint64]Entry)
			mu[id] = m
		}
		if prev, ok := m[e.Index]; ok && !reflect.DeepEqual(prev, e) {
			t.Fatalf("node %d applied two entries at index %d", id, e.Index)
		}
		m[e.Index] = cloneEntry(e)
	}
	s, ids := newRaftSimApply(t, 42, 5, apply)
	if err := s.RunUntil(200); err != nil {
		t.Fatalf("RunUntil(200): %v", err)
	}
	leader := singleLeader(t, s, ids)
	leaderNode := mustNode(t, s, leader.ID)

	req := ClientRequest{ClientID: 100, Seq: 1, Data: []byte("cmd")}
	if err := s.ScheduleFunc(s.Now(), "client-request", func(*sim.Sim) error {
		leaderNode.HandleMessage(sim.Message{
			From: 100, Kind: KindClientRequest, Payload: EncodeClientRequest(req),
		})
		return nil
	}); err != nil {
		t.Fatalf("ScheduleFunc: %v", err)
	}
	if err := s.RunUntil(400); err != nil {
		t.Fatalf("RunUntil(400): %v", err)
	}

	views := liveViews(t, s, ids)
	if len(views) != 5 {
		t.Fatalf("live views = %d, want 5", len(views))
	}
	for _, v := range views {
		if v.CommitIndex < 2 || v.LastApplied < 2 {
			t.Fatalf("node %d commit/applied = %d/%d, want >=2",
				v.ID, v.CommitIndex, v.LastApplied)
		}
		if v.LastLogIndex < 2 || v.Log[2].Kind != EntryCommand {
			t.Fatalf("node %d log index 2 = %+v, want command", v.ID, v.Log)
		}
		got := mu[v.ID][2]
		if string(got.Data) != string(EncodeClientRequest(req)) {
			t.Fatalf("node %d applied command data %q", v.ID, got.Data)
		}
	}
}

func TestSnapshotCanonicalReplicationState(t *testing.T) {
	envA := newTestEnv(1)
	a := bootTestNode(t, envA, 1, []sim.NodeID{2, 3})
	envB := newTestEnv(1)
	b := bootTestNode(t, envB, 1, []sim.NodeID{2, 3})
	for _, n := range []*Node{a, b} {
		setLeaderState(t, n, 2, []Entry{
			logEntry(1, 2, EntryNoop, ""),
			logEntry(2, 2, EntryCommand, "x"),
		}, 2, []uint64{2, 1}, []uint64{3, 2}, 2, 1)
		n.pendingClients[2] = clientRef{ClientID: 9, Seq: 7}
	}
	if got, want := string(a.Snapshot()), string(b.Snapshot()); got != want {
		t.Fatalf("identical replication states differ:\n%s\n%s", got, want)
	}
	a.matchIndex[0] = 0
	if string(a.Snapshot()) == string(b.Snapshot()) {
		t.Fatalf("different matchIndex produced identical snapshots")
	}
}

func TestFigure8NoDuplicateApply(t *testing.T) {
	// A compact history check: an old-term entry may be present on a majority
	// but must not be applied until a current-term entry commits.
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3, 4, 5})
	applied := map[uint64]Entry{}
	n.cfg.Apply = func(e Entry) {
		if prev, ok := applied[e.Index]; ok && !reflect.DeepEqual(prev, e) {
			t.Fatalf("conflicting apply at index %d", e.Index)
		}
		applied[e.Index] = cloneEntry(e)
	}
	setLeaderState(t, n, 5, []Entry{
		logEntry(1, 1, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "a"),
		logEntry(3, 3, EntryCommand, "b"),
	}, 3, []uint64{3, 3, 2, 1}, []uint64{4, 4, 4, 4}, 0, 0)
	env.clearTraffic()
	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed an old-term entry without a current-term entry: %d", n.commitIndex)
	}
	n.log = append(n.log, logEntry(4, 5, EntryNoop, ""))
	n.durableLastIndex = 4
	n.matchIndex = []uint64{4, 4, 4, 2}
	n.advanceCommit()
	if n.commitIndex != 4 {
		t.Fatalf("current-term no-op commit = %d, want 4", n.commitIndex)
	}
	for i := uint64(1); i <= 4; i++ {
		if _, ok := applied[i]; !ok {
			t.Fatalf("index %d missing from apply history", i)
		}
	}
}

func TestLogFileRejectsCorruption(t *testing.T) {
	env := newTestEnv(1)
	_ = env
	good := encodeLogBatch(logBatch{baseIndex: 1, entries: []Entry{logEntry(1, 1, EntryNoop, "")}})
	bad := cloneTestBytes(good)
	bad[len(bad)-1] ^= 0xff
	mustPanic(t, "corrupt log CRC", func() { decodeLogFile(bad) })
	mustPanic(t, "trailing partial log", func() { decodeLogFile(good[:len(good)-1]) })
}

func TestAppendEntryTermCannotExceedRequestTerm(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	setFollowerTerm(n, 1, 0)
	mustPanic(t, "future-term append entry", func() {
		n.HandleMessage(appendRequestMessage(1, AppendRequest{
			Term: 1, LeaderID: 1,
			Entries: []Entry{logEntry(1, 2, EntryNoop, "")},
		}))
	})
}

func TestClientFollowerRedirect(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	n.leaderID = 1
	env.clearTraffic()
	n.HandleMessage(sim.Message{
		From: 100, Kind: KindClientRequest,
		Payload: EncodeClientRequest(ClientRequest{ClientID: 100, Seq: 1, Data: []byte("x")}),
	})
	if len(env.sent) != 1 {
		t.Fatalf("follower sent %d client responses, want 1", len(env.sent))
	}
	resp, err := DecodeClientResponse(env.sent[0].payload)
	if err != nil {
		t.Fatalf("DecodeClientResponse: %v", err)
	}
	if resp.Accepted || resp.Committed || resp.LeaderID != 1 || resp.Term != 0 {
		t.Fatalf("redirect response = %+v", resp)
	}
}

func TestDecodeClientResponseRejectsMalformed(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		{0, 1},
		append(encodeClientResponse(ClientResponse{Term: 1}), 0),
	} {
		if _, err := DecodeClientResponse(payload); err == nil {
			t.Fatalf("malformed client response accepted: %x", payload)
		}
	}
}

func TestViewIncludesLogAndCommit(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	setLeaderState(t, n, 2, []Entry{
		logEntry(1, 2, EntryNoop, ""),
		logEntry(2, 2, EntryCommand, "x"),
	}, 2, []uint64{2, 2}, []uint64{3, 3}, 2, 2)
	v := n.View()
	if v.CommitIndex != 2 || v.LastApplied != 2 || v.LastLogIndex != 2 || v.LastLogTerm != 2 {
		t.Fatalf("view = %+v", v)
	}
	if len(v.Log) != 3 || v.Log[2].Kind != EntryCommand {
		t.Fatalf("view log = %+v", v.Log)
	}
	if fmt.Sprintf("%s", v.Log[1].Kind) != "noop" {
		t.Fatalf("entry kind string = %s", v.Log[1].Kind)
	}
}

func TestHigherTermAppendPersistsTermBeforeLog(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()

	entries := []Entry{
		logEntry(1, 3, EntryNoop, ""),
		logEntry(2, 3, EntryCommand, "x"),
	}
	n.HandleMessage(appendRequestMessage(1, AppendRequest{
		Term: 3, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries,
	}))
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before the higher term was durable")
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistMeta ||
		n.pending.action != actionAppendAfterTerm {
		t.Fatalf("higher-term append did not start a meta batch: %+v", n.pending)
	}
	if len(env.appends) != 0 {
		t.Fatalf("log append issued before the term sync: %d", len(env.appends))
	}
	if n.currentTerm != 3 || n.durableTerm != 0 {
		t.Fatalf("term current/durable = %d/%d, want 3/0", n.currentTerm, n.durableTerm)
	}

	completeReplace(t, env, n)
	if len(env.appends) != 0 || len(env.sent) != 0 {
		t.Fatalf("log append/reply issued before the meta sync: appends=%d sent=%d",
			len(env.appends), len(env.sent))
	}
	deliverSync(t, env, n, env.lastSync().tag)
	if n.durableTerm != 3 || n.votedFor != 0 || n.durableVotedFor != 0 {
		t.Fatalf("durable term/vote = %d/%d/%d, want 3/0/0",
			n.durableTerm, n.votedFor, n.durableVotedFor)
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistLog ||
		n.pending.action != actionFollowerAppendReply {
		t.Fatalf("append did not start after the term sync: %+v", n.pending)
	}
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before the log sync")
	}

	completeAppend(t, env, n)
	if len(env.sent) != 0 {
		t.Fatalf("reply sent before the log sync completion")
	}
	deliverSync(t, env, n, env.lastSync().tag)
	if len(env.sent) != 1 {
		t.Fatalf("responses = %d, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 3 || !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("response = %+v, want term 3 success match 2", resp)
	}
	if n.durableLastIndex != 2 || n.lastLogIndex() != 2 {
		t.Fatalf("durable/last = %d/%d, want 2/2", n.durableLastIndex, n.lastLogIndex())
	}
}

func TestAppendResponseHigherTermStepsDown(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	setLeaderState(t, n, 2, []Entry{logEntry(1, 2, EntryNoop, "")}, 1,
		[]uint64{1, 0}, []uint64{2, 2}, 0, 0)
	env.clearTraffic()

	n.HandleMessage(appendResponseMessage(2, 3, false))
	if n.role != Follower || n.currentTerm != 3 || n.leaderID != 0 {
		t.Fatalf("step-down state role=%s term=%d leader=%d, want follower/3/0",
			n.role, n.currentTerm, n.leaderID)
	}
	if !n.busy || n.pending == nil || n.pending.kind != persistMeta ||
		n.pending.action != actionStepDown {
		t.Fatalf("higher-term response did not persist the term: %+v", n.pending)
	}
	if len(env.sent) != 0 {
		t.Fatalf("leader replied to a higher-term response: %d", len(env.sent))
	}

	completePersist(t, env, n)
	if n.durableTerm != 3 || n.durableVotedFor != 0 {
		t.Fatalf("durable term/vote = %d/%d, want 3/0", n.durableTerm, n.durableVotedFor)
	}
	if env.timerCount(timerTagElection) != 1 || env.timerCount(timerTagHeartbeat) != 0 {
		t.Fatalf("timers election=%d heartbeat=%d, want 1/0",
			env.timerCount(timerTagElection), env.timerCount(timerTagHeartbeat))
	}
}

func TestLogFileSizeGuard(t *testing.T) {
	env := newTestEnv(1)
	n := bootTestNode(t, env, 1, []sim.NodeID{2, 3})
	env.clearTraffic()
	n.logBytes = maxLogFileSize

	mustPanic(t, "log file size guard", func() {
		n.beginLogPersist(logBatch{
			baseIndex: 1,
			entries:   []Entry{logEntry(1, 1, EntryNoop, "")},
		}, actionLeaderNoop, requestSummary{})
	})
	if n.busy || len(env.appends) != 0 {
		t.Fatalf("size guard issued a write: busy=%t appends=%d", n.busy, len(env.appends))
	}
}

func TestRecoveryRejectsLogTermAheadOfMeta(t *testing.T) {
	env := newTestEnv(2)
	env.durable[metaFile] = encodeMeta(1, 0)
	env.durable[logFile] = encodeLogBatch(logBatch{
		baseIndex: 1,
		entries:   []Entry{logEntry(1, 2, EntryNoop, "")},
	})
	factory := NewFactory(Config{ID: 2, Peers: []sim.NodeID{1, 3}})
	n := factory(env).(*Node)
	n.Start()

	mustPanic(t, "recover log term ahead of meta", func() {
		completeBoot(t, env, n)
	})
}

func TestAppendRequestCountDoesNotFit(t *testing.T) {
	payload := appendU16(nil, wireVersion)
	payload = appendU64(payload, 1)
	payload = appendU16(payload, 1)
	payload = appendU64(payload, 0)
	payload = appendU64(payload, 0)
	payload = appendU32(payload, 1<<20)

	mustPanic(t, "oversized append entry count", func() {
		decodeAppendRequest(payload)
	})
}

func TestFollowerAppendAfterRestartIsIdempotent(t *testing.T) {
	env := newTestEnv(2)
	n := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	env.clearTraffic()
	n.HandleMessage(heartbeatMessage(1, 1))
	completePersist(t, env, n)
	env.clearTraffic()

	req := AppendRequest{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{
			logEntry(1, 1, EntryNoop, ""),
			logEntry(2, 1, EntryCommand, "x"),
		},
	}
	n.HandleMessage(appendRequestMessage(1, req))
	completePersist(t, env, n)
	if len(env.sent) != 1 {
		t.Fatalf("initial append responses = %d, want 1", len(env.sent))
	}

	env.crash()
	restarted := bootTestNode(t, env, 2, []sim.NodeID{1, 3})
	if restarted.lastLogIndex() != 2 || restarted.durableLastIndex != 2 {
		t.Fatalf("restart log last/durable = %d/%d, want 2/2",
			restarted.lastLogIndex(), restarted.durableLastIndex)
	}
	env.clearTraffic()

	restarted.HandleMessage(appendRequestMessage(1, req))
	if restarted.busy || len(env.appends) != 0 || len(env.replaces) != 0 {
		t.Fatalf("duplicate after restart persisted: busy=%t appends=%d replaces=%d",
			restarted.busy, len(env.appends), len(env.replaces))
	}
	if len(env.sent) != 1 {
		t.Fatalf("duplicate after restart responses = %d, want 1", len(env.sent))
	}
	resp := decodeAppendResponse(env.sent[0].payload)
	if resp.Term != 1 || !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("duplicate response = %+v, want term 1 success match 2", resp)
	}
}

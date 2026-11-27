package raft

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"

	"raftkv/internal/sim"
)

// ViewSource resolves a live, recovered node to a View. It is injectable so
// the Election Safety logic can be unit-tested without a broken production
// node.
type ViewSource func(s *sim.Sim, id sim.NodeID) (View, bool)

// ElectionSafety enforces Raft's Election Safety property: at most one leader
// per term, over the entire run. It also tracks durable term and vote
// monotonicity across crashes and restarts.
type ElectionSafety struct {
	ids    []sim.NodeID
	source ViewSource

	leadersByTerm   map[uint64]sim.NodeID
	lastDurableTerm map[sim.NodeID]uint64
	lastDurableVote map[sim.NodeID]sim.NodeID
	lastTerm        map[sim.NodeID]uint64
	lastIncarnation map[sim.NodeID]uint64
}

// NewElectionSafety returns an invariant that reads production Raft nodes
// through View().
func NewElectionSafety(ids []sim.NodeID) *ElectionSafety {
	return NewElectionSafetyWithSource(ids, liveRaftView)
}

// NewElectionSafetyWithSource returns an invariant backed by a custom view
// source. The source must be deterministic and must not mutate simulation
// state.
func NewElectionSafetyWithSource(ids []sim.NodeID, source ViewSource) *ElectionSafety {
	if source == nil {
		panic("raft: nil ElectionSafety view source")
	}
	cp := append([]sim.NodeID(nil), ids...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return &ElectionSafety{
		ids:             cp,
		source:          source,
		leadersByTerm:   make(map[uint64]sim.NodeID),
		lastDurableTerm: make(map[sim.NodeID]uint64),
		lastDurableVote: make(map[sim.NodeID]sim.NodeID),
		lastTerm:        make(map[sim.NodeID]uint64),
		lastIncarnation: make(map[sim.NodeID]uint64),
	}
}

func (e *ElectionSafety) Name() string { return "raft.election-safety" }

func (e *ElectionSafety) Check(s *sim.Sim) error {
	for _, id := range e.ids {
		v, ok := e.source(s, id)
		if !ok || v.Recovering {
			continue
		}

		if prevInc, seen := e.lastIncarnation[id]; !seen || prevInc != v.Incarnation {
			// A restart recovers whatever was durable. It is not a term
			// decrease, but durable state must still never go backwards.
			e.lastTerm[id] = v.Term
			e.lastIncarnation[id] = v.Incarnation
		}

		if v.Term < v.DurableTerm {
			return fmt.Errorf("node %d term %d is below durable term %d", id, v.Term, v.DurableTerm)
		}
		if v.DurableTerm == 0 && v.DurableVotedFor != 0 {
			return fmt.Errorf("node %d has durable vote %d at term 0", id, v.DurableVotedFor)
		}
		if prev, seen := e.lastDurableTerm[id]; seen && v.DurableTerm < prev {
			return fmt.Errorf("node %d durable term went backwards: %d -> %d", id, prev, v.DurableTerm)
		}
		if v.Term < e.lastTerm[id] {
			return fmt.Errorf("node %d term went backwards within incarnation %d: %d -> %d",
				id, v.Incarnation, e.lastTerm[id], v.Term)
		}
		if prevTerm, seen := e.lastDurableTerm[id]; seen && v.DurableTerm == prevTerm {
			if prevVote := e.lastDurableVote[id]; prevVote != 0 && v.DurableVotedFor != prevVote {
				return fmt.Errorf("node %d durable vote changed at term %d: %d -> %d",
					id, v.DurableTerm, prevVote, v.DurableVotedFor)
			}
		}

		if v.Role == Leader {
			if v.Term < 1 {
				return fmt.Errorf("node %d is leader at term 0", id)
			}
			if v.DurableTerm != v.Term {
				return fmt.Errorf("node %d is leader at term %d but durable term is %d",
					id, v.Term, v.DurableTerm)
			}
			if v.VotedFor != id || v.DurableVotedFor != id {
				return fmt.Errorf("node %d is leader at term %d but its vote is %d (durable %d)",
					id, v.Term, v.VotedFor, v.DurableVotedFor)
			}
			if prev, seen := e.leadersByTerm[v.Term]; seen && prev != id {
				return fmt.Errorf("Election Safety violated: nodes %d and %d were both leader in term %d",
					prev, id, v.Term)
			}
			e.leadersByTerm[v.Term] = id
		}

		e.lastTerm[id] = v.Term
		e.lastDurableTerm[id] = v.DurableTerm
		e.lastDurableVote[id] = v.DurableVotedFor
	}
	return nil
}

func liveRaftView(s *sim.Sim, id sim.NodeID) (View, bool) {
	node, up := s.Node(id)
	if !up {
		return View{}, false
	}
	viewer, ok := node.(interface{ View() View })
	if !ok {
		panic(fmt.Sprintf("raft: node %d does not implement View()", id))
	}
	v := viewer.View()
	if v.Recovering {
		return View{}, false
	}
	return v, true
}

type entryIdentity struct {
	Term uint64
	Kind EntryKind
	Data string // hex-encoded, canonical
}

func identityOf(e Entry) entryIdentity {
	return entryIdentity{
		Term: e.Term,
		Kind: e.Kind,
		Data: hex.EncodeToString(e.Data),
	}
}

func (id entryIdentity) String() string {
	return fmt.Sprintf("term=%d kind=%s data=%s", id.Term, id.Kind, id.Data)
}

func entryDesc(e Entry) string {
	return fmt.Sprintf("index=%d term=%d kind=%s data=%x", e.Index, e.Term, e.Kind, e.Data)
}

func viewDesc(v View) string {
	return fmt.Sprintf("node %d inc=%d role=%s term=%d commit=%d applied=%d last=%d durable-last=%d",
		v.ID, v.Incarnation, v.Role, v.Term, v.CommitIndex, v.LastApplied, v.LastLogIndex, v.DurableLastIndex)
}

// ApplyRecorder records the first applied identity for each Raft index. It is
// written by the scenario Apply callback and read by the safety invariants, all
// on the simulator goroutine.
type ApplyRecorder struct {
	ids     []sim.NodeID
	known   map[sim.NodeID]bool
	applied map[uint64]entryIdentity
	err     error
}

// NewApplyRecorder returns a recorder for the given Raft node IDs. The IDs are
// copied and sorted so reporting is deterministic.
func NewApplyRecorder(ids []sim.NodeID) *ApplyRecorder {
	cp := append([]sim.NodeID(nil), ids...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	known := make(map[sim.NodeID]bool, len(cp))
	for _, id := range cp {
		known[id] = true
	}
	return &ApplyRecorder{
		ids:     cp,
		known:   known,
		applied: make(map[uint64]entryIdentity),
	}
}

// Apply records one applied entry. Reapplying the same entry at an index after
// a restart is idempotent; applying a different entry at the same index is a
// State Machine Safety violation.
func (r *ApplyRecorder) Apply(node sim.NodeID, e Entry) {
	if r.err != nil {
		return
	}
	if !r.known[node] {
		r.err = fmt.Errorf("State Machine Safety violated: node %d is not a registered Raft node", node)
		return
	}
	if e.Index == 0 {
		r.err = fmt.Errorf("State Machine Safety violated: node %d applied the log sentinel", node)
		return
	}
	if e.Kind != EntryNoop && e.Kind != EntryCommand {
		r.err = fmt.Errorf("State Machine Safety violated: node %d applied invalid entry %s at index %d",
			node, e.Kind, e.Index)
		return
	}
	id := identityOf(e)
	if prev, ok := r.applied[e.Index]; ok {
		if prev != id {
			r.err = fmt.Errorf("State Machine Safety violated at index %d: node %d applied %s, previously %s",
				e.Index, node, id, prev)
		}
		return
	}
	r.applied[e.Index] = id
}

// Err returns the first recorded State Machine Safety violation, if any.
func (r *ApplyRecorder) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}

func (r *ApplyRecorder) appliedIdentity(index uint64) (entryIdentity, bool) {
	id, ok := r.applied[index]
	return id, ok
}

// StateMachineSafety enforces the State Machine Safety property: no two nodes
// ever apply different entries at the same log index.
type StateMachineSafety struct {
	ids      []sim.NodeID
	source   ViewSource
	recorder *ApplyRecorder
}

// NewStateMachineSafety returns an invariant backed by the production Raft
// nodes and the scenario's apply recorder.
func NewStateMachineSafety(recorder *ApplyRecorder) *StateMachineSafety {
	return NewStateMachineSafetyWithSource(recorder, liveRaftView)
}

// NewStateMachineSafetyWithSource returns an invariant backed by a custom view
// source. The source must be deterministic and must not mutate simulation
// state.
func NewStateMachineSafetyWithSource(recorder *ApplyRecorder, source ViewSource) *StateMachineSafety {
	if recorder == nil {
		panic("raft: nil ApplyRecorder")
	}
	if source == nil {
		panic("raft: nil StateMachineSafety view source")
	}
	return &StateMachineSafety{
		ids:      append([]sim.NodeID(nil), recorder.ids...),
		source:   source,
		recorder: recorder,
	}
}

func (m *StateMachineSafety) Name() string { return "raft.state-machine-safety" }

func (m *StateMachineSafety) Check(s *sim.Sim) error {
	if m.recorder.err != nil {
		return m.recorder.err
	}
	for _, id := range m.ids {
		v, ok := m.source(s, id)
		if !ok || v.Recovering {
			continue
		}
		if v.LastApplied > v.CommitIndex {
			return fmt.Errorf("State Machine Safety violated: %s applied %d beyond commit %d",
				viewDesc(v), v.LastApplied, v.CommitIndex)
		}
		for i := uint64(1); i <= v.LastApplied; i++ {
			identity, recorded := m.recorder.appliedIdentity(i)
			if !recorded {
				return fmt.Errorf("State Machine Safety violated: %s applied index %d but no entry identity was recorded",
					viewDesc(v), i)
			}
			if i > v.LastLogIndex || int(i) >= len(v.Log) {
				return fmt.Errorf("State Machine Safety violated: %s applied index %d beyond its log (last %d)",
					viewDesc(v), i, v.LastLogIndex)
			}
			got := identityOf(v.Log[i])
			if got != identity {
				return fmt.Errorf("State Machine Safety violated: %s log at applied index %d is %s, recorded %s",
					viewDesc(v), i, got, identity)
			}
		}
	}
	return nil
}

// LogMatching enforces the Raft Log Matching property and the structural
// invariants of each live node's log.
type LogMatching struct {
	ids    []sim.NodeID
	source ViewSource
}

// NewLogMatching returns an invariant that reads production Raft nodes through
// View().
func NewLogMatching(ids []sim.NodeID) *LogMatching {
	return NewLogMatchingWithSource(ids, liveRaftView)
}

// NewLogMatchingWithSource returns an invariant backed by a custom view source.
func NewLogMatchingWithSource(ids []sim.NodeID, source ViewSource) *LogMatching {
	if source == nil {
		panic("raft: nil LogMatching view source")
	}
	cp := append([]sim.NodeID(nil), ids...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return &LogMatching{ids: cp, source: source}
}

func (m *LogMatching) Name() string { return "raft.log-matching" }

func (m *LogMatching) Check(s *sim.Sim) error {
	views := make([]View, 0, len(m.ids))
	for _, id := range m.ids {
		v, ok := m.source(s, id)
		if !ok || v.Recovering {
			continue
		}
		if err := checkLogView(v); err != nil {
			return err
		}
		views = append(views, v)
	}
	for i := 0; i < len(views); i++ {
		for j := i + 1; j < len(views); j++ {
			if err := compareLogPrefixes(views[i], views[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkLogView(v View) error {
	if len(v.Log) == 0 {
		return fmt.Errorf("Log Matching violated: %s has an empty log", viewDesc(v))
	}
	sentinel := v.Log[0]
	if sentinel.Index != 0 || sentinel.Term != 0 || sentinel.Kind != EntrySentinel || len(sentinel.Data) != 0 {
		return fmt.Errorf("Log Matching violated: %s sentinel is %s, want index=0 term=0 kind=sentinel with empty data",
			viewDesc(v), entryDesc(sentinel))
	}
	if v.LastLogIndex != uint64(len(v.Log)-1) {
		return fmt.Errorf("Log Matching violated: %s last log index %d does not match log length %d",
			viewDesc(v), v.LastLogIndex, len(v.Log))
	}
	if v.LastLogTerm != v.Log[len(v.Log)-1].Term {
		return fmt.Errorf("Log Matching violated: %s last log term %d does not match entry term %d",
			viewDesc(v), v.LastLogTerm, v.Log[len(v.Log)-1].Term)
	}
	for i := 1; i < len(v.Log); i++ {
		e := v.Log[i]
		if e.Index != uint64(i) {
			return fmt.Errorf("Log Matching violated: %s log position %d has index %d",
				viewDesc(v), i, e.Index)
		}
		if e.Term < 1 {
			return fmt.Errorf("Log Matching violated: %s has zero term at index %d",
				viewDesc(v), e.Index)
		}
		if e.Term < v.Log[i-1].Term {
			return fmt.Errorf("Log Matching violated: %s terms decrease at index %d: %d -> %d",
				viewDesc(v), e.Index, v.Log[i-1].Term, e.Term)
		}
		if e.Kind != EntryNoop && e.Kind != EntryCommand {
			return fmt.Errorf("Log Matching violated: %s has invalid kind %s at index %d",
				viewDesc(v), e.Kind, e.Index)
		}
		if len(e.Data) > maxEntryBytes {
			return fmt.Errorf("Log Matching violated: %s entry at index %d has %d data bytes, cap %d",
				viewDesc(v), e.Index, len(e.Data), maxEntryBytes)
		}
	}
	if v.CommitIndex > v.LastLogIndex {
		return fmt.Errorf("Log Matching violated: %s commit %d exceeds last log index %d",
			viewDesc(v), v.CommitIndex, v.LastLogIndex)
	}
	if v.CommitIndex > v.DurableLastIndex {
		return fmt.Errorf("Log Matching violated: %s commit %d exceeds durable last index %d",
			viewDesc(v), v.CommitIndex, v.DurableLastIndex)
	}
	if v.LastApplied > v.CommitIndex {
		return fmt.Errorf("Log Matching violated: %s applied %d exceeds commit %d",
			viewDesc(v), v.LastApplied, v.CommitIndex)
	}
	if v.LastApplied > v.LastLogIndex {
		return fmt.Errorf("Log Matching violated: %s applied %d exceeds last log index %d",
			viewDesc(v), v.LastApplied, v.LastLogIndex)
	}
	return nil
}

func compareLogPrefixes(a, b View) error {
	limit := a.LastLogIndex
	if b.LastLogIndex < limit {
		limit = b.LastLogIndex
	}
	mismatch := false
	firstMismatch := uint64(0)
	for i := uint64(1); i <= limit; i++ {
		ae, be := a.Log[i], b.Log[i]
		sameTerm := ae.Term == be.Term
		sameEntry := sameTerm && ae.Kind == be.Kind && bytes.Equal(ae.Data, be.Data)
		if sameTerm && (mismatch || !sameEntry) {
			first := firstMismatch
			if !mismatch {
				first = i
			}
			return fmt.Errorf("Log Matching violated: nodes %d and %d share term %d at index %d but their prefixes differ at index %d: %s; %s",
				a.ID, b.ID, ae.Term, i, first, entryDesc(ae), entryDesc(be))
		}
		if !sameEntry {
			if !mismatch {
				firstMismatch = i
			}
			mismatch = true
		}
	}
	return nil
}

type leaderCheck struct {
	incarnation  uint64
	term         uint64
	lastLogIndex uint64
	maxCommitted uint64
}

// CommitDurability enforces two properties over the whole run:
//
//   - a committed entry never disappears, including across crashes/restarts;
//   - every future leader contains every previously committed entry.
//
// Leader Completeness is qualified by the term in which an entry was first
// observed committed. A stale leader in an old term may legally be partitioned
// from a newer majority that has already committed later entries; it is only
// required to contain entries committed at or before its own term.
type CommitDurability struct {
	ids    []sim.NodeID
	source ViewSource

	recorder *ApplyRecorder

	committed       map[uint64]entryIdentity
	committedTerm   map[uint64]uint64
	maxCommitted    uint64
	nodeMaxCommit   map[sim.NodeID]uint64
	nodeCommitSeen  map[sim.NodeID]uint64
	nodeIncarnation map[sim.NodeID]uint64
	leaderVerified  map[sim.NodeID]leaderCheck
}

// NewCommitDurability returns an invariant backed by production Raft nodes.
// The recorder is accepted for symmetry with StateMachineSafety; committed
// entry tracking is independent of the applied-entry record.
func NewCommitDurability(ids []sim.NodeID, recorder *ApplyRecorder) *CommitDurability {
	return NewCommitDurabilityWithSource(ids, recorder, liveRaftView)
}

// NewCommitDurabilityWithSource returns an invariant backed by a custom view
// source.
func NewCommitDurabilityWithSource(ids []sim.NodeID, recorder *ApplyRecorder, source ViewSource) *CommitDurability {
	if source == nil {
		panic("raft: nil CommitDurability view source")
	}
	cp := append([]sim.NodeID(nil), ids...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return &CommitDurability{
		ids:             cp,
		source:          source,
		recorder:        recorder,
		committed:       make(map[uint64]entryIdentity),
		committedTerm:   make(map[uint64]uint64),
		nodeMaxCommit:   make(map[sim.NodeID]uint64),
		nodeCommitSeen:  make(map[sim.NodeID]uint64),
		nodeIncarnation: make(map[sim.NodeID]uint64),
		leaderVerified:  make(map[sim.NodeID]leaderCheck),
	}
}

func (c *CommitDurability) Name() string { return "raft.commit-durability" }

func (c *CommitDurability) Check(s *sim.Sim) error {
	for _, id := range c.ids {
		v, ok := c.source(s, id)
		if !ok || v.Recovering {
			continue
		}

		if prev, seen := c.nodeIncarnation[id]; !seen || prev != v.Incarnation {
			if err := c.checkIncarnation(v); err != nil {
				return err
			}
			c.nodeIncarnation[id] = v.Incarnation
			c.nodeCommitSeen[id] = 0
		}

		if v.CommitIndex < c.nodeCommitSeen[id] {
			return fmt.Errorf("Commit Durability violated: %s commit index went backwards within incarnation: %d -> %d",
				viewDesc(v), c.nodeCommitSeen[id], v.CommitIndex)
		}
		for i := c.nodeCommitSeen[id] + 1; i <= v.CommitIndex; i++ {
			if i > v.LastLogIndex || int(i) >= len(v.Log) {
				return fmt.Errorf("Commit Durability violated: %s committed index %d is beyond its log (last %d)",
					viewDesc(v), i, v.LastLogIndex)
			}
			identity := identityOf(v.Log[i])
			if prev, seen := c.committed[i]; seen {
				if prev != identity {
					return fmt.Errorf("Commit Durability violated: committed index %d conflicts on %s: recorded %s, node has %s",
						i, viewDesc(v), prev, identity)
				}
			} else {
				c.committed[i] = identity
				c.committedTerm[i] = v.Term
				if i > c.maxCommitted {
					c.maxCommitted = i
				}
			}
			c.nodeCommitSeen[id] = i
			if i > c.nodeMaxCommit[id] {
				c.nodeMaxCommit[id] = i
			}
		}
		if v.CommitIndex > c.nodeMaxCommit[id] {
			c.nodeMaxCommit[id] = v.CommitIndex
		}

		if v.Role == Leader {
			if err := c.checkLeader(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CommitDurability) checkIncarnation(v View) error {
	high := c.nodeMaxCommit[v.ID]
	if high == 0 {
		return nil
	}
	if v.LastLogIndex < high {
		return fmt.Errorf("Commit Durability violated: %s restarted with last log index %d below committed high-water %d",
			viewDesc(v), v.LastLogIndex, high)
	}
	for i := uint64(1); i <= high; i++ {
		recorded, ok := c.committed[i]
		if !ok {
			return fmt.Errorf("Commit Durability violated: %s restarted but committed index %d has no recorded identity",
				viewDesc(v), i)
		}
		if i > v.LastLogIndex || int(i) >= len(v.Log) {
			return fmt.Errorf("Commit Durability violated: %s restarted without committed index %d (last %d)",
				viewDesc(v), i, v.LastLogIndex)
		}
		got := identityOf(v.Log[i])
		if got != recorded {
			return fmt.Errorf("Commit Durability violated: %s restarted with %s at committed index %d, recorded %s",
				viewDesc(v), got, i, recorded)
		}
	}
	return nil
}

func (c *CommitDurability) checkLeader(v View) error {
	key := leaderCheck{
		incarnation:  v.Incarnation,
		term:         v.Term,
		lastLogIndex: v.LastLogIndex,
		maxCommitted: c.maxCommitted,
	}
	if prev, ok := c.leaderVerified[v.ID]; ok && prev == key {
		return nil
	}
	eligibleCommitted := uint64(0)
	for i := uint64(1); i <= c.maxCommitted; i++ {
		if commitTerm, ok := c.committedTerm[i]; ok && commitTerm > v.Term {
			// This entry was committed by a later term. A leader in an
			// older term may not have heard of it yet.
			continue
		}
		eligibleCommitted = i
	}
	if v.LastLogIndex < eligibleCommitted {
		return fmt.Errorf("Commit Durability violated: leader %s has last log index %d below committed index %d",
			viewDesc(v), v.LastLogIndex, eligibleCommitted)
	}
	for i := uint64(1); i <= c.maxCommitted; i++ {
		if commitTerm, ok := c.committedTerm[i]; ok && commitTerm > v.Term {
			continue
		}
		recorded, ok := c.committed[i]
		if !ok {
			return fmt.Errorf("Commit Durability violated: leader %s is missing recorded identity for committed index %d",
				viewDesc(v), i)
		}
		if int(i) >= len(v.Log) {
			return fmt.Errorf("Commit Durability violated: leader %s is missing committed index %d (last %d)",
				viewDesc(v), i, v.LastLogIndex)
		}
		got := identityOf(v.Log[i])
		if got != recorded {
			return fmt.Errorf("Commit Durability violated: leader %s has %s at committed index %d, recorded %s",
				viewDesc(v), got, i, recorded)
		}
	}
	c.leaderVerified[v.ID] = key
	return nil
}

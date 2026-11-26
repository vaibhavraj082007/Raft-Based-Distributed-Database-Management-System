package raft

import (
	"fmt"

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

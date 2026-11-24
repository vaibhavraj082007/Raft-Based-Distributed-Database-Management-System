package sim

import "fmt"

type NetworkConfig struct {
	MinLatency   Tick
	MaxLatency   Tick
	DropPermille int
}

type blockKey struct {
	from NodeID
	to   NodeID
}

type network struct {
	sim     *Sim
	cfg     NetworkConfig
	blocked map[blockKey]bool
}

func newNetwork(s *Sim, cfg NetworkConfig) *network {
	return &network{sim: s, cfg: cfg, blocked: make(map[blockKey]bool)}
}

func (n *network) send(from, to NodeID, kind string, payload []byte) {
	s := n.sim
	s.stats.MessagesSent++
	msg := Message{From: from, To: to, Kind: kind, Payload: cloneBytes(payload)}
	s.tracef("send from=%d to=%d kind=%s bytes=%d", from, to, kind, len(msg.Payload))

	dest, ok := s.nodes[to]
	if !ok {
		n.drop("unknown-node", msg)
		return
	}
	if !dest.up {
		n.drop("down", msg)
		return
	}
	if n.blocked[blockKey{from: from, to: to}] {
		n.drop("partition", msg)
		return
	}
	if n.cfg.DropPermille > 0 && s.Chance(n.cfg.DropPermille) {
		n.drop("loss", msg)
		return
	}

	delay := Tick(s.Int64Range(int64(n.cfg.MinLatency), int64(n.cfg.MaxLatency)))
	at := s.now + delay
	if at < s.now {
		panic("sim: network latency overflow")
	}
	destEpoch := dest.epoch
	err := s.schedule(at, "deliver", fmt.Sprintf("from=%d to=%d kind=%s", from, to, kind), func(s *Sim) error {
		ns, ok := s.nodes[msg.To]
		if !ok || !ns.up {
			n.drop("down", msg)
			return nil
		}
		if ns.epoch != destEpoch {
			n.drop("stale-epoch", msg)
			return nil
		}
		if n.blocked[blockKey{from: msg.From, to: msg.To}] {
			n.drop("partition", msg)
			return nil
		}
		s.stats.MessagesDelivered++
		s.tracef("recv from=%d to=%d kind=%s bytes=%d", msg.From, msg.To, msg.Kind, len(msg.Payload))
		ns.node.HandleMessage(msg)
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (n *network) drop(reason string, msg Message) {
	n.sim.stats.MessagesDropped++
	n.sim.tracef("drop from=%d to=%d kind=%s reason=%s", msg.From, msg.To, msg.Kind, reason)
}

func (n *network) block(from, to NodeID) {
	n.blocked[blockKey{from: from, to: to}] = true
}

func (n *network) unblock(from, to NodeID) {
	delete(n.blocked, blockKey{from: from, to: to})
}

func (n *network) healAll() {
	n.blocked = make(map[blockKey]bool)
}

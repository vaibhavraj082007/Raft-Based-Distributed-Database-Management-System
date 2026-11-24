package sim

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"sort"
)

func (s *Sim) StateHash() uint64 {
	h := fnv.New64a()
	writeU64(h, Version)
	writeU64(h, s.cfg.Seed)
	writeString(h, s.cfg.Scenario)
	writeI64(h, int64(s.now))
	writeU64(h, s.step)
	writeU64(h, s.nextSeq)
	writeU64(h, s.nextTimerID)
	writeU64(h, s.rng.stateValue())
	writeU64(h, uint64(len(s.order)))
	for _, id := range s.order {
		ns := s.nodes[id]
		writeU64(h, uint64(id))
		if ns.up {
			writeU64(h, 1)
		} else {
			writeU64(h, 0)
		}
		writeU64(h, ns.epoch)
		if ns.up {
			if snap, ok := ns.node.(Snapshotter); ok {
				writeBytes(h, snap.Snapshot())
			} else {
				writeString(h, "<no-snapshot>")
			}
		}
	}

	timerIDs := make([]uint64, 0, len(s.timers))
	for id := range s.timers {
		timerIDs = append(timerIDs, uint64(id))
	}
	sort.Slice(timerIDs, func(i, j int) bool { return timerIDs[i] < timerIDs[j] })
	writeU64(h, uint64(len(timerIDs)))
	for _, raw := range timerIDs {
		ts := s.timers[TimerID(raw)]
		writeU64(h, raw)
		writeU64(h, uint64(ts.node))
		writeU64(h, ts.epoch)
		writeU64(h, ts.tag)
	}

	keys := make([]blockKey, 0, len(s.network.blocked))
	for k := range s.network.blocked {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].from != keys[j].from {
			return keys[i].from < keys[j].from
		}
		return keys[i].to < keys[j].to
	})
	writeU64(h, uint64(len(keys)))
	for _, k := range keys {
		writeU64(h, uint64(k.from))
		writeU64(h, uint64(k.to))
	}

	for _, id := range s.order {
		d := s.nodes[id].disk
		writeU64(h, uint64(id))
		writeI64(h, int64(d.busyUntil))
		writeU64(h, d.nextOp)
		names := make([]string, 0, len(d.files))
		for name := range d.files {
			names = append(names, name)
		}
		sort.Strings(names)
		writeU64(h, uint64(len(names)))
		for _, name := range names {
			f := d.files[name]
			writeString(h, name)
			writeBytes(h, f.durable)
			writeBytes(h, f.visible)
		}
	}

	queue := make([]*pendingEvent, len(s.queue))
	copy(queue, s.queue)
	sort.Slice(queue, func(i, j int) bool {
		if queue[i].at != queue[j].at {
			return queue[i].at < queue[j].at
		}
		return queue[i].seq < queue[j].seq
	})
	writeU64(h, uint64(len(queue)))
	for _, ev := range queue {
		writeI64(h, int64(ev.at))
		writeU64(h, ev.seq)
		writeString(h, ev.kind)
		writeString(h, ev.desc)
	}

	writeU64(h, s.stats.Events)
	writeU64(h, s.stats.MessagesSent)
	writeU64(h, s.stats.MessagesDelivered)
	writeU64(h, s.stats.MessagesDropped)
	writeU64(h, s.stats.DiskOps)
	writeU64(h, s.stats.Boots)
	writeU64(h, s.stats.Crashes)
	writeU64(h, s.stats.Restarts)
	return h.Sum64()
}

func writeU64(h hash.Hash64, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
}

func writeI64(h hash.Hash64, v int64) { writeU64(h, uint64(v)) }

func writeString(h hash.Hash64, s string) { writeBytes(h, []byte(s)) }

func writeBytes(h hash.Hash64, b []byte) {
	writeU64(h, uint64(len(b)))
	_, _ = h.Write(b)
}

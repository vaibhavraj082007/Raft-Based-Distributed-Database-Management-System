package sim

import (
	"fmt"
	"sort"
	"strings"
)

type DiskConfig struct {
	Latency Tick
}

type diskFile struct {
	durable []byte
	visible []byte
}

type diskOp struct {
	id    uint64
	epoch uint64
	tag   uint64
	kind  string
	name  string
	off   int64
	n     int64
	data  []byte
}

type disk struct {
	sim       *Sim
	node      NodeID
	latency   Tick
	files     map[string]*diskFile
	busyUntil Tick
	nextOp    uint64
}

func newDisk(s *Sim, node NodeID, latency Tick) *disk {
	return &disk{
		sim:     s,
		node:    node,
		latency: latency,
		files:   make(map[string]*diskFile),
	}
}

func (d *disk) issue(epoch uint64, op diskOp) {
	d.nextOp++
	op.id = d.nextOp
	op.epoch = epoch
	op.data = cloneBytes(op.data)
	start := d.sim.now
	if d.busyUntil > start {
		start = d.busyUntil
	}
	done := start + d.latency
	if done < start {
		panic("sim: disk latency overflow")
	}
	d.busyUntil = done
	d.sim.stats.DiskOps++
	err := d.sim.schedule(done, "disk", fmt.Sprintf("node=%d op=%d kind=%s", d.node, op.id, op.kind), func(s *Sim) error {
		return d.complete(op)
	})
	if err != nil {
		panic(err)
	}
}

func (d *disk) complete(op diskOp) error {
	s := d.sim
	ns, ok := s.nodes[d.node]
	if !ok || !ns.up || ns.epoch != op.epoch {
		s.tracef("drop disk node=%d op=%d reason=stale", d.node, op.id)
		return nil
	}

	var data []byte
	var opErr error
	switch op.kind {
	case IORead:
		if op.off < 0 || op.n < 0 {
			opErr = fmt.Errorf("invalid read offset=%d n=%d", op.off, op.n)
			break
		}
		if f := d.files[op.name]; f != nil {
			if op.off < int64(len(f.visible)) {
				end := op.off + op.n
				if end > int64(len(f.visible)) {
					end = int64(len(f.visible))
				}
				data = cloneBytes(f.visible[op.off:end])
			}
		}
	case IOAppend:
		if err := validateFileName(op.name); err != nil {
			opErr = err
			break
		}
		f := d.file(op.name)
		f.visible = append(f.visible, op.data...)
	case IOReplace:
		if err := validateFileName(op.name); err != nil {
			opErr = err
			break
		}
		f := d.file(op.name)
		f.visible = cloneBytes(op.data)
	case IOSync:
		for _, f := range d.files {
			f.durable = cloneBytes(f.visible)
		}
	case IOList:
		var names []string
		for name := range d.files {
			names = append(names, name)
		}
		sort.Strings(names)
		var b strings.Builder
		for _, name := range names {
			fmt.Fprintf(&b, "%s\t%d\n", name, len(d.files[name].visible))
		}
		data = []byte(b.String())
	default:
		opErr = fmt.Errorf("unknown disk operation %q", op.kind)
	}

	if opErr != nil {
		s.tracef("io node=%d op=%d kind=%s tag=%d error=%s", d.node, op.id, op.kind, op.tag, opErr)
	} else {
		s.tracef("io node=%d op=%d kind=%s tag=%d bytes=%d", d.node, op.id, op.kind, op.tag, len(data))
	}
	ns.node.HandleIO(IOEvent{Tag: op.tag, Op: op.kind, Data: data, Err: opErr})
	return nil
}

func (d *disk) file(name string) *diskFile {
	f := d.files[name]
	if f == nil {
		f = &diskFile{}
		d.files[name] = f
	}
	return f
}

func (d *disk) crash() {
	for _, f := range d.files {
		f.visible = cloneBytes(f.durable)
	}
	d.busyUntil = 0
}

func validateFileName(name string) error {
	if name == "" {
		return fmt.Errorf("empty file name")
	}
	if strings.ContainsAny(name, "\t\n\r") {
		return fmt.Errorf("invalid file name %q", name)
	}
	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

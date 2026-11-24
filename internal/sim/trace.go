package sim

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"strings"
)

// TraceLevel controls how much detail is written to the trace.
type TraceLevel int

const (
	TraceEvents TraceLevel = iota
	TraceDecisions
)

func (l TraceLevel) String() string {
	switch l {
	case TraceEvents:
		return "events"
	case TraceDecisions:
		return "decisions"
	default:
		return fmt.Sprintf("invalid(%d)", int(l))
	}
}

const traceTailLimit = 200

type tracer struct {
	w     io.Writer
	hash  hash.Hash64
	level TraceLevel
	tail  []string
	err   error
}

func newTracer(w io.Writer, level TraceLevel) *tracer {
	if w == nil {
		w = io.Discard
	}
	return &tracer{w: w, hash: fnv.New64a(), level: level}
}

func (t *tracer) writeRaw(line string) {
	if t.err != nil {
		return
	}
	if _, err := io.WriteString(t.w, line); err != nil {
		t.err = err
		return
	}
	_, _ = io.WriteString(t.hash, line)
	t.tail = append(t.tail, strings.TrimSuffix(line, "\n"))
	if len(t.tail) > traceTailLimit*2 {
		t.tail = append([]string(nil), t.tail[len(t.tail)-traceTailLimit:]...)
	}
}

func (t *tracer) line(now Tick, step uint64, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	msg = strings.ReplaceAll(msg, "\n", `\n`)
	t.writeRaw(fmt.Sprintf("t=%d step=%d %s\n", now, step, msg))
}

func (t *tracer) sum64() uint64 { return t.hash.Sum64() }

func (t *tracer) tailCopy(n int) []string {
	if n <= 0 || len(t.tail) == 0 {
		return nil
	}
	if n > len(t.tail) {
		n = len(t.tail)
	}
	out := make([]string, n)
	copy(out, t.tail[len(t.tail)-n:])
	return out
}

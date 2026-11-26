package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	logFile         = "raft.log"
	maxLogFileSize  = 4 << 20
	maxEntryBytes   = 4096
	maxClientBytes  = 1024
	tagRecoverLog   = uint64(2)
	logRecordHeader = 24
	minEntryBytes   = 8 + 8 + 1 + 1 + 4
)

var logMagic = [4]byte{'R', '2', 'L', 'G'}

const (
	logVersion    uint16 = 1
	logRecordKind uint8  = 1
)

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		panic(fmt.Sprintf("raft: node %d missing log sentinel", n.id))
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		panic(fmt.Sprintf("raft: node %d missing log sentinel", n.id))
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) firstIndexOfTerm(term uint64) (uint64, bool) {
	for i := 1; i < len(n.log); i++ {
		if n.log[i].Term == term {
			return n.log[i].Index, true
		}
		if n.log[i].Term > term {
			return 0, false
		}
	}
	return 0, false
}

func (n *Node) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := len(n.log) - 1; i >= 1; i-- {
		if n.log[i].Term == term {
			return n.log[i].Index, true
		}
		if n.log[i].Term < term {
			return 0, false
		}
	}
	return 0, false
}

func (n *Node) appendLocal(kind EntryKind, data []byte) Entry {
	if kind != EntryNoop && kind != EntryCommand {
		panic(fmt.Sprintf("raft: node %d cannot append kind %s", n.id, kind))
	}
	if len(data) > maxEntryBytes {
		panic(fmt.Sprintf("raft: node %d entry data %d exceeds cap %d", n.id, len(data), maxEntryBytes))
	}
	e := Entry{
		Index: n.lastLogIndex() + 1,
		Term:  n.currentTerm,
		Kind:  kind,
		Data:  cloneEntryData(data),
	}
	n.log = append(n.log, e)
	return e
}

func cloneEntryData(data []byte) []byte {
	if data == nil {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func cloneEntry(e Entry) Entry {
	e.Data = cloneEntryData(e.Data)
	return e
}

func cloneEntries(entries []Entry) []Entry {
	if entries == nil {
		return nil
	}
	out := make([]Entry, len(entries))
	for i, e := range entries {
		out[i] = cloneEntry(e)
	}
	return out
}

func cloneRequest(req requestSummary) requestSummary {
	req.entries = cloneEntries(req.entries)
	return req
}

func encodeLogBatch(batch logBatch) []byte {
	if len(batch.entries) == 0 {
		panic("raft: cannot encode empty log batch")
	}
	if batch.baseIndex != batch.entries[0].Index {
		panic(fmt.Sprintf("raft: log batch base %d does not match first entry %d",
			batch.baseIndex, batch.entries[0].Index))
	}

	payloadLen := 0
	for i, e := range batch.entries {
		if e.Index != batch.baseIndex+uint64(i) {
			panic(fmt.Sprintf("raft: log batch entry %d has index %d, want %d",
				i, e.Index, batch.baseIndex+uint64(i)))
		}
		if e.Term < 1 {
			panic(fmt.Sprintf("raft: log batch entry %d has term %d", i, e.Term))
		}
		if e.Kind != EntryNoop && e.Kind != EntryCommand {
			panic(fmt.Sprintf("raft: log batch entry %d has kind %s", i, e.Kind))
		}
		if len(e.Data) > maxEntryBytes {
			panic(fmt.Sprintf("raft: log batch entry %d data %d exceeds cap %d",
				i, len(e.Data), maxEntryBytes))
		}
		payloadLen += minEntryBytes + len(e.Data)
	}
	if uint64(len(batch.entries)) > uint64(^uint32(0)) {
		panic("raft: log batch entry count overflow")
	}
	if uint64(payloadLen) > uint64(^uint32(0)) {
		panic("raft: log batch payload length overflow")
	}

	out := make([]byte, 0, logRecordHeader+payloadLen+4)
	out = append(out, logMagic[:]...)
	out = appendU16(out, logVersion)
	out = append(out, logRecordKind)
	out = append(out, 0)
	out = appendU64(out, batch.baseIndex)
	out = appendU32(out, uint32(len(batch.entries)))
	out = appendU32(out, uint32(payloadLen))
	for _, e := range batch.entries {
		out = appendU64(out, e.Index)
		out = appendU64(out, e.Term)
		out = append(out, byte(e.Kind))
		out = append(out, 0)
		out = appendU32(out, uint32(len(e.Data)))
		out = append(out, e.Data...)
	}
	crc := crc32.ChecksumIEEE(out)
	out = appendU32(out, crc)
	return out
}

// decodeLogFile replays framed batches into the current log. Corruption or an
// impossible record is a deterministic boot failure.
func decodeLogFile(data []byte) []Entry {
	log := []Entry{{Index: 0, Term: 0, Kind: EntrySentinel}}
	off := 0
	for off < len(data) {
		if len(data)-off < logRecordHeader {
			panic(fmt.Sprintf("raft: raft.log has trailing partial header at offset %d", off))
		}
		header := data[off : off+logRecordHeader]
		for i := 0; i < 4; i++ {
			if header[i] != logMagic[i] {
				panic(fmt.Sprintf("raft: raft.log bad magic at offset %d", off))
			}
		}
		if v := binary.BigEndian.Uint16(header[4:6]); v != logVersion {
			panic(fmt.Sprintf("raft: raft.log version %d at offset %d, want %d", v, off, logVersion))
		}
		if kind := header[6]; kind != logRecordKind {
			panic(fmt.Sprintf("raft: raft.log record kind %d at offset %d", kind, off))
		}
		if reserved := header[7]; reserved != 0 {
			panic(fmt.Sprintf("raft: raft.log reserved byte %d at offset %d", reserved, off))
		}
		baseIndex := binary.BigEndian.Uint64(header[8:16])
		count := binary.BigEndian.Uint32(header[16:20])
		payloadLen := binary.BigEndian.Uint32(header[20:24])
		if count == 0 {
			panic(fmt.Sprintf("raft: raft.log empty batch at offset %d", off))
		}
		if uint64(count) > uint64(payloadLen)/minEntryBytes {
			panic(fmt.Sprintf("raft: raft.log batch count %d does not fit payload %d at offset %d",
				count, payloadLen, off))
		}
		if len(data)-off < logRecordHeader+4 {
			panic(fmt.Sprintf("raft: raft.log has trailing partial record at offset %d", off))
		}
		if uint64(payloadLen) > uint64(len(data)-off-logRecordHeader-4) {
			panic(fmt.Sprintf("raft: raft.log truncated payload at offset %d", off))
		}
		recordLen := logRecordHeader + int(payloadLen) + 4
		record := data[off : off+recordLen]
		wantCRC := binary.BigEndian.Uint32(record[len(record)-4:])
		gotCRC := crc32.ChecksumIEEE(record[:len(record)-4])
		if gotCRC != wantCRC {
			panic(fmt.Sprintf("raft: raft.log CRC %08x at offset %d, want %08x", gotCRC, off, wantCRC))
		}

		payload := record[logRecordHeader : logRecordHeader+int(payloadLen)]
		entries := make([]Entry, 0, count)
		pos := 0
		for i := uint32(0); i < count; i++ {
			if len(payload)-pos < minEntryBytes {
				panic(fmt.Sprintf("raft: raft.log truncated entry %d at offset %d", i, off))
			}
			index := binary.BigEndian.Uint64(payload[pos : pos+8])
			term := binary.BigEndian.Uint64(payload[pos+8 : pos+16])
			kind := EntryKind(payload[pos+16])
			reserved := payload[pos+17]
			dataLen := binary.BigEndian.Uint32(payload[pos+18 : pos+22])
			pos += minEntryBytes
			if reserved != 0 {
				panic(fmt.Sprintf("raft: raft.log entry %d reserved byte %d", i, reserved))
			}
			if dataLen > maxEntryBytes {
				panic(fmt.Sprintf("raft: raft.log entry %d data %d exceeds cap %d", i, dataLen, maxEntryBytes))
			}
			if uint64(dataLen) > uint64(len(payload)-pos) {
				panic(fmt.Sprintf("raft: raft.log entry %d truncated data at offset %d", i, off))
			}
			data := cloneEntryData(payload[pos : pos+int(dataLen)])
			pos += int(dataLen)
			entries = append(entries, Entry{Index: index, Term: term, Kind: kind, Data: data})
		}
		if pos != len(payload) {
			panic(fmt.Sprintf("raft: raft.log batch at offset %d has %d trailing payload bytes", off, len(payload)-pos))
		}

		if baseIndex < 1 || baseIndex > uint64(len(log)) {
			panic(fmt.Sprintf("raft: raft.log batch base %d outside [1,%d] at offset %d",
				baseIndex, len(log), off))
		}
		if baseIndex < uint64(len(log)) {
			log = log[:baseIndex]
		}
		prevTerm := log[baseIndex-1].Term
		expected := baseIndex
		for i, e := range entries {
			if e.Index != expected {
				panic(fmt.Sprintf("raft: raft.log batch entry %d index %d, want %d at offset %d",
					i, e.Index, expected, off))
			}
			if e.Term < 1 {
				panic(fmt.Sprintf("raft: raft.log entry %d term %d at offset %d", i, e.Term, off))
			}
			if e.Term < prevTerm {
				panic(fmt.Sprintf("raft: raft.log entry %d term %d below previous term %d at offset %d",
					i, e.Term, prevTerm, off))
			}
			if e.Kind != EntryNoop && e.Kind != EntryCommand {
				panic(fmt.Sprintf("raft: raft.log entry %d kind %s at offset %d", i, e.Kind, off))
			}
			if len(e.Data) > maxEntryBytes {
				panic(fmt.Sprintf("raft: raft.log entry %d data %d exceeds cap %d", i, len(e.Data), maxEntryBytes))
			}
			log = append(log, e)
			prevTerm = e.Term
			expected++
		}
		off += recordLen
	}
	return log
}

func appendU32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

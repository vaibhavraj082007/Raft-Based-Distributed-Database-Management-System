package raft

import (
	"encoding/binary"
	"fmt"

	"raftkv/internal/sim"
)

const wireVersion uint16 = 1

// Message kinds. Every payload begins with wireVersion.
const (
	KindVoteRequest    = "raft.vote-req"
	KindVoteResponse   = "raft.vote-resp"
	KindAppendRequest  = "raft.append-req"
	KindAppendResponse = "raft.append-resp"
)

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

// AppendRequest is an empty heartbeat in Stage 2A. The log fields are present
// for wire compatibility and must be zero until Stage 2B adds entries.
type AppendRequest struct {
	Term         uint64
	LeaderID     sim.NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
}

type AppendResponse struct {
	Term    uint64
	Success bool
}

func encodeVoteRequest(req VoteRequest) []byte {
	b := make([]byte, 0, 28)
	b = appendU16(b, wireVersion)
	b = appendU64(b, req.Term)
	b = appendU16(b, uint16(req.CandidateID))
	b = appendU64(b, req.LastLogIndex)
	b = appendU64(b, req.LastLogTerm)
	return b
}

func decodeVoteRequest(payload []byte) VoteRequest {
	d := newDecoder(payload, KindVoteRequest)
	req := VoteRequest{
		Term:         d.u64(),
		CandidateID:  sim.NodeID(d.u16()),
		LastLogIndex: d.u64(),
		LastLogTerm:  d.u64(),
	}
	d.done()
	return req
}

func encodeVoteResponse(resp VoteResponse) []byte {
	b := make([]byte, 0, 11)
	b = appendU16(b, wireVersion)
	b = appendU64(b, resp.Term)
	b = appendU8(b, boolByte(resp.VoteGranted))
	return b
}

func decodeVoteResponse(payload []byte) VoteResponse {
	d := newDecoder(payload, KindVoteResponse)
	resp := VoteResponse{
		Term:        d.u64(),
		VoteGranted: d.boolean(),
	}
	d.done()
	return resp
}

func encodeAppendRequest(req AppendRequest) []byte {
	b := make([]byte, 0, 36)
	b = appendU16(b, wireVersion)
	b = appendU64(b, req.Term)
	b = appendU16(b, uint16(req.LeaderID))
	b = appendU64(b, req.PrevLogIndex)
	b = appendU64(b, req.PrevLogTerm)
	b = appendU64(b, req.LeaderCommit)
	return b
}

func decodeAppendRequest(payload []byte) AppendRequest {
	d := newDecoder(payload, KindAppendRequest)
	req := AppendRequest{
		Term:         d.u64(),
		LeaderID:     sim.NodeID(d.u16()),
		PrevLogIndex: d.u64(),
		PrevLogTerm:  d.u64(),
		LeaderCommit: d.u64(),
	}
	d.done()
	return req
}

func encodeAppendResponse(resp AppendResponse) []byte {
	b := make([]byte, 0, 11)
	b = appendU16(b, wireVersion)
	b = appendU64(b, resp.Term)
	b = appendU8(b, boolByte(resp.Success))
	return b
}

func decodeAppendResponse(payload []byte) AppendResponse {
	d := newDecoder(payload, KindAppendResponse)
	resp := AppendResponse{
		Term:    d.u64(),
		Success: d.boolean(),
	}
	d.done()
	return resp
}

func appendU8(b []byte, v uint8) []byte { return append(b, v) }

func appendU16(b []byte, v uint16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	return append(b, tmp[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

type decoder struct {
	b    []byte
	off  int
	kind string
}

func newDecoder(payload []byte, kind string) *decoder {
	d := &decoder{b: payload, kind: kind}
	version := d.u16()
	if version != wireVersion {
		panic(fmt.Sprintf("raft: %s wire version %d, want %d", kind, version, wireVersion))
	}
	return d
}

func (d *decoder) u8() uint8 {
	if d.off+1 > len(d.b) {
		panic(fmt.Sprintf("raft: truncated %s payload", d.kind))
	}
	v := d.b[d.off]
	d.off++
	return v
}

func (d *decoder) u16() uint16 {
	if d.off+2 > len(d.b) {
		panic(fmt.Sprintf("raft: truncated %s payload", d.kind))
	}
	v := binary.BigEndian.Uint16(d.b[d.off:])
	d.off += 2
	return v
}

func (d *decoder) u64() uint64 {
	if d.off+8 > len(d.b) {
		panic(fmt.Sprintf("raft: truncated %s payload", d.kind))
	}
	v := binary.BigEndian.Uint64(d.b[d.off:])
	d.off += 8
	return v
}

func (d *decoder) boolean() bool {
	switch d.u8() {
	case 0:
		return false
	case 1:
		return true
	default:
		panic(fmt.Sprintf("raft: invalid boolean in %s payload", d.kind))
	}
}

func (d *decoder) done() {
	if d.off != len(d.b) {
		panic(fmt.Sprintf("raft: %s payload has %d trailing bytes", d.kind, len(d.b)-d.off))
	}
}

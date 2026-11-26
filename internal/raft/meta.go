package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"raftkv/internal/sim"
)

const (
	metaFile        = "raft.meta"
	metaSize        = 28
	maxMetaFileSize = 64
	tagRecoverMeta  = uint64(1)
	firstRuntimeTag = uint64(100)
)

var metaMagic = [4]byte{'R', '2', 'M', 'T'}

const metaVersion uint16 = 1

// encodeMeta produces the fixed 28-byte raft.meta record:
//
//	0..3   magic
//	4..5   version
//	6..7   flags (zero)
//	8..15  currentTerm
//	16..23 votedFor (zero-extended uint16)
//	24..27 CRC-32 IEEE over bytes 0..23
func encodeMeta(term uint64, vote sim.NodeID) []byte {
	b := make([]byte, metaSize)
	copy(b[0:4], metaMagic[:])
	binary.BigEndian.PutUint16(b[4:6], metaVersion)
	binary.BigEndian.PutUint16(b[6:8], 0)
	binary.BigEndian.PutUint64(b[8:16], term)
	binary.BigEndian.PutUint64(b[16:24], uint64(vote))
	binary.BigEndian.PutUint32(b[24:28], crc32.ChecksumIEEE(b[0:24]))
	return b
}

// decodeMeta accepts a missing/empty file as term 0, vote 0. Any other
// length or corruption is a deterministic boot failure.
func decodeMeta(b []byte) (uint64, sim.NodeID) {
	if len(b) == 0 {
		return 0, 0
	}
	if len(b) != metaSize {
		panic(fmt.Sprintf("raft: raft.meta is %d bytes, want %d", len(b), metaSize))
	}
	for i := 0; i < 4; i++ {
		if b[i] != metaMagic[i] {
			panic("raft: raft.meta has bad magic")
		}
	}
	if v := binary.BigEndian.Uint16(b[4:6]); v != metaVersion {
		panic(fmt.Sprintf("raft: raft.meta version %d, want %d", v, metaVersion))
	}
	if v := binary.BigEndian.Uint16(b[6:8]); v != 0 {
		panic(fmt.Sprintf("raft: raft.meta flags %d, want 0", v))
	}
	want := binary.BigEndian.Uint32(b[24:28])
	got := crc32.ChecksumIEEE(b[0:24])
	if got != want {
		panic(fmt.Sprintf("raft: raft.meta CRC %08x, want %08x", got, want))
	}
	vote64 := binary.BigEndian.Uint64(b[16:24])
	if vote64 > 0xffff {
		panic(fmt.Sprintf("raft: raft.meta votedFor %d exceeds uint16", vote64))
	}
	return binary.BigEndian.Uint64(b[8:16]), sim.NodeID(vote64)
}

package raft

import (
	"encoding/binary"
	"fmt"

	"raftkv/internal/sim"
)

type ClientRequest struct {
	ClientID sim.NodeID
	Seq      uint64
	Data     []byte
}

type ClientResponse struct {
	Term      uint64
	Accepted  bool
	Committed bool
	LeaderID  sim.NodeID
	Index     uint64
}

// EncodeClientRequest returns the deterministic client request payload used by
// the scenario client and by the leader's log entries.
func EncodeClientRequest(req ClientRequest) []byte {
	if len(req.Data) > maxClientBytes {
		panic(fmt.Sprintf("raft: client data %d exceeds cap %d", len(req.Data), maxClientBytes))
	}
	b := make([]byte, 0, 16+len(req.Data))
	b = appendU16(b, wireVersion)
	b = appendU16(b, uint16(req.ClientID))
	b = appendU64(b, req.Seq)
	b = appendU32(b, uint32(len(req.Data)))
	b = append(b, req.Data...)
	return b
}

func decodeClientRequest(payload []byte) ClientRequest {
	d := newDecoder(payload, KindClientRequest)
	req := ClientRequest{
		ClientID: sim.NodeID(d.u16()),
		Seq:      d.u64(),
	}
	n := d.u32()
	if n > maxClientBytes {
		panic(fmt.Sprintf("raft: %s data %d exceeds cap %d", KindClientRequest, n, maxClientBytes))
	}
	req.Data = cloneEntryData(d.bytes(int(n)))
	d.done()
	return req
}

func encodeClientResponse(resp ClientResponse) []byte {
	b := make([]byte, 0, 22)
	b = appendU16(b, wireVersion)
	b = appendU64(b, resp.Term)
	b = appendU8(b, boolByte(resp.Accepted))
	b = appendU8(b, boolByte(resp.Committed))
	b = appendU16(b, uint16(resp.LeaderID))
	b = appendU64(b, resp.Index)
	return b
}

// DecodeClientResponse parses a client response and reports malformed payloads
// as errors instead of panicking.
func DecodeClientResponse(payload []byte) (ClientResponse, error) {
	if len(payload) < 2 {
		return ClientResponse{}, fmt.Errorf("raft: truncated %s payload", KindClientResponse)
	}
	if v := binary.BigEndian.Uint16(payload[0:2]); v != wireVersion {
		return ClientResponse{}, fmt.Errorf("raft: %s wire version %d, want %d", KindClientResponse, v, wireVersion)
	}
	const size = 2 + 8 + 1 + 1 + 2 + 8
	if len(payload) != size {
		return ClientResponse{}, fmt.Errorf("raft: %s payload is %d bytes, want %d", KindClientResponse, len(payload), size)
	}
	accepted, err := decodeClientBool(payload[10])
	if err != nil {
		return ClientResponse{}, err
	}
	committed, err := decodeClientBool(payload[11])
	if err != nil {
		return ClientResponse{}, err
	}
	return ClientResponse{
		Term:      binary.BigEndian.Uint64(payload[2:10]),
		Accepted:  accepted,
		Committed: committed,
		LeaderID:  sim.NodeID(binary.BigEndian.Uint16(payload[12:14])),
		Index:     binary.BigEndian.Uint64(payload[14:22]),
	}, nil
}

func decodeClientBool(b byte) (bool, error) {
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("raft: invalid boolean %d in %s payload", b, KindClientResponse)
	}
}

func (n *Node) handleClientRequest(msg sim.Message) {
	req := decodeClientRequest(msg.Payload)
	if req.ClientID == 0 {
		panic(fmt.Sprintf("raft: node %d client request with zero client", n.id))
	}
	if req.Seq == 0 {
		panic(fmt.Sprintf("raft: node %d client request with zero sequence", n.id))
	}
	if msg.From != req.ClientID {
		panic(fmt.Sprintf("raft: node %d client request from %d claims client %d", n.id, msg.From, req.ClientID))
	}
	if len(req.Data) > maxClientBytes {
		panic(fmt.Sprintf("raft: node %d client data %d exceeds cap %d", n.id, len(req.Data), maxClientBytes))
	}

	if n.role != Leader {
		n.sendClientResponse(req.ClientID, ClientResponse{
			Term:     n.currentTerm,
			Accepted: false,
			LeaderID: n.leaderID,
		})
		return
	}

	data := EncodeClientRequest(req)
	entry := n.appendLocal(EntryCommand, data)
	n.beginLogPersist(logBatch{baseIndex: entry.Index, entries: []Entry{entry}}, actionLeaderClientAppend,
		requestSummary{clientID: req.ClientID, clientSeq: req.Seq})
}

func (n *Node) sendClientResponse(client sim.NodeID, resp ClientResponse) {
	n.env.Send(client, KindClientResponse, encodeClientResponse(resp))
}

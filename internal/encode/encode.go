// Package encode builds the push frames the bridge sends back up the tunnel.
//
// These are the exact inverse of what the retrieval plane decodes, and the two
// directions are deliberately kept in one repo so they cannot drift: a subtree
// that arrives as BRC-143 and one this cluster produced must be byte-identical
// on the wire.
//
// Every frame is self-verified against the shared codec before it is returned.
// The lanes carry bare objects — no length prefix, no sync marker — so a frame
// that sizes wrong would not merely be rejected, it would desynchronise the
// stream and cost every object behind it.
package encode

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lightwebinc/shard-common/objfmt"
)

// CoinbasePlaceholder is node 0 of a block's first subtree: 32 bytes of 0xFF
// standing in for the coinbase, which is carried in-band in the block frame
// instead. Detected by value, so it must be exact.
var CoinbasePlaceholder = [32]byte{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

// Subtree builds a BRC-143 subtree push frame.
//
//	root[32] | NodeCount u64 BE | NodeCount × hash[32]
//
// root and nodes are in internal (wire) byte order. The root must be the merkle
// root the node list reproduces — the receiving cluster rebuilds the tree and
// rejects a mismatch.
func Subtree(root [32]byte, nodes [][32]byte) ([]byte, error) {
	if len(nodes) == 0 {
		return nil, errors.New("encode: subtree with no nodes")
	}
	out := make([]byte, 0, objfmt.SubtreeHeaderSize+len(nodes)*32)
	out = append(out, root[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(nodes)))
	for i := range nodes {
		out = append(out, nodes[i][:]...)
	}
	if n, err := objfmt.SubtreeSize(out); err != nil || n != len(out) {
		return nil, fmt.Errorf("encode: built subtree fails its own codec (size=%d err=%v len=%d)", n, err, len(out))
	}
	return out, nil
}

// Block describes a BRC-144 block push frame.
type Block struct {
	Header       []byte     // 80 bytes, consensus layout
	TxCount      uint64     // transactions in the block, including the coinbase
	SizeInBytes  uint64     // serialized block size
	SubtreeRoots [][32]byte // ordered, internal byte order
	Coinbase     []byte     // full coinbase transaction, carried in-band
	Height       uint64
	CoinbaseBUMP []byte // BRC-74 merkle path for the coinbase; may be empty
}

// Encode builds the BRC-144 frame.
//
//	header[80] | TxCount u64 BE | SizeInBytes u64 BE | SubtreeCount u64 BE |
//	roots[32×M] | coinbase (self-delimiting) | Height u64 BE |
//	BUMPLen u64 BE | BUMP
func (b Block) Encode() ([]byte, error) {
	if len(b.Header) != 80 {
		return nil, fmt.Errorf("encode: header is %d bytes, want 80", len(b.Header))
	}
	if len(b.Coinbase) == 0 {
		return nil, errors.New("encode: block without an in-band coinbase")
	}
	// The coinbase is delimited by walking its transaction structure, so it must
	// parse exactly — a trailing byte would swallow the height field.
	if n, err := objfmt.TxSize(b.Coinbase); err != nil || n != len(b.Coinbase) {
		return nil, fmt.Errorf("encode: coinbase is not one whole transaction (size=%d err=%v len=%d)", n, err, len(b.Coinbase))
	}
	for i := range b.SubtreeRoots {
		if b.SubtreeRoots[i] == ([32]byte{}) {
			return nil, fmt.Errorf("encode: subtree root %d is all zero", i)
		}
	}

	out := make([]byte, 0, objfmt.BlockPrefixSize+len(b.SubtreeRoots)*32+len(b.Coinbase)+16+len(b.CoinbaseBUMP))
	out = append(out, b.Header...)
	out = binary.BigEndian.AppendUint64(out, b.TxCount)
	out = binary.BigEndian.AppendUint64(out, b.SizeInBytes)
	out = binary.BigEndian.AppendUint64(out, uint64(len(b.SubtreeRoots)))
	for i := range b.SubtreeRoots {
		out = append(out, b.SubtreeRoots[i][:]...)
	}
	out = append(out, b.Coinbase...)
	out = binary.BigEndian.AppendUint64(out, b.Height)
	out = binary.BigEndian.AppendUint64(out, uint64(len(b.CoinbaseBUMP)))
	out = append(out, b.CoinbaseBUMP...)

	if n, err := objfmt.BlockSize(out); err != nil || n != len(out) {
		return nil, fmt.Errorf("encode: built block fails its own codec (size=%d err=%v len=%d)", n, err, len(out))
	}
	return out, nil
}

// Package tnwire converts between Teranode's block serialization and the
// BRC-144 push frame.
//
// The two formats carry identical information in identical order and differ
// only in how four counts are written: BRC-144 uses fixed 8-byte big-endian
// fields, so a frame can be sized without parsing; Teranode uses Bitcoin
// CompactSize varints. The 80-byte header, the subtree roots, the in-band
// coinbase and the coinbase BUMP are byte-for-byte the same in both.
//
// Both directions live in one file on purpose. The bridge decodes this format
// when it serves a pushed block to the cluster and encodes it when it publishes
// a cluster-produced block, and a round trip through the pair must be lossless
// — which the tests assert.
//
//	BRC-144:  header[80] | txCount u64BE | size u64BE | subtreeCount u64BE |
//	          roots[32×M] | coinbase | height u64BE | bumpLen u64BE | bump
//	Teranode: header[80] | varint txCount | varint size | varint subtreeCount |
//	          roots[32×M] | coinbase | varint height | varint bumpLen | bump
package tnwire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lightwebinc/shard-common/objfmt"

	"github.com/lightwebinc/teranode-bridge/internal/encode"
)

// ToTeranode rewrites a BRC-144 push frame into Teranode's block serialization.
func ToTeranode(obj []byte) ([]byte, error) {
	total, err := objfmt.BlockSize(obj)
	if err != nil {
		return nil, fmt.Errorf("brc-144 size: %w", err)
	}
	if total != len(obj) {
		return nil, fmt.Errorf("brc-144: object is %d bytes, frame declares %d", len(obj), total)
	}
	if len(obj) < objfmt.BlockPrefixSize {
		return nil, errors.New("brc-144: short prefix")
	}

	txCount := binary.BigEndian.Uint64(obj[80:88])
	sizeInBytes := binary.BigEndian.Uint64(obj[88:96])
	subtreeCount := binary.BigEndian.Uint64(obj[96:104])

	off := objfmt.BlockPrefixSize
	rootsLen := int(subtreeCount) * 32
	if off+rootsLen > len(obj) {
		return nil, errors.New("brc-144: subtree roots overrun")
	}
	roots := obj[off : off+rootsLen]
	off += rootsLen

	// Teranode rejects an all-zero subtree hash outright, so catch it here
	// rather than letting it surface as an opaque parse failure in the cluster.
	for i := 0; i < int(subtreeCount); i++ {
		if isZero32(roots[i*32 : (i+1)*32]) {
			return nil, fmt.Errorf("brc-144: subtree root %d is all zero", i)
		}
	}

	cbLen, err := objfmt.TxSize(obj[off:])
	if err != nil {
		return nil, fmt.Errorf("brc-144: coinbase: %w", err)
	}
	coinbase := obj[off : off+cbLen]
	off += cbLen

	if off+16 > len(obj) {
		return nil, errors.New("brc-144: truncated before height/bump length")
	}
	height := binary.BigEndian.Uint64(obj[off : off+8])
	off += 8
	bumpLen := binary.BigEndian.Uint64(obj[off : off+8])
	off += 8
	if uint64(len(obj)-off) < bumpLen {
		return nil, errors.New("brc-144: truncated BUMP")
	}
	bump := obj[off : off+int(bumpLen)]

	out := make([]byte, 0, len(obj)+16)
	out = append(out, obj[:80]...)
	out = AppendVarInt(out, txCount)
	out = AppendVarInt(out, sizeInBytes)
	out = AppendVarInt(out, subtreeCount)
	out = append(out, roots...)
	out = append(out, coinbase...)
	out = AppendVarInt(out, height)
	out = AppendVarInt(out, uint64(len(bump)))
	out = append(out, bump...)
	return out, nil
}

// FromTeranode parses Teranode's block serialization into the parts of a BRC-144
// push frame.
//
// The trailing BUMP length is optional: Teranode's own parser tolerates a body
// that ends right after the height, so this does too and yields an empty BUMP.
func FromTeranode(b []byte) (encode.Block, error) {
	var out encode.Block
	if len(b) < 92 {
		return out, fmt.Errorf("teranode block: %d bytes, minimum is 92", len(b))
	}
	out.Header = b[:80]
	off := 80

	var n int
	if out.TxCount, n = readVarInt(b[off:]); n <= 0 {
		return out, errors.New("teranode block: bad transaction count")
	}
	off += n
	if out.SizeInBytes, n = readVarInt(b[off:]); n <= 0 {
		return out, errors.New("teranode block: bad size")
	}
	off += n
	subtreeCount, n := readVarInt(b[off:])
	if n <= 0 {
		return out, errors.New("teranode block: bad subtree count")
	}
	off += n

	if off+int(subtreeCount)*32 > len(b) {
		return out, errors.New("teranode block: subtree roots overrun")
	}
	out.SubtreeRoots = make([][32]byte, subtreeCount)
	for i := range out.SubtreeRoots {
		copy(out.SubtreeRoots[i][:], b[off:off+32])
		off += 32
	}

	cbLen, err := objfmt.TxSize(b[off:])
	if err != nil {
		return out, fmt.Errorf("teranode block: coinbase: %w", err)
	}
	out.Coinbase = b[off : off+cbLen]
	off += cbLen

	if out.Height, n = readVarInt(b[off:]); n <= 0 {
		return out, errors.New("teranode block: bad height")
	}
	off += n

	if off >= len(b) {
		return out, nil // no BUMP field at all — legal
	}
	bumpLen, n := readVarInt(b[off:])
	if n <= 0 {
		return out, errors.New("teranode block: bad BUMP length")
	}
	off += n
	if off+int(bumpLen) > len(b) {
		return out, errors.New("teranode block: truncated BUMP")
	}
	out.CoinbaseBUMP = b[off : off+int(bumpLen)]
	return out, nil
}

// CoinbaseOf returns the in-band coinbase transaction of a BRC-144 push frame.
//
// It exists for origin detection: `coinbase_arbitrary_text` is per-node
// Teranode CONFIGURATION stamped into every block that node mines, so a bridge
// that knows its own cluster's tag can tell locally-mined blocks from blocks
// the cluster merely learned about — statelessly, from block content alone,
// with no Teranode code change and no reliance on in-memory registry state
// that a restart wipes.
func CoinbaseOf(obj []byte) ([]byte, error) {
	if len(obj) < objfmt.BlockPrefixSize {
		return nil, errors.New("brc-144: short prefix")
	}
	subtreeCount := binary.BigEndian.Uint64(obj[96:104])
	off := objfmt.BlockPrefixSize + int(subtreeCount)*32
	if off > len(obj) {
		return nil, errors.New("brc-144: subtree roots overrun")
	}
	cbLen, err := objfmt.TxSize(obj[off:])
	if err != nil {
		return nil, fmt.Errorf("brc-144: coinbase: %w", err)
	}
	return obj[off : off+cbLen], nil
}

func isZero32(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// AppendVarInt writes a Bitcoin CompactSize integer.
func AppendVarInt(b []byte, v uint64) []byte {
	switch {
	case v < 0xfd:
		return append(b, byte(v))
	case v <= 0xffff:
		b = append(b, 0xfd)
		return binary.LittleEndian.AppendUint16(b, uint16(v))
	case v <= 0xffffffff:
		b = append(b, 0xfe)
		return binary.LittleEndian.AppendUint32(b, uint32(v))
	default:
		b = append(b, 0xff)
		return binary.LittleEndian.AppendUint64(b, v)
	}
}

// readVarInt returns the value and how many bytes it consumed, or n<=0 if the
// buffer is too short.
func readVarInt(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, 0
	}
	switch b[0] {
	case 0xfd:
		if len(b) < 3 {
			return 0, 0
		}
		return uint64(binary.LittleEndian.Uint16(b[1:3])), 3
	case 0xfe:
		if len(b) < 5 {
			return 0, 0
		}
		return uint64(binary.LittleEndian.Uint32(b[1:5])), 5
	case 0xff:
		if len(b) < 9 {
			return 0, 0
		}
		return binary.LittleEndian.Uint64(b[1:9]), 9
	default:
		return uint64(b[0]), 1
	}
}

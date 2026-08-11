package tnwire

import (
	"bytes"

	"encoding/binary"
	"github.com/lightwebinc/teranode-bridge/internal/encode"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

// minimalCoinbase is a syntactically valid one-input one-output transaction,
// enough for the self-delimiting walk that both formats rely on.
func minimalCoinbase() []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint32(b, 1) // version
	b = append(b, 0x01)                        // input count
	b = append(b, bytes.Repeat([]byte{0}, 32)...)
	b = binary.LittleEndian.AppendUint32(b, 0xffffffff) // prev index
	b = append(b, 0x02, 0x51, 0x51)                     // script len + OP_1 OP_1
	b = binary.LittleEndian.AppendUint32(b, 0xffffffff) // sequence
	b = append(b, 0x01)                                 // output count
	b = binary.LittleEndian.AppendUint64(b, 5000000000) // satoshis
	b = append(b, 0x01, 0x51)                           // script len + OP_1
	b = binary.LittleEndian.AppendUint32(b, 0)          // locktime
	return b
}

func buildBRC144(t *testing.T, subtreeRoots [][]byte, bump []byte, height uint64) []byte {
	t.Helper()
	cb := minimalCoinbase()

	obj := make([]byte, 0, 256)
	obj = append(obj, bytes.Repeat([]byte{0xAB}, 80)...) // header
	obj = binary.BigEndian.AppendUint64(obj, 42)         // tx count
	obj = binary.BigEndian.AppendUint64(obj, 4242)       // size in bytes
	obj = binary.BigEndian.AppendUint64(obj, uint64(len(subtreeRoots)))
	for _, r := range subtreeRoots {
		obj = append(obj, r...)
	}
	obj = append(obj, cb...)
	obj = binary.BigEndian.AppendUint64(obj, height)
	obj = binary.BigEndian.AppendUint64(obj, uint64(len(bump)))
	obj = append(obj, bump...)

	// The frame must be self-consistent before we transcode it, or the test is
	// asserting against a fixture the codec would itself reject.
	if n, err := objfmt.BlockSize(obj); err != nil || n != len(obj) {
		t.Fatalf("fixture is not a valid BRC-144 frame: size=%d err=%v len=%d", n, err, len(obj))
	}
	return obj
}

func TestBlockToTeranodeRoundTrip(t *testing.T) {
	roots := [][]byte{bytes.Repeat([]byte{0x11}, 32), bytes.Repeat([]byte{0x22}, 32)}
	bump := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	obj := buildBRC144(t, roots, bump, 707)

	out, err := ToTeranode(obj)
	if err != nil {
		t.Fatalf("transcode: %v", err)
	}

	// Header is copied verbatim — the block hash must not change.
	if !bytes.Equal(out[:80], obj[:80]) {
		t.Fatal("header not preserved")
	}

	off := 80
	txCount, n := readVarInt(out[off:])
	off += n
	sizeInBytes, n := readVarInt(out[off:])
	off += n
	subtreeCount, n := readVarInt(out[off:])
	off += n

	if txCount != 42 || sizeInBytes != 4242 || subtreeCount != 2 {
		t.Fatalf("counts wrong: tx=%d size=%d subtrees=%d", txCount, sizeInBytes, subtreeCount)
	}
	for i, want := range roots {
		got := out[off : off+32]
		if !bytes.Equal(got, want) {
			t.Fatalf("subtree root %d not preserved", i)
		}
		off += 32
	}
	cb := minimalCoinbase()
	if !bytes.Equal(out[off:off+len(cb)], cb) {
		t.Fatal("coinbase not preserved")
	}
	off += len(cb)

	height, n := readVarInt(out[off:])
	off += n
	if height != 707 {
		t.Fatalf("height %d", height)
	}
	bumpLen, n := readVarInt(out[off:])
	off += n
	if bumpLen != uint64(len(bump)) || !bytes.Equal(out[off:off+len(bump)], bump) {
		t.Fatal("BUMP not preserved")
	}
	if off+len(bump) != len(out) {
		t.Fatalf("%d trailing bytes", len(out)-(off+len(bump)))
	}
}

func TestBlockToTeranodeEmptyBUMP(t *testing.T) {
	obj := buildBRC144(t, [][]byte{bytes.Repeat([]byte{0x33}, 32)}, nil, 1)
	out, err := ToTeranode(obj)
	if err != nil {
		t.Fatalf("transcode: %v", err)
	}
	if out[len(out)-1] != 0x00 {
		t.Fatal("expected a zero-length BUMP varint as the final byte")
	}
}

// A zero subtree hash is rejected outright by Teranode's parser, so the bridge
// must catch it here rather than announce an object that can never be accepted.
func TestBlockToTeranodeRejectsZeroSubtreeRoot(t *testing.T) {
	obj := buildBRC144(t, [][]byte{make([]byte, 32)}, nil, 1)
	if _, err := ToTeranode(obj); err == nil {
		t.Fatal("expected rejection of an all-zero subtree root")
	}
}

func TestBlockToTeranodeRejectsTruncated(t *testing.T) {
	obj := buildBRC144(t, [][]byte{bytes.Repeat([]byte{0x44}, 32)}, []byte{0x01}, 9)
	if _, err := ToTeranode(obj[:len(obj)-1]); err == nil {
		t.Fatal("expected rejection of a truncated frame")
	}
}

func TestVarIntBoundaries(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want int
	}{{0, 1}, {0xfc, 1}, {0xfd, 3}, {0xffff, 3}, {0x10000, 5}, {0xffffffff, 5}, {0x100000000, 9}} {
		got := AppendVarInt(nil, tc.v)
		if len(got) != tc.want {
			t.Fatalf("varint(%d): %d bytes, want %d", tc.v, len(got), tc.want)
		}
		back, n := readVarInt(got)
		if back != tc.v || n != tc.want {
			t.Fatalf("varint(%d) round-trip: got %d in %d bytes", tc.v, back, n)
		}
	}
}

// The bridge decodes this format when serving a pushed block to the cluster and
// encodes it when publishing a cluster-produced one. A round trip must be
// lossless in both directions, or a block that crosses the bridge twice would
// not be the block that was mined.
func TestBlockRoundTripIsLossless(t *testing.T) {
	roots := [][]byte{bytes.Repeat([]byte{0x11}, 32), bytes.Repeat([]byte{0x22}, 32)}
	original := buildBRC144(t, roots, []byte{0xDE, 0xAD, 0xBE, 0xEF}, 707)

	tn, err := ToTeranode(original)
	if err != nil {
		t.Fatalf("to teranode: %v", err)
	}
	parsed, err := FromTeranode(tn)
	if err != nil {
		t.Fatalf("from teranode: %v", err)
	}
	rebuilt, err := parsed.Encode()
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(rebuilt, original) {
		t.Fatalf("round trip changed the frame:\n orig %d bytes\n back %d bytes", len(original), len(rebuilt))
	}
}

func TestBlockRoundTripNoBUMP(t *testing.T) {
	original := buildBRC144(t, [][]byte{bytes.Repeat([]byte{0x33}, 32)}, nil, 1)
	tn, err := ToTeranode(original)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := FromTeranode(tn)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := parsed.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuilt, original) {
		t.Fatal("round trip changed a BUMP-less frame")
	}
}

// Teranode's own parser tolerates a body that ends right after the height, so
// ours must too — and must yield an empty BUMP rather than an error.
func TestFromTeranodeToleratesMissingBUMPField(t *testing.T) {
	original := buildBRC144(t, [][]byte{bytes.Repeat([]byte{0x44}, 32)}, nil, 9)
	tn, err := ToTeranode(original)
	if err != nil {
		t.Fatal(err)
	}
	truncated := tn[:len(tn)-1] // drop the trailing zero-length BUMP varint
	parsed, err := FromTeranode(truncated)
	if err != nil {
		t.Fatalf("a body ending after the height is legal: %v", err)
	}
	if len(parsed.CoinbaseBUMP) != 0 || parsed.Height != 9 {
		t.Fatalf("height=%d bump=%d", parsed.Height, len(parsed.CoinbaseBUMP))
	}
}

// TestCoinbaseOf pins the origin-detection extractor: the in-band coinbase of
// a BRC-144 frame must come back exactly, so a tag check against it is a check
// against what the miner actually stamped.
func TestCoinbaseOf(t *testing.T) {
	tag := []byte("/teranode2/")
	script := append([]byte{0x03, 0x01, 0x02, 0x03}, tag...)
	cb := make([]byte, 0, 64)
	cb = append(cb, 1, 0, 0, 0) // version
	cb = append(cb, 1)          // input count
	cb = append(cb, make([]byte, 32)...)
	cb = append(cb, 0xFF, 0xFF, 0xFF, 0xFF) // coinbase vout
	cb = append(cb, byte(len(script)))
	cb = append(cb, script...)
	cb = append(cb, 0xFF, 0xFF, 0xFF, 0xFF) // sequence
	cb = append(cb, 1)                      // output count
	cb = append(cb, 1, 0, 0, 0, 0, 0, 0, 0)
	cb = append(cb, 1, 0x51)
	cb = append(cb, 0, 0, 0, 0) // locktime

	blk := encode.Block{
		Header:       make([]byte, 80),
		TxCount:      1,
		SizeInBytes:  uint64(len(cb)) + 80,
		SubtreeRoots: [][32]byte{{0xAB}},
		Coinbase:     cb,
		Height:       7,
	}
	frame, err := blk.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := CoinbaseOf(frame)
	if err != nil {
		t.Fatalf("CoinbaseOf: %v", err)
	}
	if !bytes.Equal(got, cb) {
		t.Fatal("extracted coinbase differs from the one encoded")
	}
	if !bytes.Contains(got, tag) {
		t.Fatal("tag not found in extracted coinbase")
	}
}

// TestSubtreeRootsOf pins the other half of origin detection: a block names the
// subtrees it contains, which is the only signal a subtree has about where it
// came from. Getting this wrong means a cluster republishes another cluster's
// subtrees whenever gossip beats the object plane.
func TestSubtreeRootsOf(t *testing.T) {
	want := [][]byte{bytes.Repeat([]byte{0x11}, 32), bytes.Repeat([]byte{0x22}, 32)}
	frame := buildBRC144(t, want, []byte{0xAA}, 700)

	got, err := SubtreeRootsOf(frame)
	if err != nil {
		t.Fatalf("SubtreeRootsOf: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d roots, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i][:], want[i]) {
			t.Fatalf("root %d: got %x want %x", i, got[i][:4], want[i][:4])
		}
	}
	// The roots must be the SAME values a round trip preserves, or the marks we
	// write would not match the notifications we later receive.
	blk, err := FromTeranode(mustToTeranode(t, frame))
	if err != nil {
		t.Fatal(err)
	}
	for i := range blk.SubtreeRoots {
		if blk.SubtreeRoots[i] != got[i] {
			t.Fatalf("root %d disagrees with the parsed block", i)
		}
	}
}

func mustToTeranode(t *testing.T, frame []byte) []byte {
	t.Helper()
	out, err := ToTeranode(frame)
	if err != nil {
		t.Fatalf("ToTeranode: %v", err)
	}
	return out
}

func TestSubtreeRootsOfRejectsGarbage(t *testing.T) {
	if _, err := SubtreeRootsOf([]byte{0x01, 0x02}); err == nil {
		t.Fatal("expected rejection of a short frame")
	}
	// A count field that overruns the buffer must not panic or allocate wildly.
	bad := make([]byte, 104)
	for i := 96; i < 104; i++ {
		bad[i] = 0xFF
	}
	if _, err := SubtreeRootsOf(bad); err == nil {
		t.Fatal("expected rejection of an overrunning subtree count")
	}
}

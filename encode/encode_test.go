package encode

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

func minimalCoinbase() []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = append(b, 0x01)
	b = append(b, bytes.Repeat([]byte{0}, 32)...)
	b = binary.LittleEndian.AppendUint32(b, 0xffffffff)
	b = append(b, 0x02, 0x51, 0x51)
	b = binary.LittleEndian.AppendUint32(b, 0xffffffff)
	b = append(b, 0x01)
	b = binary.LittleEndian.AppendUint64(b, 5000000000)
	b = append(b, 0x01, 0x51)
	b = binary.LittleEndian.AppendUint32(b, 0)
	return b
}

func TestSubtreeLayout(t *testing.T) {
	root := [32]byte{0xAA}
	nodes := [][32]byte{CoinbasePlaceholder, {0x01}, {0x02}}

	out, err := Subtree(root, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != objfmt.SubtreeHeaderSize+3*32 {
		t.Fatalf("length %d", len(out))
	}
	if !bytes.Equal(out[:32], root[:]) {
		t.Fatal("root not first")
	}
	if got := binary.BigEndian.Uint64(out[32:40]); got != 3 {
		t.Fatalf("node count %d", got)
	}
	// The coinbase placeholder must survive byte-exact: it is detected by value.
	if !bytes.Equal(out[40:72], CoinbasePlaceholder[:]) {
		t.Fatal("coinbase placeholder corrupted")
	}
}

func TestSubtreeRejectsEmpty(t *testing.T) {
	if _, err := Subtree([32]byte{1}, nil); err == nil {
		t.Fatal("expected rejection of a subtree with no nodes")
	}
}

func TestBlockRoundTripsThroughCodec(t *testing.T) {
	b := Block{
		Header:       bytes.Repeat([]byte{0x7A}, 80),
		TxCount:      3,
		SizeInBytes:  999,
		SubtreeRoots: [][32]byte{{0x11}, {0x22}},
		Coinbase:     minimalCoinbase(),
		Height:       800001,
		CoinbaseBUMP: []byte{0xBE, 0xEF},
	}
	out, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	// Encode already self-verifies with BlockSize; assert the field placement too.
	if !bytes.Equal(out[:80], b.Header) {
		t.Fatal("header")
	}
	if binary.BigEndian.Uint64(out[80:88]) != 3 ||
		binary.BigEndian.Uint64(out[88:96]) != 999 ||
		binary.BigEndian.Uint64(out[96:104]) != 2 {
		t.Fatal("counts")
	}
	off := 104 + 64
	if !bytes.Equal(out[off:off+len(b.Coinbase)], b.Coinbase) {
		t.Fatal("coinbase")
	}
	off += len(b.Coinbase)
	if binary.BigEndian.Uint64(out[off:off+8]) != 800001 {
		t.Fatal("height")
	}
	if binary.BigEndian.Uint64(out[off+8:off+16]) != 2 {
		t.Fatal("bump length")
	}
}

func TestBlockEmptyBUMP(t *testing.T) {
	b := Block{
		Header: bytes.Repeat([]byte{0x01}, 80), TxCount: 1, SizeInBytes: 200,
		SubtreeRoots: [][32]byte{{0x33}}, Coinbase: minimalCoinbase(), Height: 1,
	}
	if _, err := b.Encode(); err != nil {
		t.Fatalf("a zero-length BUMP is legal: %v", err)
	}
}

func TestBlockRejectsBadInputs(t *testing.T) {
	good := Block{
		Header: bytes.Repeat([]byte{0x01}, 80), TxCount: 1, SizeInBytes: 200,
		SubtreeRoots: [][32]byte{{0x33}}, Coinbase: minimalCoinbase(), Height: 1,
	}
	short := good
	short.Header = good.Header[:79]
	if _, err := short.Encode(); err == nil {
		t.Fatal("expected rejection of a 79-byte header")
	}
	zero := good
	zero.SubtreeRoots = [][32]byte{{}}
	if _, err := zero.Encode(); err == nil {
		t.Fatal("expected rejection of an all-zero subtree root")
	}
	trailing := good
	trailing.Coinbase = append(append([]byte{}, good.Coinbase...), 0x00)
	if _, err := trailing.Encode(); err == nil {
		t.Fatal("expected rejection of a coinbase with a trailing byte")
	}
	noCB := good
	noCB.Coinbase = nil
	if _, err := noCB.Encode(); err == nil {
		t.Fatal("expected rejection of a block with no coinbase")
	}
}

// The frames we build must be indistinguishable from the ones we receive: strip
// the multicast wrapper off our own encoding and get the original bytes back.
func TestFramesSurviveMulticastWrap(t *testing.T) {
	st, err := Subtree([32]byte{0xAB}, [][32]byte{{0x01}, {0x02}})
	if err != nil {
		t.Fatal(err)
	}
	mc, err := objfmt.MulticastBytes(objfmt.ClassSubtree, st)
	if err != nil {
		t.Fatal(err)
	}
	back, err := objfmt.StripBytes(objfmt.ClassSubtree, mc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, st) {
		t.Fatal("subtree frame did not survive wrap/strip")
	}
}

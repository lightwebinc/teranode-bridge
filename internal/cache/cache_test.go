package cache

import (
	"bytes"
	"io"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

// TestPutCopies pins the cache's ownership contract: bytes handed to Put must
// remain readable unchanged even if the caller's slice is later overwritten.
//
// Every lane handler passes the slice returned by objfmt.Reader.Next straight
// into Put, and that slice is documented to alias the reader's buffer, valid
// only until the following Next call. If Put retains it without copying, later
// objects on the same connection compact over the cached bytes — corrupting
// entries the retrieval plane will serve to the cluster.
func TestPutCopies(t *testing.T) {
	c := New(Options{})
	body := []byte("original-bytes-the-cache-must-keep")
	var key Key
	copy(key[:], "k1")

	c.Put(key, "tx", body)
	for i := range body {
		body[i] = 0xEE // caller's buffer is reused, as a lane reader's is
	}

	got, _, ok := c.Get(key)
	if !ok {
		t.Fatal("entry missing")
	}
	if !bytes.Equal(got, []byte("original-bytes-the-cache-must-keep")) {
		t.Fatalf("cache returned mutated bytes: %q", got)
	}
}

// chunkReader yields the stream in small pieces, forcing the objfmt.Reader to
// compact its window between objects the way a real socket read pattern does.
type chunkReader struct {
	data []byte
	n    int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.n
	if n > len(r.data) {
		n = len(r.data)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// TestReaderAliasSurvivesInCache reproduces the end-to-end hazard: cache an
// object exactly as the lane handlers do (no copy at the call site), keep
// reading the stream, and require the cached bytes to still be the original
// object. Guards the Put copy against regression at the integration seam.
func TestReaderAliasSurvivesInCache(t *testing.T) {
	// Minimal valid BRC-30/BRC-12-shaped transactions are overkill here; use
	// the subtree codec instead, whose frames are trivial to build: root[32] |
	// count u64 BE | count×hash[32].
	frame := func(fill byte, nodes int) []byte {
		b := make([]byte, 40+nodes*32)
		for i := 0; i < 32; i++ {
			b[i] = fill
		}
		b[39] = byte(nodes)
		for i := 0; i < nodes*32; i++ {
			b[40+i] = fill
		}
		return b
	}
	stream := append(frame(0xAA, 3), frame(0xBB, 3)...)
	stream = append(stream, frame(0xCC, 3)...)

	rd := objfmt.NewReader(&chunkReader{data: stream, n: 7}, objfmt.ClassSubtree)
	c := New(Options{})

	first, err := rd.Next()
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	want := bytes.Clone(first)
	var key Key
	copy(key[:], first[:32])
	c.Put(key, "subtree", first) // as handleSubtree does: no copy at call site

	for {
		if _, err := rd.Next(); err != nil {
			break
		}
	}

	got, _, ok := c.Get(key)
	if !ok {
		t.Fatal("entry missing")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cached object corrupted by later reads on the same connection:\n want %x\n got  %x", want[:16], got[:16])
	}
}

package lanes

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

// subtreeFrame builds a BRC-143 frame: root | u64 count | count × 32B nodes.
func subtreeFrame(fill byte, nodes int) []byte {
	out := make([]byte, 40+nodes*32)
	for i := 0; i < 32; i++ {
		out[i] = fill
	}
	binary.BigEndian.PutUint64(out[32:40], uint64(nodes))
	for i := 40; i < len(out); i++ {
		out[i] = fill
	}
	return out
}

type captured struct {
	mu   sync.Mutex
	objs [][]byte
	errs int
}

func (c *captured) handle(fail bool) Handler {
	return func(_ context.Context, obj []byte) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.objs = append(c.objs, bytes.Clone(obj))
		if fail {
			c.errs++
			return io.ErrUnexpectedEOF
		}
		return nil
	}
}

func startLane(t *testing.T, h Handler, maxObject int) (*Lane, string, context.CancelFunc) {
	t.Helper()
	l := &Lane{
		Name: "subtree", Class: objfmt.ClassSubtree, Addr: "127.0.0.1:0",
		Handle: h, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxObject: maxObject,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Serve(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for !l.Bound() {
		if time.Now().After(deadline) {
			t.Fatal("lane never bound")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Cleanup(func() { cancel(); <-done })
	return l, l.ListenerAddr().String(), cancel
}

func waitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWholeObjectsDelivered pins the basic contract: back-to-back bare frames
// are split on structure boundaries and each handler call sees exactly one
// whole object.
func TestWholeObjectsDelivered(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, c.handle(false), 0)

	a, b := subtreeFrame(0xAA, 2), subtreeFrame(0xBB, 5)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(bytes.Clone(a), b...)); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	waitCond(t, "2 objects", func() bool { return l.Stats().Objects == 2 })
	c.mu.Lock()
	defer c.mu.Unlock()
	if !bytes.Equal(c.objs[0], a) || !bytes.Equal(c.objs[1], b) {
		t.Fatal("handler saw different bytes than were sent")
	}
	if s := l.Stats(); s.Dropped != 0 || s.Errors != 0 {
		t.Fatalf("unexpected faults: %+v", s)
	}
}

// TestFramingFaultDropsConnectionOnly pins the no-resync rule: a malformed
// object costs the CONNECTION (counted in Dropped), but the listener keeps
// accepting and prior objects stand.
func TestFramingFaultDropsConnectionOnly(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, c.handle(false), 0)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	good := subtreeFrame(0x01, 1)
	// A node count that cannot fit in memory is unambiguously malformed
	// (a zero-node frame, by contrast, is codec-valid).
	bad := make([]byte, 40)
	for i := 0; i < 40; i++ {
		bad[i] = 0xFF
	}
	if _, err := conn.Write(append(bytes.Clone(good), bad...)); err != nil {
		t.Fatal(err)
	}

	waitCond(t, "connection dropped", func() bool { return l.Stats().Dropped == 1 })
	if got := l.Stats().Objects; got != 1 {
		t.Fatalf("objects = %d, want the good one delivered before the fault", got)
	}
	// A dead read confirms the lane closed the connection on its side.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after framing fault")
	}
	_ = conn.Close()

	// The listener must still accept new connections.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn2.Write(subtreeFrame(0x03, 1)); err != nil {
		t.Fatal(err)
	}
	_ = conn2.Close()
	waitCond(t, "post-fault delivery", func() bool { return l.Stats().Objects == 2 })
}

// TestHandlerErrorKeepsConnection pins the error split: a cluster-side failure
// on one object is counted and logged but must not cost the stream.
func TestHandlerErrorKeepsConnection(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, c.handle(true), 0)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	two := append(subtreeFrame(0x0A, 1), subtreeFrame(0x0B, 1)...)
	if _, err := conn.Write(two); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	waitCond(t, "both objects despite handler errors", func() bool {
		s := l.Stats()
		return s.Objects == 2 && s.Errors == 2
	})
	if l.Stats().Dropped != 0 {
		t.Fatal("handler errors must not drop the connection")
	}
}

// TestMaxObjectBound pins the size ceiling: an object larger than MaxObject is
// a framing fault (no resync point), not an allocation.
func TestMaxObjectBound(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, c.handle(false), 100) // 100B ceiling

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(subtreeFrame(0x0C, 8)); err != nil { // 296B > 100B
		t.Fatal(err)
	}
	waitCond(t, "oversize dropped", func() bool { return l.Stats().Dropped == 1 })
	if l.Stats().Objects != 0 {
		t.Fatal("oversize object must not be delivered")
	}
	_ = conn.Close()
}

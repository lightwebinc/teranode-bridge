package lanes

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

// TestRejectIsNotAnError pins the admission refusal: an object the handler
// refuses on format policy counts as Rejected, not Errors, and the stream
// carries on — framing was never in doubt.
func TestRejectIsNotAnError(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, func(_ context.Context, obj []byte) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.objs = append(c.objs, bytes.Clone(obj))
		if obj[0] == 0x0A {
			return fmt.Errorf("%w: policy", ErrReject)
		}
		return nil
	}, 0)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	two := append(subtreeFrame(0x0A, 1), subtreeFrame(0x0B, 1)...)
	if _, err := conn.Write(two); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	waitCond(t, "one refused, one accepted", func() bool {
		s := l.Stats()
		return s.Objects == 2 && s.Rejected == 1
	})
	if s := l.Stats(); s.Errors != 0 || s.Dropped != 0 {
		t.Fatalf("refusal must not count as error or drop the connection: %+v", s)
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

// TestOwnedObjectsSurviveLaterReads pins the Owned contract the tx lane relies
// on: a handler may keep the slice it was given, and nothing the reader does
// afterwards (more reads, buffer compaction, a new slab) changes its bytes.
func TestOwnedObjectsSurviveLaterReads(t *testing.T) {
	var mu sync.Mutex
	var kept [][]byte
	l := &Lane{
		Name: "subtree", Class: objfmt.ClassSubtree, Addr: "127.0.0.1:0", Owned: true,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handle: func(_ context.Context, obj []byte) error {
			mu.Lock()
			kept = append(kept, obj) // retained WITHOUT a copy
			mu.Unlock()
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitCond(t, "bound", l.Bound)

	conn, err := net.Dial("tcp", l.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Enough objects to fill several slabs, including one too large for a slab.
	var want [][]byte
	for i := 0; i < 600; i++ {
		nodes := 100
		if i == 300 {
			nodes = slabSize / 32 // larger than slabSize/8: its own allocation
		}
		f := subtreeFrame(byte(i), nodes)
		want = append(want, f)
		if _, err := conn.Write(f); err != nil {
			t.Fatal(err)
		}
	}
	waitCond(t, "all objects", func() bool { mu.Lock(); defer mu.Unlock(); return len(kept) == len(want) })
	mu.Lock()
	defer mu.Unlock()
	for i := range want {
		if !bytes.Equal(kept[i], want[i]) {
			t.Fatalf("object %d changed after later reads", i)
		}
		if cap(kept[i]) != len(kept[i]) {
			t.Fatalf("object %d has spare capacity %d: an append would overwrite its neighbour", i, cap(kept[i])-len(kept[i]))
		}
	}
	if st := l.Stats(); st.Objects != uint64(len(want)) {
		t.Fatalf("live connection objects = %d, want %d", st.Objects, len(want))
	}
}

// TestStatsSumLiveAndClosedConnections pins the per-connection counters: a
// snapshot counts objects on open connections and on connections already gone.
func TestStatsSumLiveAndClosedConnections(t *testing.T) {
	var c captured
	l, addr, _ := startLane(t, c.handle(false), 0)
	f := subtreeFrame(7, 4)

	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, _ = first.Write(f)
	}
	waitCond(t, "first conn objects", func() bool { return l.Stats().Objects == 3 })
	_ = first.Close()
	waitCond(t, "first conn retired", func() bool { return l.Stats().Active == 0 })

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	for i := 0; i < 2; i++ {
		_, _ = second.Write(f)
	}
	waitCond(t, "both conns counted", func() bool { return l.Stats().Objects == 5 })
	if st := l.Stats(); st.Bytes != uint64(5*len(f)) {
		t.Fatalf("bytes = %d, want %d", st.Bytes, 5*len(f))
	}
}

// TestSessionFlushesBeforeBlocking pins the latency half of the session
// contract: an object that arrives alone is flushed before the lane waits for
// the next one, so batching the hand-off never holds an object back.
func TestSessionFlushesBeforeBlocking(t *testing.T) {
	var mu sync.Mutex
	var pending, flushed int
	l := &Lane{
		Name: "subtree", Class: objfmt.ClassSubtree, Addr: "127.0.0.1:0",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		NewSession: func() Session {
			return Session{
				Handle: func(context.Context, []byte) error {
					mu.Lock()
					pending++
					mu.Unlock()
					return nil
				},
				Flush: func(context.Context) error {
					mu.Lock()
					flushed += pending
					pending = 0
					mu.Unlock()
					return nil
				},
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitCond(t, "bound", l.Bound)

	conn, err := net.Dial("tcp", l.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for i := 1; i <= 3; i++ {
		if _, err := conn.Write(subtreeFrame(byte(i), 2)); err != nil {
			t.Fatal(err)
		}
		// Nothing more is sent until this object has been flushed.
		want := i
		waitCond(t, fmt.Sprintf("object %d flushed while the lane waits", i), func() bool {
			mu.Lock()
			defer mu.Unlock()
			return flushed == want
		})
	}
}

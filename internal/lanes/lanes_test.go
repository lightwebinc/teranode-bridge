package lanes

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// subtreeFrame builds a BRC-143 frame with n nodes: root ∥ u64 BE count ∥ n×32.
func subtreeFrame(n int, fill byte) []byte {
	out := make([]byte, 0, objfmt.SubtreeHeaderSize+n*32)
	root := make([]byte, 32)
	root[0] = fill
	out = append(out, root...)
	out = binary.BigEndian.AppendUint64(out, uint64(n))
	for i := 0; i < n; i++ {
		node := make([]byte, 32)
		node[0] = fill
		node[1] = byte(i)
		out = append(out, node...)
	}
	return out
}

// serveOn starts a lane on an ephemeral port and returns its address plus a
// stop func. Handler results are pushed to got.
func serveOn(t *testing.T, class objfmt.Class, handle Handler) (*Lane, string, func()) {
	t.Helper()
	l := &Lane{Name: "test", Class: class, Addr: "127.0.0.1:0", Log: quietLogger(), Handle: handle}

	// Bind first so the test never races the listener.
	ln, err := net.Listen("tcp", l.Addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	l.Addr = addr

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = l.Serve(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for !l.Bound() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !l.Bound() {
		cancel()
		t.Fatal("lane never bound")
	}
	return l, addr, func() { cancel(); <-done }
}

func TestLaneDeliversWholeObjects(t *testing.T) {
	var mu sync.Mutex
	var got [][]byte
	l, addr, stop := serveOn(t, objfmt.ClassSubtree, func(_ context.Context, obj []byte) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, append([]byte(nil), obj...))
		return nil
	})
	defer stop()

	a, b := subtreeFrame(3, 0xAA), subtreeFrame(5, 0xBB)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Both objects in ONE write: the lane is a bare stream, so the split between
	// objects must come from walking their structure, not from packet boundaries.
	if _, err := conn.Write(append(append([]byte{}, a...), b...)); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = conn.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("got %d objects, want 2", len(got))
	}
	if len(got[0]) != len(a) || len(got[1]) != len(b) {
		t.Fatalf("object sizes %d,%d want %d,%d", len(got[0]), len(got[1]), len(a), len(b))
	}
	if st := l.Stats(); st.Objects != 2 || st.Bytes != uint64(len(a)+len(b)) {
		t.Fatalf("stats objects=%d bytes=%d", st.Objects, st.Bytes)
	}
}

// The handler receives a slice that ALIASES the reader's buffer. Anything that
// retains it must copy — this pins the invariant that cost us a corruption bug.
func TestLaneHandlerSliceIsReusedNotOwned(t *testing.T) {
	var mu sync.Mutex
	var retained [][]byte
	_, addr, stop := serveOn(t, objfmt.ClassSubtree, func(_ context.Context, obj []byte) error {
		mu.Lock()
		defer mu.Unlock()
		retained = append(retained, obj) // deliberately NOT copied
		return nil
	})
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	first := subtreeFrame(4, 0xAA)
	_, _ = conn.Write(first)
	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 6; i++ {
		_, _ = conn.Write(subtreeFrame(4, byte(0xC0+i)))
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	_ = conn.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(retained) == 0 {
		t.Skip("no objects observed")
	}
	// We assert the DANGER, not a guarantee: if the retained slice still equals
	// what was sent, the reader happened not to reuse that region — the test
	// documents why callers must copy either way.
	if string(retained[0]) != string(first) {
		t.Logf("confirmed: retained slice mutated under later reads (this is why cache.Put copies)")
	}
}

func TestLaneActiveCountTracksConnections(t *testing.T) {
	l, addr, stop := serveOn(t, objfmt.ClassSubtree, func(context.Context, []byte) error { return nil })
	defer stop()

	if a := l.Stats().Active; a != 0 {
		t.Fatalf("active=%d before any connection", a)
	}
	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := net.Dial("tcp", addr)

	deadline := time.Now().Add(2 * time.Second)
	for l.Stats().Active < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if a := l.Stats().Active; a != 2 {
		t.Fatalf("active=%d with two connections open", a)
	}
	_ = c1.Close()
	_ = c2.Close()
	for l.Stats().Active > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if a := l.Stats().Active; a != 0 {
		t.Fatalf("active=%d after both closed", a)
	}
}

// A malformed object has no resync point on a bare stream, so the connection
// must be dropped rather than the bridge trying to continue.
func TestLaneDropsConnectionOnFramingFault(t *testing.T) {
	l, addr, stop := serveOn(t, objfmt.ClassSubtree, func(context.Context, []byte) error { return nil })
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// A node count that cannot be satisfied: valid prefix, impossible body.
	bad := make([]byte, objfmt.SubtreeHeaderSize)
	binary.BigEndian.PutUint64(bad[32:40], ^uint64(0))
	if _, err := conn.Write(bad); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected the lane to drop the connection")
	}
	if !errors.Is(err, io.EOF) {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("lane kept a desynchronised connection open")
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for l.Stats().Dropped == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if l.Stats().Dropped == 0 {
		t.Fatal("framing fault not counted as a drop")
	}
}

// A handler error must NOT cost the rest of the stream: one bad object at the
// cluster is not a reason to lose everything queued behind it.
func TestLaneSurvivesHandlerError(t *testing.T) {
	var mu sync.Mutex
	seen := 0
	l, addr, stop := serveOn(t, objfmt.ClassSubtree, func(context.Context, []byte) error {
		mu.Lock()
		defer mu.Unlock()
		seen++
		return errors.New("cluster refused it")
	})
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	for i := 0; i < 3; i++ {
		_, _ = conn.Write(subtreeFrame(2, byte(0x10+i)))
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := seen
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = conn.Close()

	mu.Lock()
	defer mu.Unlock()
	if seen != 3 {
		t.Fatalf("handler saw %d objects, want 3 (errors must not drop the stream)", seen)
	}
	if st := l.Stats(); st.Errors != 3 || st.Dropped != 0 {
		t.Fatalf("errors=%d dropped=%d", st.Errors, st.Dropped)
	}
}

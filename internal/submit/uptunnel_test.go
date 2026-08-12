package submit

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

// sink accepts one connection at a time on a fixed address and records bytes.
type sink struct {
	t    *testing.T
	mu   sync.Mutex
	ln   net.Listener
	got  bytes.Buffer
	addr string
}

func newSink(t *testing.T) *sink {
	t.Helper()
	s := &sink{t: t}
	s.listen("127.0.0.1:0")
	t.Cleanup(func() { s.close() })
	return s
}

func (s *sink) listen(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.t.Fatalf("listen: %v", err)
	}
	s.mu.Lock()
	s.ln, s.addr = ln, ln.Addr().String()
	s.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						s.mu.Lock()
						s.got.Write(buf[:n])
						s.mu.Unlock()
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
}

func (s *sink) close() {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

func (s *sink) bytesGot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.got.Bytes())
}

func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSendWritesWholeObjectsInOrder pins the bare-stream contract: objects are
// concatenated with no framing of our own, in submission order.
func TestSendWritesWholeObjectsInOrder(t *testing.T) {
	s := newSink(t)
	u := &UpTunnel{Addr: s.addr, Class: "subtree", Log: testLog()}
	defer func() { _ = u.Close() }()

	a := bytes.Repeat([]byte{0xAA}, 40)
	b := bytes.Repeat([]byte{0xBB}, 72)
	for _, obj := range [][]byte{a, b} {
		if err := u.Send(context.Background(), obj); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	waitFor(t, "both objects", func() bool { return len(s.bytesGot()) == len(a)+len(b) })
	if !bytes.Equal(s.bytesGot(), append(bytes.Clone(a), b...)) {
		t.Fatal("stream is not the objects concatenated in order")
	}
	if st := u.Stats(); st.Sent != 2 || st.Redials != 1 || st.Failures != 0 {
		t.Fatalf("stats = %+v, want 2 sent on 1 dial", st)
	}
}

// TestEmptyObjectRejected pins that a zero-length write never reaches the
// wire: on a bare stream it would be indistinguishable from nothing, and the
// caller has a bug worth surfacing.
func TestEmptyObjectRejected(t *testing.T) {
	s := newSink(t)
	u := &UpTunnel{Addr: s.addr, Class: "block", Log: testLog()}
	defer func() { _ = u.Close() }()

	if err := u.Send(context.Background(), nil); err == nil {
		t.Fatal("want an error for an empty object")
	}
	if st := u.Stats(); st.Sent != 0 {
		t.Fatalf("sent = %d, want 0", st.Sent)
	}
}

// TestDialFailureCountsAndRecovers pins the failure accounting and the
// redial: with nothing listening the send fails and is counted, and once the
// peer exists a later send connects and succeeds.
func TestDialFailureCountsAndRecovers(t *testing.T) {
	// Grab a free port, then close it so the first dial is refused.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	u := &UpTunnel{Addr: addr, Class: "subtree", Log: testLog()}
	defer func() { _ = u.Close() }()

	if err := u.Send(context.Background(), []byte{1, 2, 3}); err == nil {
		t.Fatal("want a dial error with nothing listening")
	}
	if st := u.Stats(); st.Failures != 1 || st.Sent != 0 {
		t.Fatalf("stats = %+v, want one failure", st)
	}

	s := &sink{t: t}
	s.listen(addr) // same address is now served
	defer s.close()

	obj := bytes.Repeat([]byte{0xCC}, 16)
	waitFor(t, "send succeeds once the peer exists", func() bool {
		return u.Send(context.Background(), obj) == nil
	})
	waitFor(t, "bytes arrive", func() bool { return len(s.bytesGot()) == len(obj) })
}

// TestCloseIsIdempotent pins shutdown: Close on a never-dialled tunnel and a
// second Close are both no-ops, so teardown paths need no bookkeeping.
func TestCloseIsIdempotent(t *testing.T) {
	u := &UpTunnel{Addr: "127.0.0.1:1", Class: "block", Log: testLog()}
	if err := u.Close(); err != nil {
		t.Fatalf("close on unused tunnel: %v", err)
	}
	if err := u.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

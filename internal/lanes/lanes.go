// Package lanes terminates the per-class object delivery lanes.
//
// Each lane is a dedicated TCP listener carrying exactly one object class. The
// stream is bare — no length prefix, no type tag, no sync marker — so objects
// are delimited by walking their own structure, which objfmt.Reader does. That
// also means a malformed object is unrecoverable for that connection: there is
// no resync point, so the only correct response is to drop the connection and
// let the edge redial.
package lanes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

// Handler consumes one whole object from a lane. Returning an error is logged
// and counted but does not drop the connection: a cluster-side failure on one
// object should not cost us the rest of the stream.
type Handler func(ctx context.Context, obj []byte) error

// Lane is one class listener.
type Lane struct {
	Name   string // "tx" | "subtree" | "block"
	Class  objfmt.Class
	Addr   string // host:port to listen on
	Handle Handler
	Log    *slog.Logger

	MaxObject int // 0 = codec default

	listener net.Listener
	bound    atomic.Bool
	counts   Counters
}

// Bound reports whether the lane's listener is open. Readiness gates on it:
// until every lane is bound the bridge cannot accept delivery.
func (l *Lane) Bound() bool { return l.bound.Load() }

// ListenerAddr returns the bound address, or nil before Serve binds. It exists
// for tests and diagnostics that start lanes on port 0.
func (l *Lane) ListenerAddr() net.Addr {
	if !l.bound.Load() || l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

// Counters are per-lane totals, read via Stats.
type Counters struct {
	Conns   atomic.Uint64
	Active  atomic.Int64 // connections open right now
	Objects atomic.Uint64
	Bytes   atomic.Uint64
	Errors  atomic.Uint64
	Dropped atomic.Uint64 // connections dropped on malformed framing
}

// Stats is a snapshot.
type Stats struct {
	Name                                   string
	Conns, Objects, Bytes, Errors, Dropped uint64
	Active                                 int64
}

// Stats returns a snapshot of this lane's counters.
func (l *Lane) Stats() Stats {
	return Stats{
		Name:    l.Name,
		Conns:   l.counts.Conns.Load(),
		Active:  l.counts.Active.Load(),
		Objects: l.counts.Objects.Load(),
		Bytes:   l.counts.Bytes.Load(),
		Errors:  l.counts.Errors.Load(),
		Dropped: l.counts.Dropped.Load(),
	}
}

// Serve listens and handles connections until ctx is cancelled.
func (l *Lane) Serve(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", l.Addr)
	if err != nil {
		return fmt.Errorf("lane %s: listen %s: %w", l.Name, l.Addr, err)
	}
	l.listener = ln
	l.bound.Store(true)
	defer l.bound.Store(false)
	l.Log.Info("lane listening", "lane", l.Name, "addr", l.Addr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return fmt.Errorf("lane %s: accept: %w", l.Name, err)
		}
		l.counts.Conns.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.serveConn(ctx, conn)
		}()
	}
}

func (l *Lane) serveConn(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	l.counts.Active.Add(1)
	defer l.counts.Active.Add(-1)
	defer func() { _ = conn.Close() }()
	l.Log.Info("lane connection open", "lane", l.Name, "remote", remote)

	// Closing the listener alone does not end a shutdown: the edge holds these
	// connections open for the life of the tunnel, so a reader parked in Next()
	// would block the wait group indefinitely and the process would have to be
	// killed. Closing the connection is what unblocks it.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	if tc, ok := conn.(*net.TCPConn); ok {
		// The edge holds these open for the life of the tunnel; keepalive is what
		// notices a peer that went away without a FIN.
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	rd := objfmt.NewReader(conn, l.Class)
	if l.MaxObject > 0 {
		rd.SetMaxObject(l.MaxObject)
	}

	var onConn uint64
	for {
		if ctx.Err() != nil {
			return
		}
		obj, err := rd.Next()
		if err != nil {
			var ne net.Error
			switch {
			case errors.Is(err, io.EOF):
				l.Log.Info("lane connection closed", "lane", l.Name, "remote", remote, "objects", onConn)

			case ctx.Err() != nil:
				// shutting down

			case errors.Is(err, io.ErrUnexpectedEOF) && onConn == 0:
				// Opened and closed without sending a whole object — a health
				// probe or a pool rotation, not a protocol fault.
				l.Log.Info("lane connection closed before any object",
					"lane", l.Name, "remote", remote)

			case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET), errors.As(err, &ne):
				// The peer went away mid-stream. The edge redials and resends;
				// nothing here is corrupt, so this is not a framing fault.
				l.Log.Warn("lane connection lost mid-stream",
					"lane", l.Name, "remote", remote, "objects", onConn, "err", err)

			default:
				// A real codec fault. The stream is bare — no length prefix, no
				// sync marker — so there is no resync point: every following byte
				// is suspect and the connection must go.
				l.counts.Dropped.Add(1)
				l.Log.Error("lane framing error, dropping connection",
					"lane", l.Name, "remote", remote, "objects", onConn, "err", err)
			}
			return
		}
		if l.MaxObject > 0 && len(obj) > l.MaxObject {
			// The reader's bound only fires while ACCUMULATING an object;
			// one that arrives whole in a single read sails past it. The
			// ceiling is a policy, not a buffering detail, so enforce it on
			// delivery too — and as with any framing fault there is no
			// resync point, so the connection goes.
			l.counts.Dropped.Add(1)
			l.Log.Error("lane object exceeds ceiling, dropping connection",
				"lane", l.Name, "remote", remote, "bytes", len(obj), "max", l.MaxObject)
			return
		}
		onConn++
		l.counts.Objects.Add(1)
		l.counts.Bytes.Add(uint64(len(obj)))

		if err := l.Handle(ctx, obj); err != nil {
			l.counts.Errors.Add(1)
			l.Log.Error("lane handler failed", "lane", l.Name, "bytes", len(obj), "err", err)
		}
	}
}

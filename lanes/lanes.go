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

	"github.com/lightwebinc/teranode-bridge/internal/obs"
)

// Handler consumes one whole object from a lane. Returning an error is logged
// and counted but does not drop the connection: a cluster-side failure on one
// object should not cost us the rest of the stream.
type Handler func(ctx context.Context, obj []byte) error

// ErrReject marks a handler error as an admission refusal rather than a
// failure: the object was well-framed but does not satisfy the lane's format
// policy, so it is discarded on purpose. Framing is intact, so the connection
// stays and the counter is separate from Errors — a refused object is the
// sender's bug, an error is ours or the cluster's.
var ErrReject = errors.New("rejected")

// Session is a per-connection handler pair. Handle receives each object;
// Flush is called whenever the lane is about to block on the network (the
// reader holds no further whole object) and when the connection ends. A
// handler can therefore pass work on in runs without delaying any of it: at
// high rates a run is many objects, and an object that arrives alone is
// flushed before the lane waits for the next. A Flush error ends the
// connection.
type Session struct {
	Handle Handler
	Flush  func(ctx context.Context) error
}

// Lane is one class listener.
type Lane struct {
	Name   string // "tx" | "subtree" | "block"
	Class  objfmt.Class
	Addr   string // host:port to listen on
	Handle Handler
	Log    *slog.Logger

	// NewSession, when set, is called once per connection and its Session
	// replaces Handle for that connection.
	NewSession func() Session

	MaxObject int // 0 = codec default

	// Owned makes Handle receive an object the handler may retain: a copy in a
	// per-connection slab that is never reused, instead of a slice aliasing the
	// reader's buffer that dies at the next read. On the tx lane this replaces
	// one heap allocation per transaction (the dominant allocation count, and
	// what the garbage collector then has to mark) with one per slab. A slab
	// stays alive while any object in it is referenced, which for a cache that
	// ages out in arrival order costs at most about one slab per connection.
	Owned bool

	// SizeHistogram enables the per-object size distribution on this lane.
	// Off by default on the tx lane: the lane bytes counter already gives the
	// mean, and at megatransaction-per-second rates a twelve-bucket observation
	// per object is real work on the read path for a shape that rarely changes.
	// On the subtree and block lanes the rate is orders of magnitude lower and
	// the distribution is genuinely informative, so it is on.
	SizeHistogram bool

	listener net.Listener
	bound    atomic.Bool
	counts   Counters

	// Objects and bytes are counted per connection (the hot path then never
	// shares a cache line between connection goroutines) and summed at Stats
	// time; counts.Objects/Bytes hold the totals of connections already closed.
	liveMu sync.Mutex
	live   map[*connCounts]struct{}
}

// connCounts are one connection's object and byte totals.
type connCounts struct {
	objects, bytes atomic.Uint64
}

func (l *Lane) addLive(c *connCounts) {
	l.liveMu.Lock()
	if l.live == nil {
		l.live = make(map[*connCounts]struct{})
	}
	l.live[c] = struct{}{}
	l.liveMu.Unlock()
}

func (l *Lane) retireLive(c *connCounts) {
	l.liveMu.Lock()
	delete(l.live, c)
	l.counts.Objects.Add(c.objects.Load())
	l.counts.Bytes.Add(c.bytes.Load())
	l.liveMu.Unlock()
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
	Conns    atomic.Uint64
	Active   atomic.Int64 // connections open right now
	Objects  atomic.Uint64
	Bytes    atomic.Uint64
	Errors   atomic.Uint64
	Dropped  atomic.Uint64 // connections dropped on malformed framing
	Rejected atomic.Uint64 // objects refused on format policy (ErrReject)
}

// Stats is a snapshot.
type Stats struct {
	Name                                             string
	Conns, Objects, Bytes, Errors, Dropped, Rejected uint64
	Active                                           int64
}

// Stats returns a snapshot of this lane's counters.
func (l *Lane) Stats() Stats {
	l.liveMu.Lock()
	objects, bytes := l.counts.Objects.Load(), l.counts.Bytes.Load()
	for c := range l.live {
		objects += c.objects.Load()
		bytes += c.bytes.Load()
	}
	l.liveMu.Unlock()
	return Stats{
		Name:     l.Name,
		Conns:    l.counts.Conns.Load(),
		Active:   l.counts.Active.Load(),
		Objects:  objects,
		Bytes:    bytes,
		Errors:   l.counts.Errors.Load(),
		Dropped:  l.counts.Dropped.Load(),
		Rejected: l.counts.Rejected.Load(),
	}
}

// flushReader runs a session's Flush before each read of the connection.
type flushReader struct {
	r     io.Reader
	ctx   context.Context
	flush func(context.Context) error
}

func (f *flushReader) Read(p []byte) (int, error) {
	if err := f.flush(f.ctx); err != nil {
		return 0, err
	}
	return f.r.Read(p)
}

// slabSize is the per-connection arena an Owned lane copies objects into.
// Objects larger than an eighth of it get their own allocation.
const slabSize = 1 << 20

// owner hands out retained copies of objects from per-connection slabs.
type owner struct{ slab []byte }

func (o *owner) own(obj []byte) []byte {
	n := len(obj)
	if n > slabSize/8 {
		b := make([]byte, n)
		copy(b, obj)
		return b
	}
	if cap(o.slab)-len(o.slab) < n {
		o.slab = make([]byte, 0, slabSize)
	}
	off := len(o.slab)
	o.slab = append(o.slab, obj...)
	// Full slice expression: a handler appending to its object must reallocate
	// rather than write into the next object's bytes.
	return o.slab[off : off+n : off+n]
}

// coarseNow is the Unix second, refreshed four times a second by one ticker,
// so the per-object "last object" stamp costs an atomic load instead of a
// clock read and a labelled-metric lookup.
var (
	coarseNow     atomic.Int64
	coarseNowOnce sync.Once
)

func startCoarseClock() {
	coarseNowOnce.Do(func() {
		coarseNow.Store(time.Now().Unix())
		go func() {
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for now := range t.C {
				coarseNow.Store(now.Unix())
			}
		}()
	})
}

// Serve listens and handles connections until ctx is cancelled.
func (l *Lane) Serve(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", l.Addr)
	if err != nil {
		return fmt.Errorf("lane %s: listen %s: %w", l.Name, l.Addr, err)
	}
	l.listener = ln
	startCoarseClock()
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

	handle := l.Handle
	var src io.Reader = conn
	if l.NewSession != nil {
		sess := l.NewSession()
		handle = sess.Handle
		if sess.Flush != nil {
			// The reader only reads when it holds no whole object, so flushing
			// in front of every read hands on everything decoded so far before
			// the lane can block.
			src = &flushReader{r: conn, ctx: ctx, flush: sess.Flush}
			defer func() { _ = sess.Flush(ctx) }()
		}
	}
	rd := objfmt.NewReader(src, l.Class)
	if l.MaxObject > 0 {
		rd.SetMaxObject(l.MaxObject)
	}

	cc := &connCounts{}
	l.addLive(cc)
	defer l.retireLive(cc)
	stamp := obs.LastObjectTime.WithLabelValues(l.Name)
	var lastStamp int64
	var own owner

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
		cc.objects.Add(1)
		cc.bytes.Add(uint64(len(obj)))
		if sec := coarseNow.Load(); sec != lastStamp {
			stamp.Set(float64(sec))
			lastStamp = sec
		}
		if l.SizeHistogram {
			obs.ObjectBytes.WithLabelValues(l.Name).Observe(float64(len(obj)))
		}
		if l.Owned {
			obj = own.own(obj)
		}

		if err := handle(ctx, obj); err != nil {
			if errors.Is(err, ErrReject) {
				l.counts.Rejected.Add(1)
				l.Log.Warn("lane object refused", "lane", l.Name, "remote", remote,
					"bytes", len(obj), "err", err)
				continue
			}
			l.counts.Errors.Add(1)
			l.Log.Error("lane handler failed", "lane", l.Name, "bytes", len(obj), "err", err)
		}
	}
}

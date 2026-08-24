package submit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/teranode-bridge/internal/obs"
)

// UpTunnel submits push frames from this cluster back into the fabric, through
// the tunnel, to the edge's privileged object-ingress ports.
//
// One long-lived connection per class. The stream is bare — no length prefix, no
// sync marker — so a partial write leaves the edge's parser mid-object with no
// way to resynchronise: the only correct recovery is to drop the connection and
// redial, which is what a failed Write does here.
//
// Addrs is a FAILOVER list, not a pool. With slot identity the submit targets
// are the tunnel's per-side slot inners ([sideA]:port, [sideB]:port), and an
// A→B failover moves delivery to the other side while this submitter would
// otherwise keep dialling the dead one forever — the customer-side half of
// "submit inherits STE mobility" that a single static address cannot provide.
// The dialer is sticky on whichever address last worked (submits must not flap
// between edges on transient blips) and advances to the next address only when
// a DIAL fails — a dead side refuses or times out, which is exactly the signal
// the active side has moved. A failed WRITE redials the same address first:
// one broken write is a dropped connection, not evidence the side is gone.
type UpTunnel struct {
	// Addrs are host:port targets for this class's ingress (9143 subtree, 9144
	// block — the bare BRC-143/144 lane numbers), reachable only through the
	// tunnel, tried in order from the last one that worked.
	Addrs []string
	Class string
	Log   *slog.Logger

	mu   sync.Mutex
	conn net.Conn
	cur  int // index into Addrs of the address conn was dialled to / to try next

	sent, bytes, failures, redials atomic.Uint64
}

// Send writes one whole object, dialling or redialling as needed.
func (u *UpTunnel) Send(ctx context.Context, obj []byte) error {
	if len(obj) == 0 {
		return errors.New("uptunnel: empty object")
	}
	// Timed from the caller's point of view, so the lock wait is included: with
	// one connection per class the writes serialise, and time spent queued
	// behind another object is time the reverse path is actually blocked.
	defer obs.Timer(obs.UpTunnelWriteDuration, u.Class)()

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.conn == nil {
		if len(u.Addrs) == 0 {
			return errors.New("uptunnel: no addresses configured")
		}
		// One full pass over the list per Send: start at the sticky index and
		// advance on each dial failure. A Send that exhausts the list fails —
		// the caller's retry gets a fresh pass, again starting from wherever
		// the cursor stopped, so a flapping first address cannot starve the
		// second and a recovered first address is retried eventually.
		var lastErr error
		for range u.Addrs {
			addr := u.Addrs[u.cur%len(u.Addrs)]
			d := net.Dialer{Timeout: 10 * time.Second}
			c, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				u.failures.Add(1)
				lastErr = err
				u.cur = (u.cur + 1) % len(u.Addrs)
				u.Log.Warn("up-tunnel dial failed, trying next", "class", u.Class, "addr", addr, "err", err)
				continue
			}
			u.conn = c
			u.redials.Add(1)
			u.Log.Info("up-tunnel connected", "class", u.Class, "addr", addr)
			break
		}
		if u.conn == nil {
			return fmt.Errorf("uptunnel %s: all %d addresses failed: %w", u.Class, len(u.Addrs), lastErr)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = u.conn.SetWriteDeadline(deadline)

	if _, err := u.conn.Write(obj); err != nil {
		_ = u.conn.Close()
		u.conn = nil
		u.failures.Add(1)
		return fmt.Errorf("uptunnel %s: write: %w", u.Class, err)
	}
	u.sent.Add(1)
	u.bytes.Add(uint64(len(obj)))
	return nil
}

// Close drops the connection.
func (u *UpTunnel) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.conn == nil {
		return nil
	}
	err := u.conn.Close()
	u.conn = nil
	return err
}

// UpStats is a snapshot for logging and metrics.
type UpStats struct {
	Class                          string
	Sent, Bytes, Failures, Redials uint64
}

// Stats returns a snapshot of this class's up-tunnel counters.
func (u *UpTunnel) Stats() UpStats {
	return UpStats{
		Class:    u.Class,
		Sent:     u.sent.Load(),
		Bytes:    u.bytes.Load(),
		Failures: u.failures.Load(),
		Redials:  u.redials.Load(),
	}
}

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
)

// UpTunnel submits push frames from this cluster back into the fabric, through
// the tunnel, to the edge's privileged object-ingress ports.
//
// One long-lived connection per class. The stream is bare — no length prefix, no
// sync marker — so a partial write leaves the edge's parser mid-object with no
// way to resynchronise: the only correct recovery is to drop the connection and
// redial, which is what a failed Write does here.
type UpTunnel struct {
	// Addr is host:port of the edge's ingress for this class (8726 subtree,
	// 8727 block), reachable only through the tunnel.
	Addr  string
	Class string
	Log   *slog.Logger

	mu   sync.Mutex
	conn net.Conn

	sent, bytes, failures, redials atomic.Uint64
}

// Send writes one whole object, dialling or redialling as needed.
func (u *UpTunnel) Send(ctx context.Context, obj []byte) error {
	if len(obj) == 0 {
		return errors.New("uptunnel: empty object")
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.conn == nil {
		d := net.Dialer{Timeout: 10 * time.Second}
		c, err := d.DialContext(ctx, "tcp", u.Addr)
		if err != nil {
			u.failures.Add(1)
			return fmt.Errorf("uptunnel %s: dial %s: %w", u.Class, u.Addr, err)
		}
		u.conn = c
		u.redials.Add(1)
		u.Log.Info("up-tunnel connected", "class", u.Class, "addr", u.Addr)
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

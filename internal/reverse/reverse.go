// Package reverse carries this cluster's own subtrees and blocks back into the
// fabric.
//
// The cluster tells us what it accepted through the blockchain service's
// notification stream. That stream does not say where an object came from — the
// node's own p2p announces every notification to its gossip peers for exactly
// the same reason — so the origin filter is ours: anything the bridge previously
// delivered *into* the cluster is registered, and a notification for a
// registered hash is remote in origin and must not be pushed back up. What
// remains is what this cluster produced.
//
// One consequence is worth stating plainly: content this node learned over
// libp2p while the tunnel was down looks unseen and will be pushed up after
// recovery. That duplicate is bounded by the outage and harmless — every
// receiver dedups by hash — and the clean fix is an origin marker on the
// notification, which is an upstream ask.
package reverse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	pb "github.com/lightwebinc/teranode-bridge/proto/blockchain_api"

	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
	"github.com/lightwebinc/teranode-bridge/internal/tnwire"
)

// Notification types, mirroring model.NotificationType.
const (
	typeSubtree int32 = 1
	typeBlock   int32 = 2
)

// Publisher sends an encoded frame up the tunnel.
type Publisher interface {
	Send(ctx context.Context, obj []byte) error
}

// Store records a published frame so the copy that returns down the delivery
// lanes can be byte-compared against what we actually sent.
type Store interface {
	Put(hash hashid.Hash, class string, body []byte)
}

// Builder turns an accepted object into the push frame for its class. It returns
// ok=false when the object should be skipped without being an error (for
// example a block whose parts are not all available yet).
type Builder interface {
	BuildSubtree(ctx context.Context, hash hashid.Hash) (frame []byte, ok bool, err error)
	BuildBlock(ctx context.Context, hash hashid.Hash) (frame []byte, ok bool, err error)
}

// Config points the subscriber at the cluster.
type Config struct {
	// BlockchainAddr is host:port of the blockchain service's gRPC listener.
	BlockchainAddr string
	// Source identifies this subscriber to the cluster.
	Source string
	// Subtrees and Blocks are the up-tunnel publishers for each class. A nil
	// publisher disables that class.
	Subtrees, Blocks Publisher
	// Published, if set, receives every frame we publish, so the echo that comes
	// back down our own delivery lanes can be verified against it.
	Published Store

	// Ready, when set, gates publishing: the submitter only publishes while it
	// returns true.
	//
	// This exists because the origin filter is only as good as our view of the
	// object plane. A freshly started bridge has an EMPTY registry, so every
	// notification looks unseen — which reads as "locally produced" — and the
	// cluster re-emits notifications for objects it already holds. The mine tag
	// covers blocks, but subtrees have no marker of their own beyond the block
	// that names them, so publishing while blind means republishing other
	// clusters' subtrees under our identity. Refusing to publish until delivery
	// is actually flowing costs nothing real: anything genuinely ours that is
	// missed here is still in the cluster and is re-announced by its own p2p.
	Ready func() bool

	// MineTag, when non-empty, is the local cluster's coinbase_arbitrary_text
	// (e.g. "/teranode1/"). Block notifications whose coinbase does not carry
	// it are treated as REMOTE in origin and skipped, closing the race the
	// seen-registry cannot: a block this cluster learned over libp2p before
	// the fabric delivered it looks unseen and would otherwise be republished
	// upward with false attribution. The check is stateless — derived from
	// block content — so it also survives bridge restarts, which wipe the
	// registry. Subtrees carry no coinbase and need no equivalent: their
	// notification has exactly one producer, local block assembly.
	MineTag []byte

	// GRPCMetrics, when set, instruments the blockchain connection with the
	// standard gRPC client metrics — the same provider Teranode uses.
	GRPCMetrics *grpcprom.ClientMetrics

	// ClusterPoll is how often to read the cluster's FSM state and tip height
	// over the blockchain connection. Zero disables the poll.
	ClusterPoll time.Duration

	// TLS mirrors Teranode's security_level_grpc and its certificate paths.
	// Level 0 (plaintext) is upstream's default; a cluster running any other
	// level cannot be reached at all by a plaintext dial. See dial.go.
	TLS TLSConfig

	// KeepaliveTime and KeepaliveTimeout set the client keepalive policy.
	// Zero picks upstream's client defaults (30s / 20s). KeepaliveTime must be
	// at least the server's grpc_server_min_ping_time_seconds or the server
	// answers GOAWAY too_many_pings.
	KeepaliveTime, KeepaliveTimeout time.Duration

	// PermitWithoutStream allows pings while no stream is open, matching
	// upstream's grpc_permit_without_stream default.
	PermitWithoutStream bool
}

// Subscriber watches the cluster and republishes what it produces.
type Subscriber struct {
	// fsmState and height hold the last cluster state read by
	// RunClusterState, for the health endpoint and the stats line.
	fsmState atomic.Pointer[string]
	height   atomic.Uint64

	cfg   Config
	seen  *registry.Registry
	build Builder
	log   *slog.Logger

	conn   *grpc.ClientConn
	active atomic.Bool

	subtreesUp, blocksUp             atomic.Uint64
	remoteSkipped, skipped, failures atomic.Uint64
	gated, standbyHeld               atomic.Uint64
	foreignSkipped                   atomic.Uint64
	reconnects                       atomic.Uint64
}

// SetActive flips whether this subscriber holds the submitter role. A standby
// keeps its subscription, registry and origin filter warm — promotion is a
// flag flip, not a cold start — but publishes nothing while inactive.
func (s *Subscriber) SetActive(v bool) { s.active.Store(v) }

// Active reports whether this subscriber currently publishes.
func (s *Subscriber) Active() bool { return s.active.Load() }

// New dials the blockchain service.
//
// Transport security follows Teranode's own security_level_grpc; the blockchain
// service registers no API-key interceptor (util.StartGRPCServer is called with
// nil auth options), so no credential beyond TLS is required.
func New(cfg Config, seen *registry.Registry, build Builder, log *slog.Logger) (*Subscriber, error) {
	if cfg.BlockchainAddr == "" {
		return nil, errors.New("reverse: no blockchain address configured")
	}
	if cfg.Source == "" {
		cfg.Source = "teranode-bridge"
	}
	creds, err := cfg.TLS.credentials()
	if err != nil {
		return nil, fmt.Errorf("reverse: blockchain transport security: %w", err)
	}
	ka, clamped := keepaliveParams(cfg.KeepaliveTime, cfg.KeepaliveTimeout, cfg.PermitWithoutStream)
	if clamped {
		log.Warn("blockchain keepalive interval raised to grpc's floor",
			"requested", cfg.KeepaliveTime, "using", ka.Time)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// Without this the client NEVER pings (grpc-go's default is infinity),
		// and a silently dropped path leaves the Subscribe stream's Recv
		// blocked forever — the reconnect loop below is only entered on a Recv
		// error. See keepaliveParams.
		grpc.WithKeepaliveParams(ka),
		// Teranode gives every gRPC client an OTel stats handler and a
		// Prometheus client interceptor (teranode/util/grpc_helper.go
		// GetGRPCClient). A bare connection here left the reverse path with no
		// visibility beyond a reconnect counter, and broke trace continuity on
		// the one call the cluster makes into its own blockchain service on our
		// behalf.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	if cfg.GRPCMetrics != nil {
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(cfg.GRPCMetrics.UnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(cfg.GRPCMetrics.StreamClientInterceptor()),
		)
	}
	conn, err := grpc.NewClient(cfg.BlockchainAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("reverse: dial %s: %w", cfg.BlockchainAddr, err)
	}
	return &Subscriber{cfg: cfg, seen: seen, build: build, log: log, conn: conn}, nil
}

// Run subscribes and republishes until ctx is cancelled, reconnecting on stream
// loss. The cluster restarts and rolls its gRPC connections periodically, so a
// dropped stream is routine rather than exceptional.
func (s *Subscriber) Run(ctx context.Context) error {
	client := pb.NewBlockchainAPIClient(s.conn)
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Source: s.cfg.Source})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.log.Warn("blockchain subscribe failed, retrying", "err", err, "in", backoff)
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = next(backoff)
			continue
		}
		s.log.Info("subscribed to blockchain notifications", "addr", s.cfg.BlockchainAddr, "source", s.cfg.Source)
		backoff = time.Second

		for {
			n, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if !errors.Is(err, io.EOF) {
					s.log.Warn("notification stream lost", "err", err)
				}
				s.reconnects.Add(1)
				break
			}
			// Stamped for EVERY notification type, not just the two we act on.
			// The cluster sends PINGs, so this gauge keeps ticking on an idle
			// chain — which is what makes "the subscription is dead" separable
			// from "the chain is quiet". Acting only on subtree and block
			// notifications would have made a silent stream look like calm.
			obs.Stamp(obs.LastNotificationTime)
			s.handle(ctx, n)
		}
		if !sleep(ctx, backoff) {
			return nil
		}
		backoff = next(backoff)
	}
}

func (s *Subscriber) handle(ctx context.Context, n *pb.Notification) {
	if n.Type != typeSubtree && n.Type != typeBlock {
		return
	}
	hash, err := hashid.FromWire(n.Hash)
	if err != nil {
		s.log.Warn("notification with an unusable hash", "type", n.Type, "err", err)
		return
	}

	// Origin filter. A hash we delivered into the cluster came from the fabric;
	// republishing it would be a loop. A hash we already submitted is a
	// notification for our own push coming back around.
	if dir, known := s.seen.Lookup(registry.Key(hash)); known {
		s.remoteSkipped.Add(1)
		s.log.Debug("skipping notification for an object we did not originate",
			"hash", hash.Display(), "seen_as", dir)
		return
	}

	switch n.Type {
	case typeSubtree:
		s.publish(ctx, "subtree", hash, s.cfg.Subtrees, s.build.BuildSubtree, &s.subtreesUp)
	case typeBlock:
		s.publish(ctx, "block", hash, s.cfg.Blocks, s.build.BuildBlock, &s.blocksUp)
	}
}

func (s *Subscriber) publish(ctx context.Context, class string, hash hashid.Hash, pub Publisher,
	build func(context.Context, hashid.Hash) ([]byte, bool, error), counter *atomic.Uint64) {

	if pub == nil {
		return
	}
	if !s.active.Load() {
		// Standby: the role lives elsewhere. Held, not lost — if this bridge
		// is promoted the cluster's own p2p re-announces recent content, and
		// anything still flowing arrives as fresh notifications.
		s.standbyHeld.Add(1)
		s.log.Debug("standby: holding publish", "class", class, "hash", hash.Display())
		return
	}
	if s.cfg.Ready != nil && !s.cfg.Ready() {
		s.gated.Add(1)
		s.log.Info("not publishing yet: no live view of the object plane",
			"class", class, "hash", hash.Display())
		return
	}
	frame, ok, err := build(ctx, hash)
	if err != nil {
		s.failures.Add(1)
		s.log.Error("building push frame failed", "class", class, "hash", hash.Display(), "err", err)
		return
	}
	if !ok {
		s.skipped.Add(1)
		s.log.Info("skipping object, not fully available", "class", class, "hash", hash.Display())
		return
	}

	// Origin gate: a block whose coinbase does not carry this cluster's tag
	// was mined elsewhere — the cluster learned of it (libp2p, catchup) and
	// notified us, but it is not ours to publish. Without this, gossip winning
	// the race against the fabric turns into a republish with false
	// attribution at every receiving cluster.
	if class == "block" && len(s.cfg.MineTag) > 0 {
		cb, err := tnwire.CoinbaseOf(frame)
		if err != nil {
			s.failures.Add(1)
			s.log.Error("origin check failed to parse coinbase", "hash", hash.Display(), "err", err)
			return
		}
		if !bytes.Contains(cb, s.cfg.MineTag) {
			s.foreignSkipped.Add(1)
			s.log.Info("skipping foreign-origin block (coinbase tag mismatch)",
				"hash", hash.Display(), "tag", string(s.cfg.MineTag))
			return
		}
	}

	// Register BEFORE sending: the object comes straight back down our own
	// delivery lanes (own-traffic exclusion covers only the tx class), and it
	// must be recognised as ours when it does. Keeping the exact bytes turns
	// that unavoidable echo into a free end-to-end correctness check.
	s.seen.Mark(registry.Key(hash), registry.Submitted)
	if s.cfg.Published != nil {
		s.cfg.Published.Put(hash, class, frame)
	}

	if err := pub.Send(ctx, frame); err != nil {
		s.failures.Add(1)
		s.log.Error("up-tunnel submit failed", "class", class, "hash", hash.Display(), "err", err)
		return
	}
	counter.Add(1)
	s.log.Info("published up-tunnel", "class", class, "hash", hash.Display(), "bytes", len(frame))
}

// Close drops the gRPC connection to the blockchain service.
func (s *Subscriber) Close() error { return s.conn.Close() }

// Stats is a snapshot for logging and metrics.
type Stats struct {
	SubtreesUp, BlocksUp, RemoteSkipped, Skipped, Failures, Reconnects uint64
	Gated                                                              uint64
	// ForeignSkipped counts block notifications whose coinbase carried another
	// cluster's tag — gossip-learned blocks correctly not republished.
	ForeignSkipped uint64
	// StandbyHeld counts publishes withheld because this bridge does not hold
	// the submitter role.
	StandbyHeld uint64
	// Active reports whether the submitter role is currently held.
	Active bool
}

// Stats returns a snapshot of the reverse path's counters.
func (s *Subscriber) Stats() Stats {
	return Stats{
		SubtreesUp:     s.subtreesUp.Load(),
		BlocksUp:       s.blocksUp.Load(),
		RemoteSkipped:  s.remoteSkipped.Load(),
		Skipped:        s.skipped.Load(),
		Failures:       s.failures.Load(),
		Reconnects:     s.reconnects.Load(),
		Gated:          s.gated.Load(),
		ForeignSkipped: s.foreignSkipped.Load(),
		StandbyHeld:    s.standbyHeld.Load(),
		Active:         s.active.Load(),
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func next(d time.Duration) time.Duration {
	if d *= 2; d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/lightwebinc/teranode-bridge/proto/blockchain_api"

	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
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
}

// Subscriber watches the cluster and republishes what it produces.
type Subscriber struct {
	cfg   Config
	seen  *registry.Registry
	build Builder
	log   *slog.Logger

	conn *grpc.ClientConn

	subtreesUp, blocksUp             atomic.Uint64
	remoteSkipped, skipped, failures atomic.Uint64
	reconnects                       atomic.Uint64
}

// New dials the blockchain service. The listener is plaintext and
// unauthenticated in this deployment.
func New(cfg Config, seen *registry.Registry, build Builder, log *slog.Logger) (*Subscriber, error) {
	if cfg.BlockchainAddr == "" {
		return nil, errors.New("reverse: no blockchain address configured")
	}
	if cfg.Source == "" {
		cfg.Source = "teranode-bridge"
	}
	conn, err := grpc.NewClient(cfg.BlockchainAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
}

// Stats returns a snapshot of the reverse path's counters.
func (s *Subscriber) Stats() Stats {
	return Stats{
		SubtreesUp:    s.subtreesUp.Load(),
		BlocksUp:      s.blocksUp.Load(),
		RemoteSkipped: s.remoteSkipped.Load(),
		Skipped:       s.skipped.Load(),
		Failures:      s.failures.Load(),
		Reconnects:    s.reconnects.Load(),
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

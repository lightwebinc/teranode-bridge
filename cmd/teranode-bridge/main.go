// Command teranode-bridge is the landing-tier shim for pushed delivery into a
// Teranode cluster.
//
// It terminates the per-class object delivery lanes, hands transactions to the
// cluster's propagation service, and — for subtrees and blocks — stores the
// pushed bytes, announces them into the cluster's Kafka pointing at itself, and
// serves the resulting pull. The cluster runs unmodified: it still learns of an
// object by announcement and still fetches, validates and stores it itself. Only
// the distance changes, from a peer across the wide area to the landing router
// on the same LAN.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"golang.org/x/sync/errgroup"

	"github.com/lightwebinc/teranode-bridge/internal/announce"
	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/lanes"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
	"github.com/lightwebinc/teranode-bridge/internal/retrieval"
	"github.com/lightwebinc/teranode-bridge/internal/reverse"
	"github.com/lightwebinc/teranode-bridge/internal/submit"
	"github.com/lightwebinc/teranode-bridge/internal/tnasset"
)

// Version is stamped at build time with -ldflags "-X main.Version=…". It is
// reported once at startup so a running process can be tied to a build.
var Version = "dev"

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

func main() {
	var (
		txListen      = flag.String("tx-listen", "[::]:8833", "delivery lane: transactions (BRC-12/30)")
		subtreeListen = flag.String("subtree-listen", "[::]:9143", "delivery lane: subtrees (BRC-143)")
		blockListen   = flag.String("block-listen", "[::]:9144", "delivery lane: blocks (BRC-144)")

		retrievalListen = flag.String("retrieval-listen", "[::]:9145", "retrieval plane listen address")
		advertise       = flag.String("advertise", "", "base URL the cluster should dial for pulls, e.g. http://[2001:db8:3f::1]:9145 (required unless -mode sink)")
		apiPrefix       = flag.String("api-prefix", "/api/v1", "path prefix for the retrieval plane; must match what the cluster expects")

		propagation stringList
		brokers     stringList

		subtreeTopic = flag.String("subtree-topic", "subtrees-teranode1", "Kafka topic for subtree announcements")
		blockTopic   = flag.String("block-topic", "blocks-teranode1", "Kafka topic for block announcements")
		peerID       = flag.String("peer-id", "", "peer identity stamped on announcements; leave empty — an identity the cluster's p2p service does not recognise causes block-path fetches to be refused")

		blockchain  = flag.String("blockchain", "", "cluster blockchain gRPC host:port; enables the reverse path (cluster -> fabric)")
		localAsset  = flag.String("local-asset", "", "cluster asset base URL incl. API prefix, e.g. http://192.0.2.10:20090/api/v1 (reverse path)")
		edgeIngress = flag.String("edge-ingress", "", "edge in-fabric ingress host for up-tunnel submits, reachable only through the tunnel")
		subtreePort = flag.Int("edge-subtree-port", 8726, "edge ingress port for BRC-143 subtree submits")
		blockPort   = flag.Int("edge-block-port", 8727, "edge ingress port for BRC-144 block submits")
		submitter   = flag.Bool("submitter", true, "hold the submitter role for this cluster; exactly one bridge per class should")

		mode       = flag.String("mode", "all", "all = full bridge; sink = receive, verify and count only (no cluster targets)")
		cacheBytes = flag.Int64("cache-bytes", 1<<30, "object cache ceiling in bytes")
		cacheTTL   = flag.Duration("cache-ttl", 10*time.Minute, "how long a pushed object stays fetchable")
		maxObject  = flag.Int("max-object", 0, "per-object size ceiling (0 = codec default)")
		statsEvery = flag.Duration("stats-every", time.Minute, "interval between stats lines (0 = off)")
		logLevel   = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Var(&propagation, "propagation", "propagation HTTP base URL(s), comma-separated, e.g. http://192.0.2.10:20833")
	flag.Var(&brokers, "kafka", "cluster Kafka broker(s), comma-separated, e.g. 192.0.2.10:19092")
	flag.Parse()

	log := newLogger(*logLevel)
	sink := *mode == "sink"

	if !sink {
		if len(propagation) == 0 || len(brokers) == 0 || *advertise == "" {
			log.Error("-propagation, -kafka and -advertise are required unless -mode sink")
			os.Exit(2)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	objects := cache.New(cache.Options{MaxBytes: *cacheBytes, TTL: *cacheTTL})
	txs := cache.New(cache.Options{MaxBytes: *cacheBytes, TTL: *cacheTTL})
	seen := registry.New(30*time.Minute, 1<<20)

	var (
		sub      *submit.Submitter
		producer *announce.Producer
		ret      *retrieval.Server
		baseURL  string
		err      error
	)

	if !sink {
		if sub, err = submit.New(submit.Config{Endpoints: propagation}, log); err != nil {
			log.Error("propagation submitter", "err", err)
			os.Exit(1)
		}
		if err := sub.Health(ctx); err != nil {
			// Not fatal: the cluster may still be starting. Delivery will retry
			// per transaction and the failure will be visible in the stats.
			log.Warn("propagation health check failed at startup", "err", err)
		}
		if producer, err = announce.New(announce.Config{
			Brokers:      brokers,
			SubtreeTopic: *subtreeTopic,
			BlockTopic:   *blockTopic,
			PeerID:       *peerID,
		}, log); err != nil {
			log.Error("kafka producer", "err", err)
			os.Exit(1)
		}
		defer producer.Close()
		if err := producer.Ping(ctx); err != nil {
			log.Warn("kafka ping failed at startup", "err", err)
		}
		ret = retrieval.New(retrieval.Config{Listen: *retrievalListen, APIPrefix: *apiPrefix}, objects, objects, txs, log)
		baseURL = ret.BaseURL(*advertise)
		log.Info("bridge starting", "version", Version, "mode", *mode, "announce-url", baseURL,
			"propagation", propagation.String(), "kafka", brokers.String())
	} else {
		log.Info("bridge starting in sink mode: objects are received, verified and counted, but not handed to a cluster",
			"version", Version)
	}

	laneSet := []*lanes.Lane{
		{
			Name: "tx", Class: objfmt.ClassTx, Addr: *txListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleTx(ctx, obj, txs, seen, sub)
			},
		},
		{
			Name: "subtree", Class: objfmt.ClassSubtree, Addr: *subtreeListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleSubtree(ctx, obj, objects, seen, producer, baseURL, log)
			},
		},
		{
			Name: "block", Class: objfmt.ClassBlock, Addr: *blockListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleBlock(ctx, obj, objects, seen, producer, baseURL, log)
			},
		},
	}

	// Reverse path: publish what this cluster produces back into the fabric.
	var rev *reverse.Subscriber
	var upSubtree, upBlock *submit.UpTunnel
	if !sink && *blockchain != "" {
		if *localAsset == "" || *edgeIngress == "" {
			log.Error("-blockchain also requires -local-asset and -edge-ingress")
			os.Exit(2)
		}
		if !*submitter {
			log.Info("submitter role disabled; the reverse path stays idle on this bridge")
		} else {
			upSubtree = &submit.UpTunnel{Addr: net.JoinHostPort(*edgeIngress, strconv.Itoa(*subtreePort)), Class: "subtree", Log: log}
			upBlock = &submit.UpTunnel{Addr: net.JoinHostPort(*edgeIngress, strconv.Itoa(*blockPort)), Class: "block", Log: log}
		}
		asset := tnasset.New(*localAsset, 30*time.Second)
		rev, err = reverse.New(reverse.Config{
			BlockchainAddr: *blockchain,
			Source:         "teranode-bridge",
			Subtrees:       nilIfNil(upSubtree),
			Blocks:         nilIfNil(upBlock),
			Published:      cacheStore{objects},
		}, seen, builderFunc{asset}, log)
		if err != nil {
			log.Error("reverse path", "err", err)
			os.Exit(1)
		}
		defer func() { _ = rev.Close() }()
		log.Info("reverse path enabled", "blockchain", *blockchain, "asset", *localAsset,
			"edge_ingress", *edgeIngress, "submitter", *submitter)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, ln := range laneSet {
		g.Go(func() error { return ln.Serve(gctx) })
	}
	if ret != nil {
		g.Go(func() error { return ret.Serve(gctx) })
	}
	if rev != nil {
		g.Go(func() error { return rev.Run(gctx) })
	}
	if *statsEvery > 0 {
		g.Go(func() error {
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-gctx.Done():
					return nil
				case <-t.C:
					logStats(log, laneSet, objects, txs, seen, sub, producer, ret, rev, upSubtree, upBlock)
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		log.Error("bridge stopped", "err", err)
		os.Exit(1)
	}
	logStats(log, laneSet, objects, txs, seen, sub, producer, ret, rev, upSubtree, upBlock)
	log.Info("bridge stopped")
}

// handleTx caches the transaction (so it can serve as a subtree member later)
// and hands it to the cluster.
func handleTx(ctx context.Context, obj []byte, txs *cache.Cache, seen *registry.Registry,
	sub *submit.Submitter) error {

	id, err := objfmt.TxID(obj)
	if err != nil {
		return fmt.Errorf("txid: %w", err)
	}
	txs.Put(cache.Key(id), "tx", obj)

	if _, known := seen.Mark(registry.Key(id), registry.Delivered); known {
		// Re-delivery after an A/B failover or reconnect. Already handed over.
		return nil
	}
	if sub == nil {
		return nil // sink mode
	}
	outcome, err := sub.Tx(ctx, obj)
	switch outcome {
	case submit.Accepted, submit.Duplicate:
		return nil
	default:
		return fmt.Errorf("tx %s %s: %w", hashid.Hash(id).Display(), outcome, err)
	}
}

// handleSubtree stores the frame and announces it, pointing the cluster at our
// retrieval plane.
func handleSubtree(ctx context.Context, obj []byte, objects *cache.Cache, seen *registry.Registry,
	producer *announce.Producer, baseURL string, log *slog.Logger) error {

	if len(obj) < objfmt.SubtreeHeaderSize {
		return fmt.Errorf("subtree frame too short: %d bytes", len(obj))
	}
	root, err := hashid.FromWire(obj[:32])
	if err != nil {
		return err
	}
	nodes := (len(obj) - objfmt.SubtreeHeaderSize) / 32

	// Store before announcing: the cluster does not retry a failed subtree
	// fetch, so the object must be servable the instant the announcement lands.
	verifyEcho(objects, seen, root, "subtree", obj, log)
	objects.Put(cache.Key(root), "subtree", obj)

	if dir, known := seen.Mark(registry.Key(root), registry.Delivered); known {
		log.Debug("subtree already seen, not re-announced", "root", root.Display(), "seen_as", dir)
		return nil
	}
	log.Info("subtree received", "root", root.Display(), "nodes", nodes, "bytes", len(obj))
	if producer == nil {
		return nil
	}
	return producer.Subtree(ctx, root.Display(), baseURL)
}

// handleBlock stores the frame and announces it.
func handleBlock(ctx context.Context, obj []byte, objects *cache.Cache, seen *registry.Registry,
	producer *announce.Producer, baseURL string, log *slog.Logger) error {

	if len(obj) < objfmt.BlockPrefixSize {
		return fmt.Errorf("block frame too short: %d bytes", len(obj))
	}
	// A block is identified by the hash of its 80-byte header, exactly as the
	// chain identifies it.
	id := hashid.DoubleSHA256(obj[:80])

	verifyEcho(objects, seen, id, "block", obj, log)
	objects.Put(cache.Key(id), "block", obj)

	if dir, known := seen.Mark(registry.Key(id), registry.Delivered); known {
		log.Debug("block already seen, not re-announced", "hash", id.Display(), "seen_as", dir)
		return nil
	}
	log.Info("block received", "hash", id.Display(), "bytes", len(obj))
	if producer == nil {
		return nil
	}
	return producer.Block(ctx, id.Display(), baseURL)
}

func logStats(log *slog.Logger, laneSet []*lanes.Lane, objects, txs *cache.Cache,
	seen *registry.Registry, sub *submit.Submitter, producer *announce.Producer, ret *retrieval.Server,
	rev *reverse.Subscriber, upSubtree, upBlock *submit.UpTunnel) {

	for _, l := range laneSet {
		st := l.Stats()
		log.Info("lane stats", "lane", st.Name, "conns", st.Conns, "objects", st.Objects,
			"bytes", st.Bytes, "errors", st.Errors, "dropped", st.Dropped)
	}
	objStats, txStats := objects.Stats(), txs.Stats()
	log.Info("cache stats", "objects", objStats.Entries, "object_bytes", objStats.Bytes,
		"txs", txStats.Entries, "tx_bytes", txStats.Bytes, "evicted", objStats.Evicted+txStats.Evicted)
	rs := seen.Stats()
	log.Info("registry stats", "entries", rs.Entries, "duplicates", rs.Hits)
	if sub != nil {
		s := sub.Stats()
		log.Info("submit stats", "accepted", s.Accepted, "duplicate", s.Duplicate,
			"rejected", s.Rejected, "failed", s.Failed)
	}
	if producer != nil {
		a := producer.Stats()
		log.Info("announce stats", "subtrees", a.Subtrees, "blocks", a.Blocks, "failures", a.Failures)
	}
	if ret != nil {
		r := ret.Stats()
		log.Info("retrieval stats", "subtree", r.Subtree, "subtree_data", r.SubtreeData,
			"txs", r.Txs, "block", r.Block, "miss", r.Miss, "errors", r.Errors)
	}
	if rev != nil {
		v := rev.Stats()
		log.Info("reverse stats", "subtrees_up", v.SubtreesUp, "blocks_up", v.BlocksUp,
			"remote_skipped", v.RemoteSkipped, "skipped", v.Skipped,
			"failures", v.Failures, "reconnects", v.Reconnects)
	}
	for _, u := range []*submit.UpTunnel{upSubtree, upBlock} {
		if u == nil {
			continue
		}
		us := u.Stats()
		log.Info("up-tunnel stats", "class", us.Class, "sent", us.Sent, "bytes", us.Bytes,
			"failures", us.Failures, "redials", us.Redials)
	}
}

// verifyEcho checks an object we published against the copy the fabric hands
// back on our own delivery lanes.
//
// Own-traffic exclusion covers only the tx class, so everything this cluster
// publishes returns to it. That is not waste: it is a free end-to-end proof that
// what the fabric carried is byte-for-byte what we sent, across encode, submit,
// reframe, multicast, strip and deliver. A mismatch means the object plane is
// corrupting data and is worth an error even though the object is then dropped.
func verifyEcho(objects *cache.Cache, seen *registry.Registry, h hashid.Hash, class string, obj []byte, log *slog.Logger) {
	dir, known := seen.Lookup(registry.Key(h))
	if !known || dir != registry.Submitted {
		return
	}
	sent, _, ok := objects.Get(cache.Key(h))
	if !ok {
		return // published before this process started, or aged out
	}
	if !bytes.Equal(sent, obj) {
		log.Error("ECHO MISMATCH: the fabric returned different bytes than we published",
			"class", class, "hash", h.Display(), "sent_bytes", len(sent), "back_bytes", len(obj))
		return
	}
	log.Info("echo verified byte-identical", "class", class, "hash", h.Display(), "bytes", len(obj))
}

// builderFunc adapts the asset client to the reverse path's Builder.
type builderFunc struct{ a *tnasset.Client }

func (b builderFunc) BuildSubtree(ctx context.Context, h hashid.Hash) ([]byte, bool, error) {
	return b.a.BuildSubtree(ctx, h)
}
func (b builderFunc) BuildBlock(ctx context.Context, h hashid.Hash) ([]byte, bool, error) {
	return b.a.BuildBlock(ctx, h)
}

// cacheStore lets the reverse path keep the frames it published.
type cacheStore struct{ c *cache.Cache }

func (s cacheStore) Put(h hashid.Hash, class string, body []byte) {
	s.c.Put(cache.Key(h), class, body)
}

// nilIfNil keeps a typed-nil *UpTunnel from becoming a non-nil Publisher
// interface, which would make the reverse path try to send through nothing.
func nilIfNil(u *submit.UpTunnel) reverse.Publisher {
	if u == nil {
		return nil
	}
	return u
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

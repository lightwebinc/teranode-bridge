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
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-common/objfmt"
	"golang.org/x/sync/errgroup"

	"github.com/lightwebinc/teranode-bridge/internal/announce"
	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/lanes"
	"github.com/lightwebinc/teranode-bridge/internal/metrics"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
	"github.com/lightwebinc/teranode-bridge/internal/retrieval"
	"github.com/lightwebinc/teranode-bridge/internal/reverse"
	"github.com/lightwebinc/teranode-bridge/internal/submit"
	"github.com/lightwebinc/teranode-bridge/internal/tnasset"
	"github.com/lightwebinc/teranode-bridge/internal/tnwire"
	"github.com/lightwebinc/teranode-bridge/internal/txpipe"
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
		txListen      = flag.String("tx-listen", "[::]:8833", "delivery lane: transactions (BRC-30 EF)")
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
		submitProbe = flag.String("submitter-probe", "", "primary bridge /readyz URL; set on a STANDBY (-submitter=false) to auto-promote when the primary dies and demote when it returns")

		mode       = flag.String("mode", "all", "all = full bridge; sink = receive, verify and count only (no cluster targets)")
		cacheBytes = flag.Int64("cache-bytes", 1<<30, "object cache ceiling in bytes")
		cacheTTL   = flag.Duration("cache-ttl", 10*time.Minute, "how long a pushed object stays fetchable")
		maxObject  = flag.Int("max-object", 0, "per-object size ceiling (0 = codec default)")
		statsEvery = flag.Duration("stats-every", time.Minute, "interval between stats lines (0 = off)")

		submitGrace = flag.Duration("submitter-grace", 45*time.Second, "after start, wait this long before the reverse path may publish; the registry starts empty, so a bridge that publishes immediately can mistake objects the cluster already held for its own")
		submitBlind = flag.Bool("submitter-when-blind", false, "publish even while no delivery lane is connected; unsafe unless this cluster is the only publisher, because the origin filter needs a live view of the object plane")

		mineTag = flag.String("mine-tag", "", "this cluster's coinbase_arbitrary_text (e.g. /teranode1/); when set, only blocks whose coinbase carries it are published up — stateless origin detection with no Teranode change")

		txBatch      = flag.Int("tx-batch", 512, "transactions per batch submit (POST /txs); 1-1023, out-of-range values are clamped. 1023 not 1024: propagation checks its limit at the TOP of the read loop, so a batch of exactly 1024 is fully processed and THEN answered 400")
		txBatchBytes = flag.Int("tx-batch-bytes", 8<<20, "batch body ceiling in bytes (server cap 32 MiB)")
		txLinger     = flag.Duration("tx-linger", 2*time.Millisecond, "max age of a non-full batch before it is sent")
		txInflight   = flag.Int("tx-inflight", 4, "concurrent batch submissions in flight")
		txBuilders   = flag.Int("tx-builders", 4, "parallel batch builders (power of two, max 16)")
		txRetries    = flag.Int("tx-retries", 3, "per-tx retries for failures that resolve with time (missing parent); 0 = off")

		metricsAddr = flag.String("metrics-addr", "[::]:9146", "HTTP listener for /metrics, /healthz, /readyz, /loglevel (empty = off)")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
		logFormat   = flag.String("log-format", "text", "log encoding: text (stderr) or json (stdout, the fleet aggregation contract)")
		debug       = flag.Bool("debug", false, "deprecated alias for -log-level=debug")
		instanceID  = flag.String("instance-id", "", "service.instance.id for logs and metrics (default: hostname)")
	)
	flag.Var(&propagation, "propagation", "propagation HTTP base URL(s), comma-separated, e.g. http://192.0.2.10:20833")
	flag.Var(&brokers, "kafka", "cluster Kafka broker(s), comma-separated, e.g. 192.0.2.10:19092")
	flag.Parse()

	level := logging.ParseLevel(*logLevel)
	if *debug {
		level = slog.LevelDebug
	}
	levelVar := logging.Init(logging.Options{
		Service:    metrics.ServiceName,
		InstanceID: *instanceID,
		Version:    Version,
		Level:      level,
		Format:     logging.ParseFormat(*logFormat),
	})
	log := slog.Default()
	stopSIGHUP := logging.InstallSIGHUPToggle(levelVar, level)
	defer stopSIGHUP()

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
	// The tx cache is generational, not LRU: at megatransaction-per-second
	// rates the LRU bookkeeping costs ~5× the payload in heap and its pointer
	// graph is what the GC scans. See cache.Generational.
	txs := cache.NewGenerational(cache.Options{MaxBytes: *cacheBytes, TTL: *cacheTTL})
	seen := registry.New(30*time.Minute, 1<<20)

	var (
		pipe     *txpipe.Pipe
		producer *announce.Producer
		ret      *retrieval.Server
		baseURL  string
		err      error
	)

	if !sink {
		if pipe, err = txpipe.New(txpipe.Config{
			Endpoints:     propagation,
			BatchTxs:      *txBatch,
			BatchBytes:    *txBatchBytes,
			Linger:        *txLinger,
			Inflight:      *txInflight,
			Builders:      *txBuilders,
			RetryAttempts: *txRetries,
		}, log); err != nil {
			log.Error("tx pipeline", "err", err)
			os.Exit(1)
		}
		if err := probeHealth(ctx, propagation); err != nil {
			// Not fatal: the cluster may still be starting. Failures stay
			// visible in the pipe's failed/rejected counters.
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

	rec := metrics.New(Version, *instanceID)

	started := time.Now()
	laneSet := []*lanes.Lane{
		{
			Name: "tx", Class: objfmt.ClassTx, Addr: *txListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleTx(ctx, obj, txs, seen, pipe)
			},
		},
		{
			Name: "subtree", Class: objfmt.ClassSubtree, Addr: *subtreeListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleSubtree(ctx, obj, objects, seen, producer, baseURL, rec, log)
			},
		},
		{
			Name: "block", Class: objfmt.ClassBlock, Addr: *blockListen, Log: log, MaxObject: *maxObject,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleBlock(ctx, obj, objects, seen, producer, baseURL, rec, log)
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
		// Publishers exist regardless of role: a standby keeps them warm so
		// promotion is a flag flip, not a construction path.
		upSubtree = &submit.UpTunnel{Addr: net.JoinHostPort(*edgeIngress, strconv.Itoa(*subtreePort)), Class: "subtree", Log: log}
		upBlock = &submit.UpTunnel{Addr: net.JoinHostPort(*edgeIngress, strconv.Itoa(*blockPort)), Class: "block", Log: log}
		if !*submitter {
			log.Info("starting as standby: reverse path subscribed but not publishing")
		}
		asset := tnasset.New(*localAsset, 30*time.Second)
		rev, err = reverse.New(reverse.Config{
			BlockchainAddr: *blockchain,
			Source:         "teranode-bridge",
			Subtrees:       nilIfNil(upSubtree),
			Blocks:         nilIfNil(upBlock),
			Published:      cacheStore{objects},
			MineTag:        []byte(*mineTag),
			Ready:          submitterReady(laneSet, started, *submitGrace, *submitBlind, log),
		}, seen, builderFunc{asset}, log)
		if err != nil {
			log.Error("reverse path", "err", err)
			os.Exit(1)
		}
		defer func() { _ = rev.Close() }()
		log.Info("reverse path enabled", "blockchain", *blockchain, "asset", *localAsset,
			"edge_ingress", *edgeIngress, "submitter", *submitter)
		rev.SetActive(*submitter)
		if *submitProbe != "" {
			if *submitter {
				log.Warn("-submitter-probe is a STANDBY setting; ignored on a configured primary")
			}
		}
	}

	// Point the recorder at every live subsystem now that they all exist. Nil
	// members simply contribute no series, so a sink exposes exactly the lane
	// and cache metrics and nothing it cannot honestly report.
	rec.SetSources(metrics.Sources{
		Lanes: laneSet, Objects: objects, Txs: txs, Seen: seen,
		Tx: pipe, Announce: producer, Retrieval: ret, Reverse: rev,
		UpTunnels: []*submit.UpTunnel{upSubtree, upBlock},
	})
	inv := hostinfo.Gather(metrics.ServiceName, Version)
	rec.SetHostInfo(inv)
	rec.SetLevelVar(levelVar)
	log.Info("host.inventory", "inventory", inv)

	g, gctx := errgroup.WithContext(ctx)
	for _, ln := range laneSet {
		g.Go(func() error { return ln.Serve(gctx) })
	}
	if ret != nil {
		g.Go(func() error { return ret.Serve(gctx) })
	}
	if pipe != nil {
		g.Go(func() error { return pipe.Run(gctx) })
	}
	if rev != nil {
		g.Go(func() error { return rev.Run(gctx) })
		if *submitProbe != "" && !*submitter {
			g.Go(func() error {
				rev.RunPromoter(gctx, reverse.PromoterConfig{ProbeURL: *submitProbe}, log)
				return nil
			})
		}
	}
	if *metricsAddr != "" {
		g.Go(func() error { return rec.Serve(gctx, *metricsAddr) })
	}
	// Ready once every lane is bound: before that the bridge cannot accept
	// delivery, and whatever gates on /readyz should hold off.
	g.Go(func() error {
		if waitLanesBound(gctx, laneSet) {
			rec.SetReady(true)
			log.Info("all lanes bound; reporting ready")
		}
		return nil
	})
	if *statsEvery > 0 {
		g.Go(func() error {
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-gctx.Done():
					return nil
				case <-t.C:
					logStats(log, laneSet, objects, txs, seen, pipe, producer, ret, rev, upSubtree, upBlock)
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		log.Error("bridge stopped", "err", err)
		os.Exit(1)
	}
	logStats(log, laneSet, objects, txs, seen, pipe, producer, ret, rev, upSubtree, upBlock)
	log.Info("bridge stopped")
}

// handleTx caches the transaction (so it can serve as a subtree member later)
// and enqueues it for batched submission. The read loop never waits on the
// cluster: submission latency is the pipe's problem, and backpressure arrives
// only when the pipe's queue is full.
func handleTx(ctx context.Context, obj []byte, txs *cache.Generational, seen *registry.Registry,
	pipe *txpipe.Pipe) error {

	id, err := objfmt.TxID(obj)
	if err != nil {
		return fmt.Errorf("txid: %w", err)
	}
	// ONE copy per transaction, shared immutably by the cache and the pipe.
	// obj itself aliases the lane reader's buffer and dies at the next read.
	owned := append([]byte(nil), obj...)
	txs.PutOwned(cache.Key(id), "tx", owned)

	if _, known := seen.Mark(registry.Key(id), registry.Delivered); known {
		// Re-delivery after an A/B failover or reconnect. Already handed over.
		return nil
	}
	if pipe == nil {
		return nil // sink mode
	}
	return pipe.EnqueueOwned(ctx, owned, hashid.Hash(id))
}

// probeHealth reports whether at least one propagation endpoint answers.
func probeHealth(ctx context.Context, endpoints []string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	var lastErr error
	for _, ep := range endpoints {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(ep, "/")+"/health", nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("%s/health: http %d", ep, resp.StatusCode)
	}
	return lastErr
}

// handleSubtree stores the frame and announces it, pointing the cluster at our
// retrieval plane.
func handleSubtree(ctx context.Context, obj []byte, objects *cache.Cache, seen *registry.Registry,
	producer *announce.Producer, baseURL string, rec *metrics.Recorder, log *slog.Logger) error {

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
	verifyEcho(objects, seen, root, "subtree", obj, rec, log)
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
	producer *announce.Producer, baseURL string, rec *metrics.Recorder, log *slog.Logger) error {

	if len(obj) < objfmt.BlockPrefixSize {
		return fmt.Errorf("block frame too short: %d bytes", len(obj))
	}
	// A block is identified by the hash of its 80-byte header, exactly as the
	// chain identifies it.
	id := hashid.DoubleSHA256(obj[:80])

	verifyEcho(objects, seen, id, "block", obj, rec, log)
	objects.Put(cache.Key(id), "block", obj)

	// A block on a delivery lane came from the object plane, and it names every
	// subtree it contains. Marking those roots now — before the cluster finishes
	// validating the block and starts emitting subtree notifications for them —
	// is what stops this cluster republishing another cluster's subtrees when
	// gossip wins the race. The block itself is covered by the mine tag; its
	// subtrees have no such marker of their own, so they inherit the block's.
	// Mark never downgrades an existing entry, so our own echo is unaffected.
	if roots, err := tnwire.SubtreeRootsOf(obj); err == nil {
		for _, r := range roots {
			seen.Mark(registry.Key(r), registry.Delivered)
		}
	} else {
		log.Warn("could not read subtree roots from block frame", "hash", id.Display(), "err", err)
	}

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

func logStats(log *slog.Logger, laneSet []*lanes.Lane, objects *cache.Cache, txs *cache.Generational,
	seen *registry.Registry, pipe *txpipe.Pipe, producer *announce.Producer, ret *retrieval.Server,
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
	if pipe != nil {
		s := pipe.Stats()
		log.Info("submit stats", "accepted", s.Accepted, "rejected", s.Rejected,
			"failed", s.Failed, "batches", s.Batches, "retried", s.Retried,
			"retry_ok", s.RetryAccepted, "queue", s.Queue,
			"seals_dep", s.SealDep, "seals_linger", s.SealLinger)
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
func verifyEcho(objects *cache.Cache, seen *registry.Registry, h hashid.Hash, class string, obj []byte,
	rec *metrics.Recorder, log *slog.Logger) {

	dir, known := seen.Lookup(registry.Key(h))
	if !known || dir != registry.Submitted {
		return
	}
	sent, _, ok := objects.Get(cache.Key(h))
	if !ok {
		return // published before this process started, or aged out
	}
	if !bytes.Equal(sent, obj) {
		rec.EchoMismatch()
		log.Error("ECHO MISMATCH: the fabric returned different bytes than we published",
			"class", class, "hash", h.Display(), "sent_bytes", len(sent), "back_bytes", len(obj))
		return
	}
	rec.EchoVerified()
	log.Info("echo verified byte-identical", "class", class, "hash", h.Display(), "bytes", len(obj))
}

// waitLanesBound blocks until every lane has its listener open, and reports
// whether that happened (false means the context was cancelled first).
func waitLanesBound(ctx context.Context, laneSet []*lanes.Lane) bool {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		bound := 0
		for _, l := range laneSet {
			if l.Bound() {
				bound++
			}
		}
		if bound == len(laneSet) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
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

// submitterReady decides whether the reverse path may publish right now.
//
// Two conditions, both about trusting the origin filter rather than about the
// cluster's health:
//
//   - a startup grace, because the seen-registry begins EMPTY. A cluster
//     re-announces objects it already holds, and to a bridge with no history
//     every one of those looks locally produced. Publishing during that window
//     attributes other clusters' work to this one.
//   - at least one live delivery lane, because "did this come from the object
//     plane?" is unanswerable while nothing is being delivered. Blocks are
//     covered by the mine tag, but subtrees inherit their origin from the block
//     that names them — and that block arrives on a lane.
//
// Anything genuinely ours that is skipped here is not lost: it stays in the
// cluster, and the cluster keeps announcing it.
func submitterReady(laneSet []*lanes.Lane, started time.Time, grace time.Duration, allowBlind bool, log *slog.Logger) func() bool {
	var announced atomic.Bool
	// Origin evidence arrives on the CLASS lanes, not the transaction lane. A
	// block frame carries this cluster's mine tag and names every subtree it
	// contains, so it is the only thing that can prove an object came from the
	// object plane rather than from gossip. Judging readiness on the tx lane
	// would arm the submitter while blind to exactly the evidence it needs —
	// which is how a cluster ends up republishing another cluster's subtrees.
	need := map[string]bool{"subtree": true, "block": true}
	return func() bool {
		if time.Since(started) < grace {
			return false
		}
		if allowBlind {
			return true
		}
		for _, l := range laneSet {
			st := l.Stats()
			if need[st.Name] && st.Active == 0 {
				return false
			}
		}
		if announced.CompareAndSwap(false, true) {
			log.Info("reverse path armed: class lanes are live, origin filter is trustworthy")
		}
		return true
	}
}

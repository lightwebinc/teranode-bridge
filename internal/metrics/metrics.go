// Package metrics exposes the bridge's counters on a Prometheus endpoint,
// alongside the health, profiling and runtime-log-level routes every service in
// the stack serves.
//
// # Why a collector rather than instrumented call sites
//
// Every subsystem already keeps its own atomic counters and hands out an
// immutable snapshot through a Stats method. This package reads those snapshots
// at scrape time instead of incrementing a second set of counters next to the
// first: one source of truth, no risk of the two drifting, and no metrics
// dependency pushed down into the hot paths.
//
// That shape cannot express a distribution — a histogram has to be observed
// where the work happens — so latency, size and freshness instrumentation lives
// in internal/obs and is registered here alongside the collector.
//
// # Deliberate deviation: no OTel SDK for counters
//
// The rest of the stack routes cold-path counters through the OTel SDK for
// optional OTLP push. This binary registers directly on the Prometheus registry
// instead, for two reasons. The bridge has no per-packet path — it counts whole
// objects, so the SDK's cost was never the deciding factor — and a directly
// registered counter is **present at zero**, while an OTel counter is absent
// from /metrics until its first increment. Presence at zero is what lets
// `teranode_bridge_echo_mismatch_total == 0` be a meaningful alert expression
// rather than one that silently matches nothing on a freshly restarted process.
//
// Tracing is a separate matter and does use the OTel SDK: see internal/tracing.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/teranode-bridge/internal/announce"
	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/health"
	"github.com/lightwebinc/teranode-bridge/internal/lanes"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
	"github.com/lightwebinc/teranode-bridge/internal/retrieval"
	"github.com/lightwebinc/teranode-bridge/internal/reverse"
	"github.com/lightwebinc/teranode-bridge/internal/submit"
	"github.com/lightwebinc/teranode-bridge/internal/txpipe"
)

// ServiceName is the OTel service.name; it must match what logging.Init is
// given so logs, metrics and traces carry the same identity.
const ServiceName = "teranode-bridge"

// StatSource is any cache-shaped subsystem that can snapshot its counters.
type StatSource interface{ Stats() cache.Stats }

// Sources are the live subsystems to read at scrape time. Every field is
// optional: a nil one simply contributes no series, which is what makes a
// sink-mode process expose the lane and cache metrics and nothing else.
type Sources struct {
	Lanes     []*lanes.Lane
	Objects   *cache.Cache
	Txs       StatSource
	Seen      *registry.Registry
	Tx        *txpipe.Pipe
	Announce  *announce.Producer
	Retrieval *retrieval.Server
	Reverse   *reverse.Subscriber
	UpTunnels []*submit.UpTunnel
}

// Options configures the recorder.
type Options struct {
	// Version and Instance identify the build and the process.
	Version, Instance string
	// LegacyPrefix dual-emits every pre-existing series under the old `btb_`
	// name as well as `teranode_bridge_`, so dashboards written against the old
	// names survive the cutover. Series introduced with the new naming are
	// never aliased.
	LegacyPrefix bool
	// Strict makes every health dependency gate readiness, which is Teranode's
	// all-or-nothing CheckAll behaviour. Off by default; see internal/health
	// for why the bridge separates gating from advisory dependencies.
	Strict bool
}

// Recorder owns the registry and serves the observability endpoints.
type Recorder struct {
	reg *prometheus.Registry
	src atomic.Pointer[Sources]

	version, instance string

	levelVar *slog.LevelVar
	ready    atomic.Bool
	host     atomic.Pointer[hostLabels]

	echoOK, echoBad atomic.Uint64

	strict bool
	checks atomic.Pointer[[]health.Check]

	// GRPCClientMetrics instruments the bridge's outbound gRPC, matching what
	// teranode/util/grpc_helper.go gives every client. Handed to the reverse
	// path at construction; registered here so it lands on the bridge's own
	// registry rather than the default one.
	GRPCClientMetrics *grpcprom.ClientMetrics
}

// New builds a recorder. Sources are attached later with [Recorder.SetSources],
// because the subsystems it reads are constructed after the counters that the
// startup path already needs to increment.
func New(opts Options) *Recorder {
	legacyOn.Store(opts.LegacyPrefix)

	r := &Recorder{
		reg:               prometheus.NewRegistry(),
		version:           opts.Version,
		instance:          opts.Instance,
		strict:            opts.Strict,
		GRPCClientMetrics: grpcprom.NewClientMetrics(grpcprom.WithClientHandlingTimeHistogram()),
	}
	r.src.Store(&Sources{})
	empty := []health.Check{}
	r.checks.Store(&empty)

	r.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.GRPCClientMetrics,
		&collector{rec: r},
	)
	r.reg.MustRegister(obs.Collectors()...)
	return r
}

// SetSources attaches the live subsystems to read at scrape time.
func (r *Recorder) SetSources(src Sources) { r.src.Store(&src) }

// SetChecks attaches the health dependency list. It is a snapshot swap rather
// than a fixed slice because the reverse path — and therefore the cluster
// dependencies — is constructed after the endpoint is already serving.
func (r *Recorder) SetChecks(checks []health.Check) { r.checks.Store(&checks) }

// EchoVerified records an object that came back byte-identical.
func (r *Recorder) EchoVerified() { r.echoOK.Add(1) }

// EchoMismatch records an object that came back altered.
func (r *Recorder) EchoMismatch() { r.echoBad.Add(1) }

// SetLevelVar registers the runtime log-level variable so Serve exposes
// POST /loglevel.
func (r *Recorder) SetLevelVar(lvl *slog.LevelVar) { r.levelVar = lvl }

// SetReady flips readiness. The bridge reports ready once every lane is bound:
// before that it cannot accept delivery, and a load balancer or unit dependency
// should hold off.
func (r *Recorder) SetReady(v bool) { r.ready.Store(v) }

// Ready reports the narrow lane-bound readiness that /readyz answers.
func (r *Recorder) Ready() bool { return r.ready.Load() }

// SetHostInfo publishes a slim host_info gauge carrying low-cardinality host
// facts, joining the host.inventory log event emitted at startup.
func (r *Recorder) SetHostInfo(inv hostinfo.Inventory) {
	var nic, speed string
	for _, ifc := range inv.Interfaces {
		if ifc.OperState == "up" && (len(ifc.IPv4) > 0 || len(ifc.IPv6) > 0) {
			nic = ifc.Name
			if ifc.SpeedMbps > 0 {
				speed = strconv.Itoa(ifc.SpeedMbps)
			}
			break
		}
	}
	r.host.Store(newHostLabels(inv, nic, speed))
}

func itoa(v int) string    { return strconv.Itoa(v) }
func utoa(v uint64) string { return strconv.FormatUint(v, 10) }

// ProfileRates enables the block and mutex profilers.
//
// Without these calls /debug/pprof/block and /debug/pprof/mutex return an EMPTY
// profile rather than an error, which reads as "no contention" during exactly
// the investigation that needs the opposite answer. Teranode exposes the same
// two rates as settings (BlockProfileRate, MutexProfileFraction) and logs when
// they are on; both are off by default because they are not free.
func ProfileRates(blockRate, mutexFraction int, log *slog.Logger) {
	if blockRate > 0 {
		runtime.SetBlockProfileRate(blockRate)
		log.Info("block profiler enabled", "rate", blockRate, "endpoint", "/debug/pprof/block")
	}
	if mutexFraction > 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
		log.Info("mutex profiler enabled", "fraction", mutexFraction, "endpoint", "/debug/pprof/mutex")
	}
}

// Serve runs the endpoint until ctx is cancelled.
func (r *Recorder) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{}))

	checks := func() []health.Check {
		if p := r.checks.Load(); p != nil {
			return *p
		}
		return nil
	}
	// Teranode's health surface: /health and /health/readiness probe
	// dependencies, /health/liveness deliberately does not, and ?timeout=
	// overrides the probe deadline. Matching the paths and the body shape means
	// cluster-side tooling reads a bridge exactly as it reads a Teranode
	// service.
	mux.HandleFunc("GET /health", health.Handler(ctx, false, r.strict, checks))
	mux.HandleFunc("GET /health/readiness", health.Handler(ctx, false, r.strict, checks))
	mux.HandleFunc("GET /health/liveness", health.Handler(ctx, true, r.strict, checks))

	// /healthz and /readyz keep their old, narrow meanings. /readyz in
	// particular is the failover contract: a standby polls the primary's
	// /readyz to decide whether to promote itself, so it must stay a statement
	// about THIS process being able to accept delivery, never about the health
	// of a shared dependency both bridges see fail at once.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if r.levelVar != nil {
		mux.Handle("/loglevel", logging.LevelHandler(r.levelVar))
	}
	// pprof rides the metrics listener: same trust boundary, and profiling a
	// throughput problem without a rebuild is worth the routes. The set matches
	// Teranode's profiler mux (teranode/daemon/daemon_services.go) — Index
	// dispatches the named runtime profiles (heap, goroutine, block, mutex),
	// while cmdline, profile, symbol and trace are separate handlers. Omitting
	// symbol breaks `go tool pprof` symbolization against this endpoint.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	slog.Info("metrics listening", "addr", addr)

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

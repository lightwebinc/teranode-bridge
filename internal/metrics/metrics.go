// Package metrics exposes the bridge's counters on a Prometheus endpoint,
// alongside the health and runtime-log-level routes every service in the stack
// serves.
//
// # Why a collector rather than instrumented call sites
//
// Every subsystem already keeps its own atomic counters and hands out an
// immutable snapshot through a Stats method. This package reads those snapshots
// at scrape time instead of incrementing a second set of counters next to the
// first: one source of truth, no risk of the two drifting, and no metrics
// dependency pushed down into the hot paths. The only counters owned here are
// the echo results, which have no other home.
//
// # Deliberate deviation: no OTel SDK
//
// The rest of the stack routes cold-path counters through the OTel SDK for
// optional OTLP push. This binary registers directly on the Prometheus registry
// instead, for two reasons. The bridge has no per-packet path — it counts whole
// objects, so the SDK's cost was never the deciding factor — and a directly
// registered counter is **present at zero**, while an OTel counter is absent
// from /metrics until its first increment. Presence at zero is what lets
// `btb_echo_mismatch_total == 0` be a meaningful alert expression rather than
// one that silently matches nothing on a freshly restarted process.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/teranode-bridge/internal/announce"
	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/lanes"
	"github.com/lightwebinc/teranode-bridge/internal/registry"
	"github.com/lightwebinc/teranode-bridge/internal/retrieval"
	"github.com/lightwebinc/teranode-bridge/internal/reverse"
	"github.com/lightwebinc/teranode-bridge/internal/submit"
	"github.com/lightwebinc/teranode-bridge/internal/txpipe"
)

// ServiceName is the OTel service.name; it must match what logging.Init is
// given so logs and metrics carry the same identity.
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

// Recorder owns the registry and serves the observability endpoints.
type Recorder struct {
	reg *prometheus.Registry
	src atomic.Pointer[Sources]

	levelVar *slog.LevelVar
	ready    atomic.Bool

	echoVerified prometheus.Counter
	echoMismatch prometheus.Counter
}

// New builds a recorder. Sources are attached later with [Recorder.SetSources],
// because the subsystems it reads are constructed after the counters that the
// startup path already needs to increment.
func New(version, instance string) *Recorder {
	r := &Recorder{reg: prometheus.NewRegistry()}
	r.src.Store(&Sources{})

	r.echoVerified = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "btb_echo_verified_total",
		Help: "Objects this bridge published that returned on its own delivery lanes byte-identical.",
	})
	r.echoMismatch = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "btb_echo_mismatch_total",
		Help: "Objects that returned on the delivery lanes with DIFFERENT bytes than were published — an object-plane data-integrity fault, never expected to be non-zero.",
	})
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "btb_build_info",
		Help: "Build identity (value always 1).",
	}, []string{"version", "instance"})
	buildInfo.WithLabelValues(version, instance).Set(1)

	r.reg.MustRegister(
		r.echoVerified, r.echoMismatch, buildInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&collector{rec: r},
	)
	return r
}

// SetSources attaches the live subsystems to read at scrape time.
func (r *Recorder) SetSources(src Sources) { r.src.Store(&src) }

// EchoVerified records an object that came back byte-identical.
func (r *Recorder) EchoVerified() { r.echoVerified.Inc() }

// EchoMismatch records an object that came back altered.
func (r *Recorder) EchoMismatch() { r.echoMismatch.Inc() }

// SetLevelVar registers the runtime log-level variable so Serve exposes
// POST /loglevel.
func (r *Recorder) SetLevelVar(lvl *slog.LevelVar) { r.levelVar = lvl }

// SetReady flips readiness. The bridge reports ready once every lane is bound:
// before that it cannot accept delivery, and a load balancer or unit dependency
// should hold off.
func (r *Recorder) SetReady(v bool) { r.ready.Store(v) }

// SetHostInfo publishes a slim btb_host_info gauge carrying low-cardinality
// host facts, joining the host.inventory log event emitted at startup.
// Best-effort; registration errors are ignored.
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
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "btb_host_info",
		Help: "Static host facts (value always 1); join with the host.inventory log event.",
	}, []string{"hostname", "kernel_version", "cpu_logical", "mem_bytes", "nic", "speed_mbps", "version"})
	if err := r.reg.Register(g); err != nil {
		return
	}
	g.WithLabelValues(
		inv.Hostname, inv.KernelVersion,
		strconv.Itoa(inv.CPULogical),
		strconv.FormatUint(inv.MemTotalBytes, 10),
		nic, speed, inv.Version,
	).Set(1)
}

// Serve runs the endpoint until ctx is cancelled.
func (r *Recorder) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{}))
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
	// throughput problem without a rebuild is worth the four routes.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/heap", pprof.Index)
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

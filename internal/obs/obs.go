// Package obs owns the bridge's latency, size and freshness instrumentation —
// everything the scrape-time collector in internal/metrics cannot express.
//
// # Why a second metrics package
//
// internal/metrics reads immutable Stats snapshots at scrape time, which is the
// right shape for counters a subsystem already keeps. It cannot produce a
// histogram: a distribution has to be observed where the work happens. So the
// call-site instrumentation lives here, in a leaf package every subsystem can
// import without pulling in the collector that reads them all.
//
// # Alignment with Teranode
//
// Names are `teranode_bridge_<name>`, matching the Namespace/Subsystem pattern
// every Teranode service uses (see teranode/services/*/metrics.go). Buckets are
// Teranode's own bucket sets, copied verbatim from teranode/util/metrics.go, so
// a bridge histogram and a cluster histogram bucket identically and can share a
// panel, a recording rule and a quantile.
//
// The collectors are registered by [Collectors] into the bridge's private
// registry rather than through promauto and the default registerer, because the
// bridge deliberately keeps one registry it owns end to end.
package obs

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Namespace and Subsystem place every series in this package on the Teranode
// metric grid: `teranode_bridge_*`.
const (
	Namespace = "teranode"
	Subsystem = "bridge"
)

// Bucket sets, copied verbatim from teranode/util/metrics.go. Do not "improve"
// these: their whole value is that they are byte-identical to the cluster's, so
// histogram_quantile over a bridge series and a cluster series means the same
// thing.
var (
	// BucketsMicroSeconds spans 128µs to 262ms.
	BucketsMicroSeconds = []float64{
		128e-6, 256e-6, 512e-6, 1024e-6, 2048e-6, 4096e-6, 8192e-6, 16384e-6, 32768e-6, 65536e-6, 131072e-6, 262144e-6,
	}
	// BucketsMilliSeconds spans 1ms to 4s.
	BucketsMilliSeconds = []float64{
		1e-3, 2e-3, 4e-3, 16e-3, 32e-3, 64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3,
	}
	// BucketsMilliLongSeconds spans 64ms to 131s.
	BucketsMilliLongSeconds = []float64{
		64e-3, 128e-3, 256e-3, 512e-3, 1024e-3, 2048e-3, 4096e-3, 8192e-3, 16384e-3, 32768e-3, 65536e-3, 131072e-3,
	}
	// BucketsSeconds spans 1s to 2048s.
	BucketsSeconds = []float64{
		1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048,
	}
	// BucketsSizeSmall spans 1 byte to 32KiB.
	BucketsSizeSmall = []float64{
		1, 16, 32, 64, 128, 256, 1024, 2048, 4096, 8192, 16384, 32768,
	}
	// BucketsSize spans 128 bytes to 256KiB.
	BucketsSize = []float64{
		128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
	}
)

func hist(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: Subsystem,
		Name: name, Help: help, Buckets: buckets,
	}, labels)
}

func gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: Subsystem,
		Name: name, Help: help,
	}, labels)
}

func counter(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: Subsystem,
		Name: name, Help: help,
	}, labels)
}

// Latency and size distributions.
var (
	// SubmitDuration times one POST /txs round trip, labelled by how it ended.
	// `ok` is a 2xx, `rejected` a 4xx on merits, `failed` transport or 5xx,
	// `rate_limited` a 429. Separating them is what distinguishes a propagation
	// endpoint that is slow from one that is refusing quickly.
	SubmitDuration = hist("submit_duration_seconds",
		"Round-trip time of one batch submission to a propagation endpoint, by outcome.",
		BucketsMilliSeconds, "outcome")

	// RetrievalDuration times a cluster pull, from request accepted to response
	// written. This sits on the cluster's block-validation critical path: a slow
	// pull here is indistinguishable, cluster-side, from a slow peer.
	RetrievalDuration = hist("retrieval_duration_seconds",
		"Time to serve one cluster pull, by route.",
		BucketsMilliSeconds, "route")

	// AnnounceDuration times ProduceSync — the ack from the cluster's Kafka.
	AnnounceDuration = hist("announce_duration_seconds",
		"Time to produce one announcement and receive its acks, by object class.",
		BucketsMilliSeconds, "class")

	// AssetFetchDuration times the reverse path's fetch from the cluster's asset
	// service. Long buckets: a block assembled from many subtrees is not a
	// millisecond operation.
	AssetFetchDuration = hist("asset_fetch_duration_seconds",
		"Time for the reverse path to build an object from the cluster's asset service, by class.",
		BucketsMilliLongSeconds, "class")

	// AnnounceToFirstPull is the bridge's own end-to-end SLI: from the moment an
	// announcement is acked to the moment the cluster first pulls that object.
	// It is the only measurement of whether the announce-shim trick is working;
	// nothing on either side of the bridge measures it. A rising p99 means the
	// cluster is slow to act on announcements, which shows up nowhere else until
	// the cache TTL starts evicting objects before they are fetched.
	AnnounceToFirstPull = hist("announce_to_first_pull_seconds",
		"Delay from announcement ack to the cluster's first pull of that object, by class.",
		BucketsSeconds, "class")

	// UpTunnelWriteDuration times a whole-object write to the edge ingress.
	UpTunnelWriteDuration = hist("uptunnel_write_duration_seconds",
		"Time to write one object to the object-plane ingress, by class.",
		BucketsMilliSeconds, "class")

	// ObjectBytes is the size distribution of objects read off a delivery lane.
	// The lane bytes counter gives the mean; only this gives the shape.
	ObjectBytes = hist("object_bytes",
		"Size of whole objects read off a delivery lane.",
		BucketsSize, "lane")
)

// Freshness gauges. Every counter in the bridge is monotonic, so a stalled lane,
// a dead subscription and a quiet network all render as the same flat line.
// These are unix timestamps, alertable as `time() - metric > N` without needing
// to know the traffic baseline.
var (
	LastObjectTime = gauge("last_object_timestamp_seconds",
		"Unix time of the last whole object read off this delivery lane; seeded with the process start time, so the value is always an age.", "lane")
	LastAnnounceTime = gauge("last_announce_timestamp_seconds",
		"Unix time of the last successful announcement, by class; seeded with the process start time.", "class")
	LastPullTime = gauge("last_pull_timestamp_seconds",
		"Unix time of the last cluster pull served; seeded with the process start time.")
	LastSubmitTime = gauge("last_submit_timestamp_seconds",
		"Unix time of the last accepted batch submission; seeded with the process start time.")
	LastNotificationTime = gauge("last_notification_timestamp_seconds",
		"Unix time of the last blockchain notification received; seeded with the process start time. A live subscription on an idle cluster still ticks, because the cluster sends PINGs.")
)

// Cluster-state gauges. The bridge holds a blockchain connection anyway; reading
// state from it turns "announce succeeded but nothing happened" from a mystery
// into a reading. Teranode's own services treat FSM state as a health
// dependency (services/blockchain/fsm.go CheckFSM) and export it as a gauge.
var (
	ClusterFSMState = gauge("cluster_fsm_state",
		"Cluster blockchain FSM state as a number: 0 IDLE, 1 RUNNING, 2 CATCHINGBLOCKS, -1 unknown/unreachable.")
	ClusterFSMStateInfo = gauge("cluster_fsm_state_info",
		"1 for the cluster's current FSM state, 0 for every other known state.", "state")
	ClusterBlockHeight = gauge("cluster_block_height",
		"Height of the cluster's best block header, as the cluster reports it.")
	ClusterProbeErrors = counter("cluster_probe_errors_total",
		"Failed reads of cluster state (FSM or best block header) over the blockchain connection.", "call")
)

// FSMStates is every state name the cluster-state gauge publishes, so
// ClusterFSMStateInfo carries a zero for the states that are not current rather
// than dropping their series.
var FSMStates = []string{"IDLE", "RUNNING", "CATCHINGBLOCKS", "UNKNOWN"}

// Stamp sets a freshness gauge to now. Kept as a function so call sites read as
// an event ("we just served a pull"), not as a clock read.
func Stamp(g *prometheus.GaugeVec, labels ...string) {
	g.WithLabelValues(labels...).Set(float64(time.Now().Unix()))
}

// Since observes a duration on a labelled histogram.
func Since(h *prometheus.HistogramVec, start time.Time, labels ...string) {
	h.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
}

// Collectors returns everything this package owns, for registration into the
// bridge's registry. Series with labels are absent until first use; the ones
// that must be present at zero for an alert to be meaningful are pre-created by
// [Preset].
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		SubmitDuration, RetrievalDuration, AnnounceDuration, AssetFetchDuration,
		AnnounceToFirstPull, UpTunnelWriteDuration, ObjectBytes,
		LastObjectTime, LastAnnounceTime, LastPullTime, LastSubmitTime, LastNotificationTime,
		ClusterFSMState, ClusterFSMStateInfo, ClusterBlockHeight, ClusterProbeErrors,
		kafkaCollectors(),
	}
}

// Preset creates the label combinations that must exist for an alert expression
// to match a freshly restarted process — the same reasoning the metrics package
// gives for registering counters directly rather than through the OTel SDK.
// `time() - metric > N` on an absent series matches nothing, which is exactly
// the silent failure a staleness alert exists to catch.
//
// The freshness gauges are seeded with the process START time, not zero. Zero
// would also be present, but `time() - 0` is fifty-odd years: the alert fires
// the instant the process starts rather than after its threshold, and every
// dashboard panel is unreadable until the first object lands. Seeding with the
// start time makes the value mean "age since we could first have received one",
// so a bridge that never receives anything trips the alert exactly N seconds
// after boot — which is the behaviour the alert was written for.
func Preset(start time.Time, lanes []string, reverse bool) {
	t0 := float64(start.Unix())

	// Histograms too. An empty HistogramVec renders as a blank panel and makes
	// a quantile alert match nothing, which reads identically to "healthy" —
	// the same trap the counters avoid by being registered directly. The label
	// sets here are closed, so presetting them costs a fixed handful of series.
	for _, l := range lanes {
		LastObjectTime.WithLabelValues(l).Set(t0)
		ObjectBytes.WithLabelValues(l)
	}
	for _, o := range []string{"ok", "rejected", "failed", "rate_limited"} {
		SubmitDuration.WithLabelValues(o)
	}
	for _, r := range []string{"subtree", "subtree_data", "txs", "block", "unserved"} {
		RetrievalDuration.WithLabelValues(r)
	}
	for _, c := range []string{"subtree", "block"} {
		AnnounceDuration.WithLabelValues(c)
		AnnounceToFirstPull.WithLabelValues(c)
		AssetFetchDuration.WithLabelValues(c)
		UpTunnelWriteDuration.WithLabelValues(c)
	}
	for _, c := range []string{"subtree", "block"} {
		LastAnnounceTime.WithLabelValues(c).Set(t0)
	}
	LastPullTime.WithLabelValues().Set(t0)
	LastSubmitTime.WithLabelValues().Set(t0)
	if reverse {
		LastNotificationTime.WithLabelValues().Set(t0)
		ClusterFSMState.WithLabelValues().Set(-1)
		for _, s := range FSMStates {
			ClusterFSMStateInfo.WithLabelValues(s).Set(0)
		}
		ClusterFSMStateInfo.WithLabelValues("UNKNOWN").Set(1)
	}
}

// SetFSMState publishes both FSM gauges from one state name, keeping the numeric
// gauge and the per-state info gauge from ever disagreeing.
func SetFSMState(name string, code float64) {
	ClusterFSMState.WithLabelValues().Set(code)
	for _, s := range FSMStates {
		v := 0.0
		if s == name {
			v = 1
		}
		ClusterFSMStateInfo.WithLabelValues(s).Set(v)
	}
}

// FirstPull matches an announcement to the cluster's first pull of that object.
//
// Bounded on purpose: an object that is never pulled would otherwise leak an
// entry forever. Entries older than the retention window are swept, and the map
// is hard-capped — past the cap new announcements are simply not tracked, which
// costs an observation, never memory.
type FirstPull struct {
	mu      sync.Mutex
	at      map[string]entry
	max     int
	ttl     time.Duration
	lastGC  time.Time
	nowFunc func() time.Time
}

type entry struct {
	t     time.Time
	class string
}

// NewFirstPull returns a tracker holding at most capacity announcements for ttl.
func NewFirstPull(capacity int, ttl time.Duration) *FirstPull {
	if capacity <= 0 {
		capacity = 1 << 16
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &FirstPull{at: make(map[string]entry), max: capacity, ttl: ttl, nowFunc: time.Now}
}

// Announced records that an announcement for key was acked now.
func (f *FirstPull) Announced(key, class string) {
	if f == nil {
		return
	}
	now := f.nowFunc()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gc(now)
	if len(f.at) >= f.max {
		return
	}
	f.at[key] = entry{t: now, class: class}
}

// Pulled observes the announce→pull delay for key, if this is its first pull.
// Later pulls of the same object are ignored: the metric is about the cluster's
// reaction time, not its fetch count.
func (f *FirstPull) Pulled(key string) {
	if f == nil {
		return
	}
	now := f.nowFunc()
	f.mu.Lock()
	e, ok := f.at[key]
	if ok {
		delete(f.at, key)
	}
	f.mu.Unlock()
	if !ok {
		return
	}
	AnnounceToFirstPull.WithLabelValues(e.class).Observe(now.Sub(e.t).Seconds())
}

// Len reports how many announcements are awaiting a first pull.
func (f *FirstPull) Len() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.at)
}

// gc drops expired entries. Called under the lock, and at most once a second so
// a burst of announcements does not walk the map on every insert.
func (f *FirstPull) gc(now time.Time) {
	if now.Sub(f.lastGC) < time.Second {
		return
	}
	f.lastGC = now
	for k, e := range f.at {
		if now.Sub(e.t) > f.ttl {
			delete(f.at, k)
		}
	}
}

// Timer starts a timer and returns the function that observes it, for the
// `defer obs.Timer(h, labels...)()` form where a function has several return
// paths and timing every one of them by hand would be a maintenance trap.
func Timer(h *prometheus.HistogramVec, labels ...string) func() {
	start := time.Now()
	return func() { Since(h, start, labels...) }
}

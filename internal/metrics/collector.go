package metrics

import (
	"sync/atomic"

	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
)

// Metric naming.
//
// Every series here is `teranode_bridge_<name>`, which is what the
// Namespace/Subsystem pair produces for every Teranode service (see
// teranode/services/*/metrics.go and teranode/docs/references/prometheusMetrics.md).
// The bridge previously used a `btb_` prefix of its own; that name is off the
// Teranode grid, so a dashboard, recording rule or metric_relabel keyed on
// `teranode_` skipped the bridge entirely.
//
// The legacy prefix is dual-emitted for one release so existing multicast-fleet
// dashboards keep working during the cutover. Only series that EXISTED under
// `btb_` get an alias: a metric introduced with the new naming has no dashboard
// to keep working and is emitted once.
const (
	modernPrefix = "teranode_bridge_"
	legacyPrefix = "btb_"
)

// legacyOn is read at scrape time so the alias can be turned off by
// configuration without rebuilding the descriptor table.
var legacyOn atomic.Bool

// pair is one metric under both names. The legacy descriptor is nil for series
// that never existed under the old prefix.
type pair struct {
	modern *prometheus.Desc
	legacy *prometheus.Desc
}

// newPair declares a metric under both prefixes.
func newPair(name, help string, labels []string) pair {
	return pair{
		modern: prometheus.NewDesc(modernPrefix+name, help, labels, nil),
		legacy: prometheus.NewDesc(legacyPrefix+name, help, labels, nil),
	}
}

// newModern declares a metric that exists only under the new prefix.
func newModern(name, help string, labels []string) pair {
	return pair{modern: prometheus.NewDesc(modernPrefix+name, help, labels, nil)}
}

// descs enumerates both names for Describe.
func (p pair) descs() []*prometheus.Desc {
	if p.legacy != nil && legacyOn.Load() {
		return []*prometheus.Desc{p.modern, p.legacy}
	}
	return []*prometheus.Desc{p.modern}
}

// Descriptors. Counters are `_total`; anything that can go down is a gauge.
//
// The `lane` label matches the stack-wide convention (tx|subtree|block), so a
// bridge's series line up with the delivery-side series for the same lane.
var (
	laneConns = newPair("lane_connections_total",
		"Connections accepted on a delivery lane.", []string{"lane"})
	laneActive = newModern("lane_connections_active",
		"Connections open on a delivery lane right now. Zero on a lane that should be fed is a whole-edge outage, which no counter can show.", []string{"lane"})
	laneObjects = newPair("lane_objects_total",
		"Whole objects read off a delivery lane.", []string{"lane"})
	laneBytes = newPair("lane_bytes_total",
		"Object bytes read off a delivery lane.", []string{"lane"})
	laneErrors = newPair("lane_handler_errors_total",
		"Objects whose handler failed. The object is lost; the connection is kept.", []string{"lane"})
	laneDropped = newPair("lane_connections_dropped_total",
		"Connections dropped on a framing fault. Bare streams have no resync point, so every byte after a codec fault is suspect and the connection must go.", []string{"lane"})
	laneRejected = newPair("lane_objects_rejected_total",
		"Well-framed objects refused on lane format policy — on `tx`, a transaction that is not BRC-30 extended format. The object is discarded and the connection kept; a non-zero rate means a sender is emitting the wrong format, not that the cluster is unhealthy.", []string{"lane"})

	cacheEntries = newPair("cache_entries",
		"Objects currently held.", []string{"kind"})
	cacheBytes = newPair("cache_bytes",
		"Bytes currently held.", []string{"kind"})
	cacheStored = newPair("cache_stored_total",
		"Objects admitted to the cache.", []string{"kind"})
	cacheHits = newPair("cache_hits_total",
		"Lookups served from the cache.", []string{"kind"})
	cacheMisses = newPair("cache_misses_total",
		"Lookups for an object not held (absent or expired).", []string{"kind"})
	cacheEvicted = newPair("cache_evicted_total",
		"Objects evicted by the byte ceiling. Expected under load; only a problem when paired with retrieval misses.", []string{"kind"})
	cacheExpired = newPair("cache_expired_total",
		"Objects dropped on TTL expiry at lookup time.", []string{"kind"})

	registryEntries = newPair("registry_entries",
		"Hashes currently in the seen-registry.", nil)
	registryDups = newPair("registry_duplicates_total",
		"Objects recognised as already seen — re-delivery after a failover or reconnect, or our own echo returning.", nil)

	submitTotal = newPair("submit_total",
		"Transactions handed to the cluster's propagation service, by outcome. `rejected` is the cluster refusing on merits (after any retries); `failed` is transport or server fault.", []string{"outcome"})

	pipeEnqueued = newModern("txpipe_enqueued_total",
		"Transactions accepted into the pipe. Subtract the submit outcomes to see what is still in flight.", nil)
	pipeBatches = newPair("txpipe_batches_total",
		"Batch submissions shipped to POST /txs.", nil)
	pipeSeals = newPair("txpipe_batch_seals_total",
		"Why batches were sealed. `dependency` = a tx referenced a parent in the open batch (the /txs contract forbids parent+child in one request); `linger` = age; `size`/`bytes` = full.", []string{"reason"})
	pipeRetried = newPair("txpipe_retried_total",
		"Transactions re-submitted individually after a partial batch failure (missing-parent resolves once the parent lands).", nil)
	pipeRetryOK = newPair("txpipe_retry_accepted_total",
		"Retried transactions the cluster then accepted.", nil)
	pipeUnattributed = newPair("txpipe_unattributed_total",
		"Error lines in a partial-failure response that named no member of the batch they answered. The outcome of that many transactions is UNKNOWN — they are excluded from `accepted` rather than assumed good, so a non-zero rate here means delivery counts are incomplete, not that delivery failed.", nil)
	pipeRateLimited = newPair("txpipe_rate_limited_total",
		"Batches refused by a propagation endpoint's HTTP rate limiter (429). The limiter is per source IP and per endpoint, so a sustained rate here means one bridge is saturating one endpoint — add endpoints or raise the server's limit; batch size will not help.", nil)
	pipeQueue = newPair("txpipe_queue_depth",
		"Transactions waiting in the pipe. At the ceiling, backpressure blocks the lane read loop.", nil)

	announceTotal = newPair("announce_total",
		"Announcements produced to the cluster's Kafka, by object class.", []string{"class"})
	announceFailures = newPair("announce_failures_total",
		"Announcements that failed to produce. The object stays cached but the cluster never learns of it.", nil)
	announceBuffered = newModern("kafka_producer_buffered_records",
		"Records the Kafka client still holds unproduced. A sustained non-zero level is an announce BACKLOG — objects the cluster has not been told about yet, which no failure counter reports.", nil)
	announceAwaitingPull = newModern("announce_awaiting_pull",
		"Announcements acked but not yet followed by a pull. A level that keeps climbing means the cluster is not acting on announcements — the failure mode the announce-shim design is most exposed to.", nil)

	retrievalServed = newPair("retrieval_served_total",
		"Cluster pulls served from the cache, by route.", []string{"route"})
	retrievalMisses = newPair("retrieval_misses_total",
		"Pulls answered 404 because the object (or a subtree member) was not held. The cluster falls back to its ordinary peer-pull path.", nil)
	retrievalErrors = newPair("retrieval_errors_total",
		"Pulls answered 5xx on a genuine bridge-side fault.", nil)
	retrievalUnserved = newPair("retrieval_unserved_route_total",
		"Requests for routes the bridge does not serve at all, by class. class=\"chain_sync\" is the canary for the catchup-divert: the cluster selected the bridge as a chain-sync source. Rare bursts accompany multi-peer degradation (the cached-alternatives walk skips the divert gate); a SUSTAINED rate means the synthetic peer-id no longer diverts catchup — the id was registered or gate semantics changed on upgrade — and must alarm before the next node falls behind.", []string{"class"})

	reversePublished = newPair("reverse_published_total",
		"Objects this cluster produced that were published back onto the object plane, by class.", []string{"class"})
	reverseSkipped = newPair("reverse_skipped_total",
		"Notifications not published. `remote_origin` is the origin filter working as intended (the object came from the fabric); `unavailable` is the asset service not holding it yet.", []string{"reason"})
	reverseFailures = newPair("reverse_failures_total",
		"Reverse-path failures: asset fetch, frame encode, or upward submit.", nil)
	submitterActive = newPair("submitter_active",
		"1 while this bridge holds the submitter role (publishes cluster output upward), 0 on a standby. Across one cluster this must SUM TO EXACTLY 1 per class: 0 means nothing is published upward, 2 means double publish.", nil)

	reverseReconnects = newPair("reverse_reconnects_total",
		"Blockchain notification stream reconnects. Routine — the cluster rolls its gRPC connections.", nil)

	upObjects = newPair("uptunnel_objects_total",
		"Objects written to the object-plane ingress, by class.", []string{"class"})
	upBytes = newPair("uptunnel_bytes_total",
		"Bytes written to the object-plane ingress, by class.", []string{"class"})
	upFailures = newPair("uptunnel_failures_total",
		"Dial or write failures on the upward connection.", []string{"class"})
	upRedials = newPair("uptunnel_redials_total",
		"Dials of the upward connection. One at startup is normal; a climbing count tracks failures and means the ingress is flapping.", []string{"class"})

	echoVerified = newPair("echo_verified_total",
		"Objects this bridge published that returned on its own delivery lanes byte-identical.", nil)
	echoMismatch = newPair("echo_mismatch_total",
		"Objects that returned on the delivery lanes with DIFFERENT bytes than were published — an object-plane data-integrity fault, never expected to be non-zero.", nil)

	buildInfo = newPair("build_info",
		"Build identity (value always 1).", []string{"version", "instance"})
	hostInfoDesc = newPair("host_info",
		"Static host facts (value always 1); join with the host.inventory log event.", []string{"hostname", "kernel_version", "cpu_logical", "mem_bytes", "nic", "speed_mbps", "version"})
)

// allPairs is every descriptor, for Describe.
func allPairs() []pair {
	return []pair{
		laneConns, laneActive, laneObjects, laneBytes, laneErrors, laneDropped, laneRejected,
		cacheEntries, cacheBytes, cacheStored, cacheHits, cacheMisses, cacheEvicted, cacheExpired,
		registryEntries, registryDups, submitTotal, announceTotal, announceFailures,
		announceBuffered, announceAwaitingPull,
		pipeEnqueued, pipeBatches, pipeSeals, pipeRetried, pipeRetryOK,
		pipeUnattributed, pipeRateLimited, pipeQueue,
		retrievalServed, retrievalMisses, retrievalErrors, retrievalUnserved,
		reversePublished, reverseSkipped, reverseFailures, reverseReconnects, submitterActive,
		upObjects, upBytes, upFailures, upRedials,
		echoVerified, echoMismatch, buildInfo, hostInfoDesc,
	}
}

// collector reads the live subsystem snapshots at scrape time.
type collector struct{ rec *Recorder }

// emitCache writes one cache's series under the given kind label.
func emitCache(gauge, counter func(pair, float64, ...string), kind string, s cache.Stats) {
	gauge(cacheEntries, float64(s.Entries), kind)
	gauge(cacheBytes, float64(s.Bytes), kind)
	counter(cacheStored, float64(s.Stored), kind)
	counter(cacheHits, float64(s.Hits), kind)
	counter(cacheMisses, float64(s.Misses), kind)
	counter(cacheEvicted, float64(s.Evicted), kind)
	counter(cacheExpired, float64(s.Expired), kind)
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, p := range allPairs() {
		for _, d := range p.descs() {
			ch <- d
		}
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	src := c.rec.src.Load()
	if src == nil {
		return
	}
	dual := legacyOn.Load()
	emit := func(t prometheus.ValueType, p pair, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(p.modern, t, v, labels...)
		if dual && p.legacy != nil {
			ch <- prometheus.MustNewConstMetric(p.legacy, t, v, labels...)
		}
	}
	counter := func(p pair, v float64, labels ...string) {
		emit(prometheus.CounterValue, p, v, labels...)
	}
	gauge := func(p pair, v float64, labels ...string) {
		emit(prometheus.GaugeValue, p, v, labels...)
	}

	// Identity. Present from the first scrape, before any subsystem exists.
	gauge(buildInfo, 1, c.rec.version, c.rec.instance)
	if inv := c.rec.host.Load(); inv != nil {
		gauge(hostInfoDesc, 1, inv.labels...)
	}
	counter(echoVerified, float64(c.rec.echoOK.Load()))
	counter(echoMismatch, float64(c.rec.echoBad.Load()))

	for _, l := range src.Lanes {
		s := l.Stats()
		counter(laneConns, float64(s.Conns), s.Name)
		gauge(laneActive, float64(s.Active), s.Name)
		counter(laneObjects, float64(s.Objects), s.Name)
		counter(laneBytes, float64(s.Bytes), s.Name)
		counter(laneErrors, float64(s.Errors), s.Name)
		counter(laneDropped, float64(s.Dropped), s.Name)
		counter(laneRejected, float64(s.Rejected), s.Name)
	}

	if src.Objects != nil {
		emitCache(gauge, counter, "object", src.Objects.Stats())
	}
	if src.Txs != nil {
		emitCache(gauge, counter, "tx", src.Txs.Stats())
	}

	if src.Seen != nil {
		s := src.Seen.Stats()
		gauge(registryEntries, float64(s.Entries))
		counter(registryDups, float64(s.Hits))
	}

	if src.Tx != nil {
		s := src.Tx.Stats()
		counter(submitTotal, float64(s.Accepted), "accepted")
		counter(submitTotal, float64(s.Rejected), "rejected")
		counter(submitTotal, float64(s.Failed), "failed")
		counter(pipeEnqueued, float64(s.Enqueued))
		counter(pipeBatches, float64(s.Batches))
		counter(pipeSeals, float64(s.SealSize), "size")
		counter(pipeSeals, float64(s.SealBytes), "bytes")
		counter(pipeSeals, float64(s.SealLinger), "linger")
		counter(pipeSeals, float64(s.SealDep), "dependency")
		counter(pipeRetried, float64(s.Retried))
		counter(pipeRetryOK, float64(s.RetryAccepted))
		counter(pipeUnattributed, float64(s.Unattributed))
		counter(pipeRateLimited, float64(s.RateLimited))
		gauge(pipeQueue, float64(s.Queue))
	}

	if src.Announce != nil {
		s := src.Announce.Stats()
		counter(announceTotal, float64(s.Subtrees), "subtree")
		counter(announceTotal, float64(s.Blocks), "block")
		counter(announceFailures, float64(s.Failures))
		gauge(announceBuffered, float64(s.Buffered))
		gauge(announceAwaitingPull, float64(s.AwaitingPull))
	}

	if src.Retrieval != nil {
		s := src.Retrieval.Stats()
		counter(retrievalServed, float64(s.Subtree), "subtree")
		counter(retrievalServed, float64(s.SubtreeData), "subtree_data")
		counter(retrievalServed, float64(s.Txs), "txs")
		counter(retrievalServed, float64(s.Block), "block")
		counter(retrievalMisses, float64(s.Miss))
		counter(retrievalErrors, float64(s.Errors))
		counter(retrievalUnserved, float64(s.UnservedChainSync), "chain_sync")
		counter(retrievalUnserved, float64(s.UnservedRoute-s.UnservedChainSync), "other")
	}

	if src.Reverse != nil {
		s := src.Reverse.Stats()
		counter(reversePublished, float64(s.SubtreesUp), "subtree")
		counter(reversePublished, float64(s.BlocksUp), "block")
		counter(reverseSkipped, float64(s.RemoteSkipped), "remote_origin")
		counter(reverseSkipped, float64(s.Skipped), "unavailable")
		counter(reverseSkipped, float64(s.ForeignSkipped), "foreign_origin")
		counter(reverseSkipped, float64(s.Gated), "not_ready")
		counter(reverseSkipped, float64(s.StandbyHeld), "standby")
		active := 0.0
		if s.Active {
			active = 1
		}
		gauge(submitterActive, active)
		counter(reverseFailures, float64(s.Failures))
		counter(reverseReconnects, float64(s.Reconnects))
	}

	for _, u := range src.UpTunnels {
		if u == nil {
			continue
		}
		s := u.Stats()
		counter(upObjects, float64(s.Sent), s.Class)
		counter(upBytes, float64(s.Bytes), s.Class)
		counter(upFailures, float64(s.Failures), s.Class)
		counter(upRedials, float64(s.Redials), s.Class)
	}
}

// hostLabels is the label tuple for the host_info series, resolved once when
// the inventory is gathered rather than on every scrape.
type hostLabels struct{ labels []string }

func newHostLabels(inv hostinfo.Inventory, nic, speed string) *hostLabels {
	return &hostLabels{labels: []string{
		inv.Hostname, inv.KernelVersion,
		itoa(inv.CPULogical), utoa(inv.MemTotalBytes),
		nic, speed, inv.Version,
	}}
}

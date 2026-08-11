package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
)

// Descriptors. Counters are `_total`; anything that can go down is a gauge.
//
// The `lane` label matches the stack-wide convention (tx|subtree|block), so a
// bridge's series line up with the delivery-side series for the same lane.
var (
	laneConns = prometheus.NewDesc("btb_lane_connections_total",
		"Connections accepted on a delivery lane.", []string{"lane"}, nil)
	laneObjects = prometheus.NewDesc("btb_lane_objects_total",
		"Whole objects read off a delivery lane.", []string{"lane"}, nil)
	laneBytes = prometheus.NewDesc("btb_lane_bytes_total",
		"Object bytes read off a delivery lane.", []string{"lane"}, nil)
	laneErrors = prometheus.NewDesc("btb_lane_handler_errors_total",
		"Objects whose handler failed. The object is lost; the connection is kept.", []string{"lane"}, nil)
	laneDropped = prometheus.NewDesc("btb_lane_connections_dropped_total",
		"Connections dropped on a framing fault. Bare streams have no resync point, so every byte after a codec fault is suspect and the connection must go.", []string{"lane"}, nil)

	cacheEntries = prometheus.NewDesc("btb_cache_entries",
		"Objects currently held.", []string{"kind"}, nil)
	cacheBytes = prometheus.NewDesc("btb_cache_bytes",
		"Bytes currently held.", []string{"kind"}, nil)
	cacheStored = prometheus.NewDesc("btb_cache_stored_total",
		"Objects admitted to the cache.", []string{"kind"}, nil)
	cacheHits = prometheus.NewDesc("btb_cache_hits_total",
		"Lookups served from the cache.", []string{"kind"}, nil)
	cacheMisses = prometheus.NewDesc("btb_cache_misses_total",
		"Lookups for an object not held (absent or expired).", []string{"kind"}, nil)
	cacheEvicted = prometheus.NewDesc("btb_cache_evicted_total",
		"Objects evicted by the byte ceiling. Expected under load; only a problem when paired with retrieval misses.", []string{"kind"}, nil)
	cacheExpired = prometheus.NewDesc("btb_cache_expired_total",
		"Objects dropped on TTL expiry at lookup time.", []string{"kind"}, nil)

	registryEntries = prometheus.NewDesc("btb_registry_entries",
		"Hashes currently in the seen-registry.", nil, nil)
	registryDups = prometheus.NewDesc("btb_registry_duplicates_total",
		"Objects recognised as already seen — re-delivery after a failover or reconnect, or our own echo returning.", nil, nil)

	submitTotal = prometheus.NewDesc("btb_submit_total",
		"Transactions handed to the cluster's propagation service, by outcome. `rejected` is the cluster refusing on merits (after any retries); `failed` is transport or server fault.", []string{"outcome"}, nil)

	pipeBatches = prometheus.NewDesc("btb_txpipe_batches_total",
		"Batch submissions shipped to POST /txs.", nil, nil)
	pipeSeals = prometheus.NewDesc("btb_txpipe_batch_seals_total",
		"Why batches were sealed. `dependency` = a tx referenced a parent in the open batch (the /txs contract forbids parent+child in one request); `linger` = age; `size`/`bytes` = full.", []string{"reason"}, nil)
	pipeRetried = prometheus.NewDesc("btb_txpipe_retried_total",
		"Transactions re-submitted individually after a partial batch failure (missing-parent resolves once the parent lands).", nil, nil)
	pipeRetryOK = prometheus.NewDesc("btb_txpipe_retry_accepted_total",
		"Retried transactions the cluster then accepted.", nil, nil)
	pipeQueue = prometheus.NewDesc("btb_txpipe_queue_depth",
		"Transactions waiting in the pipe. At the ceiling, backpressure blocks the lane read loop.", nil, nil)

	announceTotal = prometheus.NewDesc("btb_announce_total",
		"Announcements produced to the cluster's Kafka, by object class.", []string{"class"}, nil)
	announceFailures = prometheus.NewDesc("btb_announce_failures_total",
		"Announcements that failed to produce. The object stays cached but the cluster never learns of it.", nil, nil)

	retrievalServed = prometheus.NewDesc("btb_retrieval_served_total",
		"Cluster pulls served from the cache, by route.", []string{"route"}, nil)
	retrievalMisses = prometheus.NewDesc("btb_retrieval_misses_total",
		"Pulls answered 404 because the object (or a subtree member) was not held. The cluster falls back to its ordinary peer-pull path.", nil, nil)
	retrievalErrors = prometheus.NewDesc("btb_retrieval_errors_total",
		"Pulls answered 5xx on a genuine bridge-side fault.", nil, nil)

	reversePublished = prometheus.NewDesc("btb_reverse_published_total",
		"Objects this cluster produced that were published back onto the object plane, by class.", []string{"class"}, nil)
	reverseSkipped = prometheus.NewDesc("btb_reverse_skipped_total",
		"Notifications not published. `remote_origin` is the origin filter working as intended (the object came from the fabric); `unavailable` is the asset service not holding it yet.", []string{"reason"}, nil)
	reverseFailures = prometheus.NewDesc("btb_reverse_failures_total",
		"Reverse-path failures: asset fetch, frame encode, or upward submit.", nil, nil)
	reverseReconnects = prometheus.NewDesc("btb_reverse_reconnects_total",
		"Blockchain notification stream reconnects. Routine — the cluster rolls its gRPC connections.", nil, nil)

	upObjects = prometheus.NewDesc("btb_uptunnel_objects_total",
		"Objects written to the object-plane ingress, by class.", []string{"class"}, nil)
	upBytes = prometheus.NewDesc("btb_uptunnel_bytes_total",
		"Bytes written to the object-plane ingress, by class.", []string{"class"}, nil)
	upFailures = prometheus.NewDesc("btb_uptunnel_failures_total",
		"Dial or write failures on the upward connection.", []string{"class"}, nil)
	upRedials = prometheus.NewDesc("btb_uptunnel_redials_total",
		"Dials of the upward connection. One at startup is normal; a climbing count tracks failures and means the ingress is flapping.", []string{"class"}, nil)
)

// collector reads the live subsystem snapshots at scrape time.
type collector struct{ rec *Recorder }

// emitCache writes one cache's series under the given kind label.
func emitCache(gauge, counter func(*prometheus.Desc, float64, ...string), kind string, s cache.Stats) {
	gauge(cacheEntries, float64(s.Entries), kind)
	gauge(cacheBytes, float64(s.Bytes), kind)
	counter(cacheStored, float64(s.Stored), kind)
	counter(cacheHits, float64(s.Hits), kind)
	counter(cacheMisses, float64(s.Misses), kind)
	counter(cacheEvicted, float64(s.Evicted), kind)
	counter(cacheExpired, float64(s.Expired), kind)
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		laneConns, laneObjects, laneBytes, laneErrors, laneDropped,
		cacheEntries, cacheBytes, cacheStored, cacheHits, cacheMisses, cacheEvicted, cacheExpired,
		registryEntries, registryDups, submitTotal, announceTotal, announceFailures,
		retrievalServed, retrievalMisses, retrievalErrors,
		reversePublished, reverseSkipped, reverseFailures, reverseReconnects,
		upObjects, upBytes, upFailures, upRedials,
	} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	src := c.rec.src.Load()
	if src == nil {
		return
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	for _, l := range src.Lanes {
		s := l.Stats()
		counter(laneConns, float64(s.Conns), s.Name)
		counter(laneObjects, float64(s.Objects), s.Name)
		counter(laneBytes, float64(s.Bytes), s.Name)
		counter(laneErrors, float64(s.Errors), s.Name)
		counter(laneDropped, float64(s.Dropped), s.Name)
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
		counter(pipeBatches, float64(s.Batches))
		counter(pipeSeals, float64(s.SealSize), "size")
		counter(pipeSeals, float64(s.SealBytes), "bytes")
		counter(pipeSeals, float64(s.SealLinger), "linger")
		counter(pipeSeals, float64(s.SealDep), "dependency")
		counter(pipeRetried, float64(s.Retried))
		counter(pipeRetryOK, float64(s.RetryAccepted))
		gauge(pipeQueue, float64(s.Queue))
	}

	if src.Announce != nil {
		s := src.Announce.Stats()
		counter(announceTotal, float64(s.Subtrees), "subtree")
		counter(announceTotal, float64(s.Blocks), "block")
		counter(announceFailures, float64(s.Failures))
	}

	if src.Retrieval != nil {
		s := src.Retrieval.Stats()
		counter(retrievalServed, float64(s.Subtree), "subtree")
		counter(retrievalServed, float64(s.SubtreeData), "subtree_data")
		counter(retrievalServed, float64(s.Txs), "txs")
		counter(retrievalServed, float64(s.Block), "block")
		counter(retrievalMisses, float64(s.Miss))
		counter(retrievalErrors, float64(s.Errors))
	}

	if src.Reverse != nil {
		s := src.Reverse.Stats()
		counter(reversePublished, float64(s.SubtreesUp), "subtree")
		counter(reversePublished, float64(s.BlocksUp), "block")
		counter(reverseSkipped, float64(s.RemoteSkipped), "remote_origin")
		counter(reverseSkipped, float64(s.Skipped), "unavailable")
		counter(reverseSkipped, float64(s.ForeignSkipped), "foreign_origin")
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

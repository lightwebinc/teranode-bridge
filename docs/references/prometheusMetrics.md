# teranode-bridge Prometheus Metrics Reference

Every series is `teranode_bridge_*`, produced by the `Namespace: "teranode"` /
`Subsystem: "bridge"` pair that names every Teranode metric — so a bridge lands
on the same grid as the cluster it fronts and is reachable from the same
dashboards, recording rules and relabel configs. The format of this document
mirrors [Teranode's own metrics reference](https://github.com/bsv-blockchain/teranode/blob/main/docs/references/prometheusMetrics.md).

Latency and size histograms use Teranode's bucket sets **verbatim** (`teranode/util/metrics.go`),
so `histogram_quantile` over a bridge series and a cluster series means the same
thing and the two can share a panel.

## Legacy prefix

Before this release the bridge used a `btb_` prefix of its own. Every series
that existed under that name is **dual-emitted** while `-metrics-legacy-prefix`
is true (the default) so existing dashboards survive the cutover. Metrics
introduced with the new naming are never aliased. Set
`-metrics-legacy-prefix=false` once dashboards are migrated; the alias is
scheduled for removal in the release after next.

## Metric types

| Type | Description |
|------|-------------|
| Counter | Monotonic; only increases or resets to zero on restart. Always suffixed `_total`. |
| Gauge | A value that can go up and down: a level, a timestamp, or a state. |
| Histogram | Bucketed observations of a duration or a size, plus `_sum` and `_count`. |

## Endpoints

| Path | Purpose |
|------|---------|
| `GET /metrics` | Prometheus exposition |
| `GET /health` | Dependency report, same body shape as Teranode |
| `GET /health/readiness` | As `/health`; probes dependencies |
| `GET /health/liveness` | Process liveness only — dependencies are **not** probed |
| `GET /healthz` | Bare liveness, plain text |
| `GET /readyz` | Narrow readiness: every delivery lane is bound. **The failover contract** — a standby polls the primary's `/readyz` |
| `POST /loglevel` | Runtime log-level change, no restart |
| `/debug/pprof/…` | `index`, `cmdline`, `profile`, `symbol`, `trace`, plus the named runtime profiles |

All health routes accept `?timeout=<duration>` to override the 5s probe
deadline, as Teranode's do.

## Identity

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_build_info` | Gauge | Build identity, value always 1. Labels: `version`, `instance`. |
| `teranode_bridge_host_info` | Gauge | Static host facts, value always 1. Labels: `hostname`, `kernel_version`, `cpu_logical`, `mem_bytes`, `nic`, `speed_mbps`, `version`. Joins the `host.inventory` log event. |

## Delivery lanes (ingest)

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_lane_connections_total` | Counter | Connections accepted, by `lane`. |
| `teranode_bridge_lane_connections_active` | Gauge | Connections open right now, by `lane`. Zero on a lane that should be fed is a whole-edge outage — a condition no counter can show. |
| `teranode_bridge_lane_objects_total` | Counter | Whole objects read, by `lane`. |
| `teranode_bridge_lane_bytes_total` | Counter | Object bytes read, by `lane`. |
| `teranode_bridge_lane_handler_errors_total` | Counter | Objects whose handler failed, by `lane`. The object is lost; the connection is kept. |
| `teranode_bridge_lane_connections_dropped_total` | Counter | Connections dropped on a framing fault, by `lane`. |
| `teranode_bridge_lane_objects_rejected_total` | Counter | Well-framed objects refused on lane format policy, by `lane` — on `tx`, a transaction that is not BRC-30 extended format. A sender bug, not a cluster fault. |
| `teranode_bridge_object_bytes` | Histogram | Object size distribution, by `lane`. Buckets: `MetricsBucketsSize`. On for `subtree` and `block`; on `tx` only with `-tx-size-histogram`. |

## Cache

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_cache_entries` | Gauge | Objects held, by `kind` (`object`, `tx`). |
| `teranode_bridge_cache_bytes` | Gauge | Bytes held, by `kind`. |
| `teranode_bridge_cache_stored_total` | Counter | Objects admitted, by `kind`. |
| `teranode_bridge_cache_hits_total` | Counter | Lookups served, by `kind`. |
| `teranode_bridge_cache_misses_total` | Counter | Lookups for an object not held, by `kind`. |
| `teranode_bridge_cache_evicted_total` | Counter | Objects evicted by the byte ceiling, by `kind`. Expected under load; a problem only when paired with retrieval misses. |
| `teranode_bridge_cache_expired_total` | Counter | Objects dropped on TTL expiry at lookup, by `kind`. |
| `teranode_bridge_registry_entries` | Gauge | Hashes in the seen-registry. |
| `teranode_bridge_registry_duplicates_total` | Counter | Objects recognised as already seen. |

## Transaction pipeline

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_submit_total` | Counter | Transactions handed to propagation, by `outcome` (`accepted`, `rejected`, `failed`). |
| `teranode_bridge_submit_duration_seconds` | Histogram | One `POST /txs` round trip, by `outcome` (`ok`, `rejected`, `failed`, `rate_limited`). Buckets: `MetricsBucketsMilliSeconds`. Separating outcomes is what distinguishes an endpoint that is slow from one refusing quickly. |
| `teranode_bridge_txpipe_enqueued_total` | Counter | Transactions accepted into the pipe. |
| `teranode_bridge_txpipe_batches_total` | Counter | Batch submissions shipped. |
| `teranode_bridge_txpipe_batch_seals_total` | Counter | Why batches sealed, by `reason` (`size`, `bytes`, `linger`, `dependency`). |
| `teranode_bridge_txpipe_retried_total` | Counter | Transactions re-submitted individually after a partial batch failure. |
| `teranode_bridge_txpipe_retry_accepted_total` | Counter | Retried transactions the cluster then accepted. |
| `teranode_bridge_txpipe_unattributed_total` | Counter | Error lines naming no batch member. Those transactions' outcome is **unknown** and excluded from `accepted`. |
| `teranode_bridge_txpipe_rate_limited_total` | Counter | Batches refused by an endpoint's HTTP limiter (429). |
| `teranode_bridge_txpipe_queue_depth` | Gauge | Transactions waiting in the pipe. At the ceiling, backpressure blocks the lane read loop. |

## Announcements

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_announce_total` | Counter | Announcements produced, by `class` (`subtree`, `block`). |
| `teranode_bridge_announce_failures_total` | Counter | Announcements that failed to produce. The object stays cached but the cluster never learns of it. |
| `teranode_bridge_announce_duration_seconds` | Histogram | Produce-to-ack latency, by `class`. Buckets: `MetricsBucketsMilliSeconds`. |
| `teranode_bridge_announce_to_first_pull_seconds` | Histogram | **The bridge's own SLI**: from announcement ack to the cluster's first pull of that object, by `class`. Buckets: `MetricsBucketsSeconds`. Nothing on either side of the bridge measures this — the cluster does not know when it was told, the fabric does not know when the cluster acted. A rising p99 means the cache TTL will start evicting objects before they are fetched. |
| `teranode_bridge_announce_awaiting_pull` | Gauge | Announcements acked but not yet pulled. A level that keeps climbing means announcements land and pulls do not follow. |
| `teranode_bridge_kafka_producer_buffered_records` | Gauge | Records the Kafka client still holds unproduced. A sustained non-zero level is an announce **backlog**, which no failure counter reports. |

## Kafka producer

Hook-for-hook with `teranode_kafka_producer_*` (`teranode/util/kafka`), since
both sides use franz-go. The bridge namespace differs so a bridge and its
cluster stay distinguishable on one dashboard.

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_kafka_producer_bytes_written_total` | Counter | Bytes written to brokers. |
| `teranode_bridge_kafka_producer_write_duration_seconds` | Histogram | Time in `conn.Write` for a produce request. |
| `teranode_bridge_kafka_producer_write_errors_total` | Counter | Broker write errors during produce. |
| `teranode_bridge_kafka_producer_e2e_duration_seconds` | Histogram | Write of a produce request to read of its response. |
| `teranode_bridge_kafka_producer_produce_request_latency_seconds` | Histogram | As above, plus the wait before the write. |
| `teranode_bridge_kafka_producer_batch_records_total` | Counter | Records successfully produced in batches. |
| `teranode_bridge_kafka_producer_batch_compressed_bytes_total` | Counter | Compressed bytes of produced batches. |
| `teranode_bridge_kafka_producer_broker_connects_total` | Counter | Broker connections opened. |
| `teranode_bridge_kafka_producer_broker_disconnects_total` | Counter | Broker connections closed. A climbing rate against a flat produce rate means the brokers are cycling us. |
| `teranode_bridge_kafka_producer_connect_errors_total` | Counter | Failed broker dials. |

## Retrieval plane

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_retrieval_served_total` | Counter | Pulls served from cache, by `route` (`subtree`, `subtree_data`, `txs`, `block`). |
| `teranode_bridge_retrieval_duration_seconds` | Histogram | Time to serve one pull, by `route`. Buckets: `MetricsBucketsMilliSeconds`. This sits on the cluster's block-validation critical path — a slow pull here is indistinguishable, cluster-side, from a slow peer. |
| `teranode_bridge_retrieval_misses_total` | Counter | Pulls answered 404. The cluster falls back to its ordinary peer-pull path. |
| `teranode_bridge_retrieval_errors_total` | Counter | Pulls answered 5xx on a genuine bridge-side fault. |
| `teranode_bridge_retrieval_unserved_route_total` | Counter | Requests for unserved routes, by `class`. `chain_sync` is the **catchup-divert canary** — see the alerting section. |

## Reverse path (cluster → object plane)

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_reverse_published_total` | Counter | Cluster-produced objects published upward, by `class`. |
| `teranode_bridge_reverse_skipped_total` | Counter | Notifications not published, by `reason` (`remote_origin`, `unavailable`, `foreign_origin`, `not_ready`, `standby`). |
| `teranode_bridge_reverse_failures_total` | Counter | Asset fetch, frame encode or upward submit failures. |
| `teranode_bridge_reverse_reconnects_total` | Counter | Notification-stream reconnects. Routine — the cluster rolls its gRPC connections — and also how a keepalive-detected dead path surfaces. A step change with no cluster restart points at the network path, not the cluster. |
| `teranode_bridge_submitter_active` | Gauge | 1 while this bridge holds the submitter role, 0 on a standby. **Must sum to exactly 1 per cluster per class.** |
| `teranode_bridge_asset_fetch_duration_seconds` | Histogram | Time to build an object from the cluster's asset service, by `class`. Buckets: `MetricsBucketsMilliLongSeconds`. |
| `teranode_bridge_uptunnel_objects_total` | Counter | Objects written to the object-plane ingress, by `class`. |
| `teranode_bridge_uptunnel_bytes_total` | Counter | Bytes written, by `class`. |
| `teranode_bridge_uptunnel_failures_total` | Counter | Dial or write failures, by `class`. |
| `teranode_bridge_uptunnel_redials_total` | Counter | Dials of the upward connection, by `class`. One at startup is normal; a climbing count means the ingress is flapping. |
| `teranode_bridge_uptunnel_write_duration_seconds` | Histogram | Time to write one object upward, by `class`. Includes time queued behind another object on the same connection. |

## Cluster state

Read over the blockchain gRPC connection the reverse path already holds, every
`-cluster-poll` (default 15s). Advisory only: the bridge announces and submits
identically whatever the cluster's state, because the cluster is unmodified and
decides for itself.

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_cluster_fsm_state` | Gauge | 0 IDLE, 1 RUNNING, 2 CATCHINGBLOCKS, -1 unknown/unreachable. |
| `teranode_bridge_cluster_fsm_state_info` | Gauge | 1 for the current state, 0 for every other, by `state`. |
| `teranode_bridge_cluster_block_height` | Gauge | Height of the cluster's best block header. |
| `teranode_bridge_cluster_probe_errors_total` | Counter | Failed cluster-state reads, by `call`. |

## Freshness

Every counter in the bridge is monotonic, so a stalled lane, a dead subscription
and a quiet network all render as the same flat line. These are unix timestamps,
alertable as `time() - metric > N` without knowing the traffic baseline.

They are **seeded with the process start time** at startup. Presence matters —
an absent series matches no alert expression, which is exactly the silent failure
these exist to catch — but so does the seed value: `time() - 0` is fifty-odd
years, so seeding with zero would fire every staleness alert the instant the
process started. Seeded with the start time, the value is always an age, and a
bridge that never receives anything trips its alert exactly N seconds after boot.

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_last_object_timestamp_seconds` | Gauge | Last whole object read, by `lane`. |
| `teranode_bridge_last_announce_timestamp_seconds` | Gauge | Last successful announcement, by `class`. |
| `teranode_bridge_last_pull_timestamp_seconds` | Gauge | Last cluster pull served. |
| `teranode_bridge_last_submit_timestamp_seconds` | Gauge | Last accepted batch submission. |
| `teranode_bridge_last_notification_timestamp_seconds` | Gauge | Last blockchain notification received. Stamped for **every** notification type including PINGs, so a live subscription on an idle chain still ticks — which is what makes "the subscription is dead" separable from "the chain is quiet". |

## Data integrity

| Metric Name | Type | Description |
|-------------|------|-------------|
| `teranode_bridge_echo_verified_total` | Counter | Objects this bridge published that returned on its own delivery lanes byte-identical. |
| `teranode_bridge_echo_mismatch_total` | Counter | Objects that returned with **different** bytes than were published. An object-plane data-integrity fault; never expected to be non-zero. |

## gRPC client

Standard `grpc_client_*` series from `github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus`,
the same provider Teranode uses for its clients, covering the blockchain
connection.

## Dashboard

`deploy/grafana/teranode-bridge-dashboard.json` imports as uid `teranode-bridge`
and templates `$job`/`$instance` off `teranode_bridge_build_info`. Scope it to
one cluster's bridges — the submitter tile sums an invariant that is per-cluster.

## Alerting

The expressions worth running. They ship as a `PrometheusRule` in the Helm
chart (`monitoring.prometheusRule.enabled`).

| Alert | Expression | Why |
|-------|-----------|-----|
| `BridgeEchoMismatch` | `increase(teranode_bridge_echo_mismatch_total[10m]) > 0` | The object plane is corrupting data. Never expected to fire. |
| `BridgeChainSyncDivertLost` | `rate(teranode_bridge_retrieval_unserved_route_total{class="chain_sync"}[15m]) > 0` sustained 30m | The cluster is selecting the bridge as a catchup source. Rare bursts are the known cached-alternatives path; a sustained rate means the synthetic peer-id divert has stopped working and the next node to fall behind may wedge. |
| `BridgeNoSubmitter` | `sum by (cluster) (teranode_bridge_submitter_active) == 0` | Nothing is publishing cluster output upward. |
| `BridgeDoubleSubmitter` | `sum by (cluster) (teranode_bridge_submitter_active) > 1` | Two bridges publishing. Dedup absorbs it, but it means the failover logic is confused. |
| `BridgeLaneStalled` | `time() - teranode_bridge_last_object_timestamp_seconds > 900` | No delivery on a lane. Catches whole-edge death, which the counters cannot. |
| `BridgeSubscriptionDead` | `time() - teranode_bridge_last_notification_timestamp_seconds > 300` | The blockchain notification stream is silent, PINGs included. Client keepalive should now turn a dead path into a reconnect within ~50s, so this firing means something keepalive cannot fix — the cluster stopped sending, or the connection is up but the stream is not. |
| `BridgeAnnounceBacklog` | `teranode_bridge_kafka_producer_buffered_records > 0` for 5m | Announcements are queuing, not failing. |
| `BridgeAwaitingPullClimbing` | `teranode_bridge_announce_awaiting_pull > 100` for 15m | The cluster is not acting on announcements. |
| `BridgeUnattributedSubmits` | `rate(teranode_bridge_txpipe_unattributed_total[15m]) > 0` | Delivery counts are incomplete, not merely bad. |
| `BridgeSlowRetrieval` | `histogram_quantile(0.99, sum by (le,route) (rate(teranode_bridge_retrieval_duration_seconds_bucket[5m]))) > 1` | The cluster is waiting on the bridge inside block validation. |
| `BridgeClusterNotRunning` | `teranode_bridge_cluster_fsm_state_info{state="RUNNING"} == 0` for 30m | Explains "announces succeed, nothing happens". |

## Tracing

Off by default (`-tracing-enabled`), matching upstream. When on, the bridge
exports OTLP/HTTP with `ParentBased(TraceIDRatioBased)` sampling, so a sampled
cluster trace stays sampled across the bridge. Instrumented boundaries:

- outbound HTTP to propagation and to the asset service (`otelhttp`)
- outbound gRPC to blockchain (`otelgrpc`)
- inbound HTTP on the retrieval plane (`otelhttp`) — this is what keeps "why was
  this block slow to validate" from dead-ending at the bridge

**Known gap, shared with upstream:** announcements ride Kafka, and Teranode does
not propagate trace context over Kafka either (see its
`util/tracing/PROPAGATION.md`). The bridge does not invent a scheme of its own;
when upstream adds a `trace_context` field, the bridge must carry it too.

Even with tracing disabled the W3C propagator is installed, so incoming trace
context is parsed and forwarded rather than dropped.

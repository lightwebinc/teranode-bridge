# Configuration

`teranode-bridge` is configured entirely by **command-line flags**. There are no
environment-variable fallbacks and no configuration file: the process is meant
to be driven by a unit file, a container command, or a chart's `args`.

```bash
teranode-bridge -help
```

Addresses in the examples below use the documentation ranges
([RFC 5737](https://www.rfc-editor.org/rfc/rfc5737) `192.0.2.0/24`,
[RFC 3849](https://www.rfc-editor.org/rfc/rfc3849) `2001:db8::/32`). Substitute
your own.

## Delivery lanes (ingest)

One TCP listener per object class. The streams are bare — no length prefix, no
type tag — so each lane carries exactly one class and objects are delimited by
their own structure.

| Flag              | Default     | Description                                                                                                                                                |
| ----------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-tx-listen`      | `[::]:8833` | Transaction lane (BRC-30 extended format only — a BRC-12 standard transaction is refused at the lane and counted in `btb_lane_objects_rejected_total{lane="tx"}`, never submitted). |
| `-subtree-listen` | `[::]:9143` | Subtree lane (BRC-143 push frames).                                                                                                                        |
| `-block-listen`   | `[::]:9144` | Block lane (BRC-144 push frames).                                                                                                                          |
| `-max-object`     | `0`         | Per-object size ceiling in bytes. `0` uses the `objfmt` codec default of 64 MiB. Applies to every lane.                                                    |

Bind these to the interface the delivery side reaches the bridge on — typically
the link-facing address, not `[::]`, once the deployment is settled.

## Retrieval plane

Answers the pulls the cluster makes after an announcement.

| Flag                | Default     | Description                                                                                                                                                                                                          |
| ------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-retrieval-listen` | `[::]:9145` | Bind address for the retrieval HTTP server. Must be reachable from the cluster's subtree- and block-validation services.                                                                                             |
| `-advertise`        | —           | **Required unless `-mode sink`.** The base URL the cluster should dial for pulls, e.g. `http://[2001:db8:3f::1]:9145`. This value, plus `-api-prefix`, is what is stamped into every announcement.                   |
| `-api-prefix`       | `/api/v1`   | Path prefix the retrieval plane serves on. Must match what the cluster expects: it concatenates the announced base URL with `/subtree/{hash}` verbatim, and its own asset service mounts everything under `/api/v1`. |

The announced URL is `strings.TrimRight(advertise, "/") + apiPrefix`. Do **not**
put the prefix in `-advertise` as well — the result would be a doubled path.

`-advertise` must be an address the _cluster_ can dial, which is not necessarily
the address the bridge binds. Binding `[::]:9145` while advertising a specific
routable address is the normal arrangement.

## Cluster ingest targets

| Flag           | Default | Description                                                                                                                                                                              |
| -------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-propagation` | —       | **Required unless `-mode sink`.** Propagation HTTP base URL(s), e.g. `http://192.0.2.10:20833`. Repeatable and/or comma-separated; multiple endpoints are round-robined per transaction. |
| `-kafka`       | —       | **Required unless `-mode sink`.** Cluster Kafka broker(s), e.g. `192.0.2.10:19092`. Repeatable and/or comma-separated.                                                                   |

Both flags accumulate: `-propagation a,b` and `-propagation a -propagation b`
are equivalent.

At startup the bridge probes `GET {propagation}/health` and pings the Kafka
brokers. **Neither failure is fatal** — the cluster may still be starting — but
both are logged at warn level, and a persistent failure shows up as `failed`
submits or `announce failures` in the stats.

The Kafka brokers must advertise a listener that resolves _from the bridge_. A
mis-advertised listener passes the ping and then fails at produce time.

## Announcements

| Flag             | Default              | Description                                                                                  |
| ---------------- | -------------------- | -------------------------------------------------------------------------------------------- |
| `-subtree-topic` | `subtrees-teranode1` | Kafka topic the cluster's subtree validation consumes.                                       |
| `-block-topic`   | `blocks-teranode1`   | Kafka topic the cluster's block validation consumes.                                         |
| `-peer-id`       | `""`                 | Peer identity stamped on announcements. **Set a synthetic id** (see below); empty is unsafe. |

Both topic names must match the cluster's own configuration; the defaults match
Teranode's single-node defaults. An announcement produced to a topic nobody
consumes is silently ineffective — the object is never pulled, never validated,
and only shows up as a growing gap between `announce stats` and
`retrieval stats`.

> **`-peer-id` must be a SYNTHETIC libp2p id** — a valid-format `12D3KooW…`
> identity derived from a fresh ed25519 key and registered with no p2p service.
> This inverts earlier guidance, which said to leave it empty; that guidance was
> wrong at teranode `1cca625` and caused a verified production-shaped wedge.
>
> Why a synthetic id works (both halves verified in the upstream source and by
> lab drills): the cluster's **catchup** gate treats an _unregistered_ id as
> unhealthy and diverts chain sync to real libp2p peers, while every
> **delivery** gate checks only bans and keeps pulling objects from the bridge.
> The bridge therefore augments the p2p network without ever being asked for the
> chain itself.
>
> Why empty is unsafe: catchup substitutes the announce **URL** for a missing
> id, so the cluster targets the bridge's retrieval plane for
> `/headers_from_common_ancestor`, gets 404s, opens a circuit breaker on that
> URL, and locks itself out of recovery. Observed live: a node wedged 300+
> blocks behind with a healthy peer one hop away.
>
> Derivation (any fresh ed25519 key; never reuse a real peer's id): ed25519
> public key → protobuf `08 01 12 20 ‖ pub` → identity multihash `00 24 ‖ …` →
> base58btc. Give every bridge instance its own id.
>
> The invariant is monitored, not assumed:
> `btb_retrieval_unserved_route_total{class="chain_sync"}` counts the cluster
> asking the bridge for chain-sync routes. Rare bursts accompany multi-peer
> degradation (upstream's cached-alternatives walk skips the divert gate); a
> sustained rate means the divert has regressed — alarm on it.
>
> Boundary worth knowing: the bridge is an **object source, never a sync
> source**. A cluster that falls further behind than its announce backlog can
> only recover through a real libp2p peer; deployments need at least one.

## Reverse path (cluster → object plane)

Disabled unless `-blockchain` is set. Ignored entirely in `-mode sink`.

| Flag                 | Default | Description                                                                                                                                            |
| -------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `-blockchain`        | —       | Cluster blockchain gRPC `host:port`, e.g. `192.0.2.10:20087`. Setting it enables the reverse path and **requires** `-local-asset` and `-edge-ingress`. |
| `-local-asset`       | —       | Cluster asset base URL **including the API prefix**, e.g. `http://192.0.2.10:20090/api/v1`. Used to fetch objects the cluster produced.                |
| `-edge-ingress`      | —       | Host of the object-plane ingress for upward submits, e.g. `2001:db8:1::1`. Typically reachable only over the delivery link.                            |
| `-edge-subtree-port` | `8726`  | Ingress port for BRC-143 subtree submits.                                                                                                              |
| `-edge-block-port`   | `8727`  | Ingress port for BRC-144 block submits.                                                                                                                |
| `-submitter`         | `true`  | Hold the submitter role on this bridge. Exactly one bridge per cluster should hold it for a given class.                                               |

The gRPC connection to the blockchain service is **plaintext and
unauthenticated**. Keep it on a trusted LAN.

With `-submitter=false` the subscriber still connects and still runs its origin
filter (so `remote_skipped` keeps counting), but it publishes nothing — held
publishes count in `btb_reverse_skipped_total{reason="standby"}`. That makes a
standby bridge a hot spare.

### Health-gated failover (`-submitter-probe`)

| Flag               | Default | Description                                                                                                                    |
| ------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `-submitter-probe` | `""`    | The PRIMARY bridge's `/readyz` URL. Set on a **standby** (`-submitter=false`); ignored with a warning on a configured primary. |

The standby probes the primary every 2 s. Five consecutive failures **promote**
it (it starts publishing, `btb_submitter_active` flips to 1, log line
`PROMOTED to submitter`); ten consecutive successes demote it again. Demotion is
deliberately slower than promotion so a flapping primary cannot flap the role,
and the failure envelope is benign in both directions: a dead primary costs ~10
s of publish gap (the failure model's existing "gap until promoted", now seconds
instead of a pager), and a false promotion costs a window of dual-publish that
every receiver's hash dedup absorbs. The standby's subscription, seen-registry
and origin filter run the whole time, so promotion is a flag flip, not a cold
start.

## Transaction pipeline

The tx lane never submits synchronously: objects are enqueued to a batching
pipeline that ships them on propagation's `POST /txs` endpoint (effective caps:
1023 txs / 32 MiB per request). The lane read loop's only costs are parse, hash,
one copy, and an enqueue — measured at >1.4M tx/s sustained on a 2×18-core host
against a mock sink, with backpressure (a full queue blocks the lane, which
closes the TCP window) bounding memory when the cluster is slower than the
fabric.

| Flag              | Default   | Description                                                                        |
| ----------------- | --------- | ---------------------------------------------------------------------------------- |
| `-tx-batch`       | `512`     | Transactions per batch (clamped to 1023 — see note).                               |
| `-tx-batch-bytes` | `8388608` | Batch body ceiling (capped under the server's 32 MiB).                             |
| `-tx-linger`      | `2ms`     | Max age of a non-full batch — bounds added latency when quiet.                     |
| `-tx-inflight`    | `4`       | Concurrent batch requests. Throughput ≈ inflight × batch / RTT.                    |
| `-tx-builders`    | `4`       | Parallel batch builders (power of two, ≤16); txs route to builders by txid.        |
| `-tx-retries`     | `3`       | Per-tx retries for failures that resolve with time (missing parent). `0` disables. |

Three contract details worth knowing:

- **The batch cap is 1023, not 1024**: propagation checks
  `totalNrTransactions >= maxTransactionsPerRequest` at the _top_ of its read
  loop, so a request holding exactly 1024 transactions is read, dispatched and
  fully processed — and only then answered `400`. Batching at the advertised
  limit therefore delivers every member _and_ reports the batch as failed, which
  would resubmit all 1024 as duplicates. `-tx-batch` is clamped to 1023 for that
  reason; raising it above that has no effect.
- **Parent/child**: propagation processes a batch concurrently, and its handler
  documents that a request must not contain both a parent and its child. The
  pipe walks each transaction's input outpoints and **seals** the open batch
  when a dependency lands in it, so same-builder chains never violate the
  contract; cross-builder and cross-connection races fall to the bounded retry
  (`422` = missing parent, the only use of that status).
- **Partial failure**: a batch answering `500 Failed to process transactions`
  had its other members processed; the failed txids are parsed from the error
  lines and retried together as one further batch (bounded by `-tx-inflight`).
  Attribution reads only the txid the error
  convention _brackets_ — messages such as `duplicate input found: <hash>:<n>`
  quote a second hash that is not the subject, and counting it would book a
  phantom failure against a batch member that in fact succeeded. Any error line
  naming no member of the batch it answers is counted in
  `btb_txpipe_unattributed_total` and excluded from `accepted`, because that
  batch's outcome is partly unknown rather than good.
- **Rate limiting**: propagation's HTTP limiter is per source IP **and** per
  endpoint. A `429` means nothing in the batch was processed, so the pipe
  retries the batch _whole_ (never splitting it into per-member requests, which
  would multiply the request rate by the batch size against the limiter that
  just refused it) and counts `btb_txpipe_rate_limited_total`. A sustained
  non-zero rate means one bridge is saturating one endpoint — add endpoints or
  raise the server's limit; batch size will not help.

## Origin detection (`-mine-tag`)

| Flag        | Default | Description                                                    |
| ----------- | ------- | -------------------------------------------------------------- |
| `-mine-tag` | `""`    | This cluster's `coinbase_arbitrary_text` (e.g. `/teranode1/`). |

Blockchain notifications carry no origin, and a block this cluster learned over
libp2p **before** the fabric delivered it looks unseen — without a check, the
reverse path republishes a remote block upward with false attribution. When
`-mine-tag` is set, only blocks whose in-band coinbase carries the tag are
published; foreign blocks count in
`btb_reverse_skipped_total{reason="foreign_origin"}`. The check is stateless
(derived from block content), so it also survives bridge restarts, which wipe
the seen-registry.

This requires **no Teranode change**: `coinbase_arbitrary_text` is existing
per-node Teranode configuration — give each cluster a distinct tag and pass the
same value here. Subtrees need no equivalent: their notification has exactly one
producer, local block assembly.

## Cache and duplicate suppression

| Flag           | Default              | Description                                                                                                                                    |
| -------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `-cache-bytes` | `1073741824` (1 GiB) | Byte ceiling **per cache**. Two caches are created — objects (subtrees, blocks) and transactions — so the process ceiling is twice this value. |
| `-cache-ttl`   | `10m`                | How long a pushed object stays fetchable.                                                                                                      |

Size the TTL against validation lag, not against retention: this is a cache, not
a store of record. An entry evicted before the cluster pulls it produces a `404`
and the cluster falls back to its ordinary peer announce-and-pull path —
degraded latency, not lost data. Watch `cache stats evicted` and
`retrieval stats miss` together; a rising `miss` with a flat `evicted` points at
a topic or URL problem instead.

The tx cache is **generational** (two map generations, rotated by TTL/2 or byte
budget, oldest dropped wholesale): at megatransaction rates an LRU costs ~5× the
payload in heap and collapses into continuous GC. Entry lifetime is between
TTL/2 and TTL — a recency window, which is exactly the subtree_data fallback's
need. The subtree/block cache keeps precise LRU+TTL semantics.

The seen-registry (duplicate suppression and the reverse path's origin filter)
is generational the same way: 30-minute nominal TTL, 2²⁰ entries; not currently
configurable.

## Process

| Flag            | Default     | Description                                                                                              |
| --------------- | ----------- | -------------------------------------------------------------------------------------------------------- |
| `-mode`         | `all`       | `all` = full bridge. `sink` = receive, parse, verify and count only, with no cluster targets.            |
| `-stats-every`  | `1m`        | Interval between stats blocks. `0` disables periodic stats (a final block is still emitted at shutdown). |
| `-metrics-addr` | `[::]:9146` | HTTP listener for `/metrics`, `/healthz`, `/readyz`, `/loglevel`. Empty disables it.                     |
| `-log-level`    | `info`      | `debug` \| `info` \| `warn` \| `error`. Runtime-changeable via `POST /loglevel` and `SIGHUP`.            |
| `-log-format`   | `text`      | `text` (stderr) or `json` (stdout — the fleet aggregation contract).                                     |
| `-instance-id`  | hostname    | `service.instance.id` shared by logs and metrics, so the two join.                                       |
| `-debug`        | `false`     | Deprecated alias for `-log-level=debug`.                                                                 |

> **Why `text` is the default here** and `json` elsewhere in the stack: the
> integration demo parses this binary's log lines with anchored regexes. Set
> `-log-format json` for any deployment that ships to log aggregation.

### Observability endpoints

| Route            | Answer                                       |
| ---------------- | -------------------------------------------- |
| `GET /metrics`   | Prometheus exposition, `btb_` prefix         |
| `GET /healthz`   | always `200`                                 |
| `GET /readyz`    | `200` once every lane is bound, `503` before |
| `POST /loglevel` | runtime log-level change                     |

Metrics are read from the same counters the stats lines report, so the two never
disagree. The one to alert on is **`btb_echo_mismatch_total`** — non-zero means
the object plane returned different bytes than were published. Series are
present at zero from startup, so `== 0` is a valid alert expression immediately
after a restart.

`-mode sink` skips the propagation submitter, the Kafka producer, the retrieval
plane and the reverse path. Lanes still run, still enforce framing, and still
populate the caches. Use it to burn in a delivery slot before a cluster exists,
or to separate object-plane faults from cluster-side ones.

## Required-flag matrix

| Flag                            | `-mode all`                          | `-mode sink` |
| ------------------------------- | ------------------------------------ | ------------ |
| `-propagation`                  | required                             | ignored      |
| `-kafka`                        | required                             | ignored      |
| `-advertise`                    | required                             | ignored      |
| `-blockchain`                   | optional (enables reverse path)      | ignored      |
| `-local-asset`, `-edge-ingress` | required **if** `-blockchain` is set | ignored      |

Missing a required flag exits `2` before any listener opens. A construction
failure (bad propagation config, unreachable Kafka client setup, bad blockchain
address) exits `1`.

## Cluster-side prerequisites

The bridge needs four things reachable on the cluster's LAN. Default ports for a
standard Teranode deployment:

| Service                   | Default                    | Used for                        |
| ------------------------- | -------------------------- | ------------------------------- |
| Propagation HTTP          | `:20833`                   | `POST /tx`, `GET /health`       |
| Kafka (external listener) | `:19092`                   | subtree and block announcements |
| Asset HTTP                | `:20090` (under `/api/v1`) | reverse path fetches            |
| Blockchain gRPC           | `:20087`                   | reverse path `Subscribe`        |

And the cluster must be able to reach the bridge's `-advertise` URL. Nothing on
the cluster is modified: no patched ingest, no new RPC, no changed validation.

## Examples

### Full bridge, delivery only

Terminates all three lanes, submits transactions, announces subtrees and blocks,
and serves the pulls. No reverse path.

```bash
teranode-bridge \
  -tx-listen        '[2001:db8:3f::1]:8833' \
  -subtree-listen   '[2001:db8:3f::1]:9143' \
  -block-listen     '[2001:db8:3f::1]:9144' \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise        'http://[2001:db8:3f::1]:9145' \
  -propagation      'http://192.0.2.10:20833' \
  -kafka            '192.0.2.10:19092'
```

### Full bridge, both directions

Adds the reverse path: what this cluster produces is published back onto the
object plane.

```bash
teranode-bridge \
  -tx-listen        '[2001:db8:3f::1]:8833' \
  -subtree-listen   '[2001:db8:3f::1]:9143' \
  -block-listen     '[2001:db8:3f::1]:9144' \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise        'http://[2001:db8:3f::1]:9145' \
  -propagation      'http://192.0.2.10:20833' \
  -kafka            '192.0.2.10:19092' \
  -blockchain       '192.0.2.10:20087' \
  -local-asset      'http://192.0.2.10:20090/api/v1' \
  -edge-ingress     '2001:db8:1::1' \
  -submitter=true
```

### Standby bridge

Identical, except it never publishes upward. Promote by flipping the flag.

```bash
teranode-bridge \
  ... \
  -blockchain   '192.0.2.10:20087' \
  -local-asset  'http://192.0.2.10:20090/api/v1' \
  -edge-ingress '2001:db8:1::1' \
  -submitter=false
```

### Multi-node cluster

Round-robin across propagation backends (or point at one VIP) and seed from
several brokers.

```bash
teranode-bridge \
  -propagation 'http://192.0.2.11:20833,http://192.0.2.12:20833,http://192.0.2.13:20833' \
  -kafka       '192.0.2.11:19092,192.0.2.12:19092' \
  -advertise   'http://[2001:db8:3f::1]:9145' \
  ...
```

### Sink mode — burn in a delivery slot with no cluster

```bash
teranode-bridge \
  -mode sink \
  -tx-listen      '[2001:db8:3f::1]:8833' \
  -subtree-listen '[2001:db8:3f::1]:9143' \
  -block-listen   '[2001:db8:3f::1]:9144' \
  -stats-every 10s \
  -log-level debug
```

### systemd unit

```ini
[Unit]
Description=teranode-bridge
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/teranode-bridge \
  -tx-listen '[2001:db8:3f::1]:8833' \
  -subtree-listen '[2001:db8:3f::1]:9143' \
  -block-listen '[2001:db8:3f::1]:9144' \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise 'http://[2001:db8:3f::1]:9145' \
  -propagation 'http://192.0.2.10:20833' \
  -kafka '192.0.2.10:19092' \
  -blockchain '192.0.2.10:20087' \
  -local-asset 'http://192.0.2.10:20090/api/v1' \
  -edge-ingress '2001:db8:1::1'
Restart=always
RestartSec=2
DynamicUser=yes

[Install]
WantedBy=multi-user.target
```

`DynamicUser` is fine: the bridge writes nothing to disk and binds only
unprivileged ports by default. Give it
`AmbientCapabilities=CAP_NET_BIND_SERVICE` only if you move a lane below 1024.

### Container

The image sets no `ENV` defaults — flags are the whole configuration surface, so
they go in the container command (or a chart's `args`).

```bash
docker run --rm \
  -p 8833:8833 -p 9143:9143 -p 9144:9144 -p 9145:9145 \
  ghcr.io/lightwebinc/teranode-bridge:0.5.0 \
    -advertise   'http://[2001:db8:3f::1]:9145' \
    -propagation 'http://192.0.2.10:20833' \
    -kafka       '192.0.2.10:19092'
```

The image runs as `nonroot` on a distroless static base, so every lane must stay
above port 1024 unless the container is given `CAP_NET_BIND_SERVICE`.

## Reading the stats

Every `-stats-every`, one block is logged. What each line means, and what an
anomaly points at:

| Line              | Field              | Healthy          | Anomaly means                                                                                                                      |
| ----------------- | ------------------ | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `lane stats`      | `dropped`          | `0`              | Framing faults — a sender is writing malformed objects, or two classes are crossed on one lane                                     |
|                   | `errors`           | `0`              | Per-object handler failures; the paired `submit`/`announce` line says which                                                        |
|                   | `rejected`         | `0`              | Objects refused on lane format policy — on `tx`, a sender emitting BRC-12 standard transactions instead of BRC-30 EF               |
| `registry stats`  | `duplicates`       | small            | Re-delivery of an object already handed over — expected after a failover or reconnect; sustained growth means the delivery side is re-sending |
| `submit stats`    | `accepted`         | rising           | —                                                                                                                                  |
|                   | `rejected`         | `0`              | The cluster refuses these on merits — missing parents, invalid, frozen. Not retryable                                              |
|                   | `failed`           | `0`              | Propagation unreachable or erroring                                                                                                |
| `announce stats`  | `failures`         | `0`              | Kafka unreachable, topic missing, or a mis-advertised broker listener                                                              |
| `retrieval stats` | `subtree`, `block` | tracks announces | A flat count against a rising `announce` means the cluster is not pulling — check `-advertise`, `-api-prefix`, and the topic names |
|                   | `miss`             | `0`              | Cache evicted before the pull (raise `-cache-bytes`/`-cache-ttl`), or a `subtree_data` member transaction was never delivered      |
|                   | `errors`           | `0`              | A genuine bridge-side fault; each one is logged with the object hash                                                               |
| `reverse stats`   | `remote_skipped`   | rising           | Correct — these are objects that arrived from the fabric, filtered out by origin                                                   |
|                   | `skipped`          | occasional       | Object not fully available yet (asset returned `404`)                                                                              |
|                   | `failures`         | `0`              | Asset fetch, encode, or upward submit failed                                                                                       |
|                   | `reconnects`       | occasional       | Routine: the cluster rolls its gRPC connections                                                                                    |
| `up-tunnel stats` | `redials`          | low              | One per dial; a rising count tracks `failures` and means the ingress is flapping                                                   |

Two log lines deserve alerts of their own:

- `ECHO MISMATCH` (error) — the object plane returned different bytes than were
  published. This is a data-integrity fault, not a delivery hiccup.
- `lane framing error, dropping connection` (error) — see `dropped` above.

## Not yet configurable

Fixed values, listed so they are not mistaken for flags:

| Value                          | Setting                                         |
| ------------------------------ | ----------------------------------------------- |
| Seen-registry TTL / capacity   | 30 min / 2²⁰ entries                            |
| Propagation HTTP timeout       | 30 s; 32 idle conns per host, 90 s idle timeout |
| Kafka produce timeout          | 10 s, `RequiredAcks=all-ISR`                    |
| Asset fetch timeout            | 30 s                                            |
| Asset rate-limit retry ladder  | 200 ms, 1 s, 3 s, 8 s                           |
| Blockchain reconnect backoff   | 1 s doubling to a 30 s ceiling                  |
| Up-tunnel dial / write timeout | 10 s / 30 s                                     |
| Retrieval server timeouts      | 10 s read-header, 10 min write, 120 s idle      |
| Lane TCP keepalive             | 30 s                                            |

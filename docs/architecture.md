# Architecture

`teranode-bridge` is a single Go binary that sits between a push-delivery
object plane and an **unmodified** Teranode cluster. It terminates the
per-class object lanes, hands each class to the cluster service that owns it,
and — in the other direction — publishes what the cluster produces back onto
the object plane.

It is a *shim*, not a node: it validates nothing, stores nothing permanently,
and holds no chain state. Everything it does is byte movement plus two format
conversions.

## Why an announce shim

Teranode learns about subtrees and blocks by **announcement plus pull**: a
Kafka message carries `{hash, URL}` and the validating service fetches the
bytes from that URL. The bridge exploits exactly that contract. It already
*has* the bytes — they were pushed to it — so it stores them, announces itself
as the source, and serves the resulting pull from the same LAN.

The result is a fully pushed wide-area path with **no fork of Teranode**: no
patched ingest, no new RPC, no changed validation. The only thing that changes
is where the bytes come from — the machine next door instead of a remote peer's
asset service across the wide area.

Writing directly into Teranode's own stores was considered and rejected: store
presence is treated as *already validated*, so pre-writing would bypass
validation entirely. The bridge never weakens validation — the cluster fetches,
validates, and then owns the data exactly as it would from a peer.

## Planes

| Plane | Role | Scales with |
| --- | --- | --- |
| **Ingest** | lane termination, frame parse, cache write, announce produce | inbound object bandwidth (stateless — round-robin delivery may spray objects across any number of bridges) |
| **Cache** | content-addressed, TTL'd store of pushed bytes — a cache, not a store of record | delivery rate × validation lag (seconds) |
| **Retrieval** | serves the cluster's asset-style pulls out of the cache | pull concurrency (stateless replicas behind a VIP) |
| **Reverse** | blockchain notifications → BRC-143/144 → object-plane submit | one submitter per class |

At small scale all four run in one process (`-mode all`, the default). Splitting
them across processes or hosts is deployment topology, not a different design:
every ingest instance writes the same cache and stamps the same retrieval URL,
so there is no affinity and no per-instance URL bookkeeping.

## Data flow

```
   object plane                    teranode-bridge                 Teranode LAN
 ───────────────────────────────────────────────────────────────────────────────

   tx lane      ─────────▶ :8833 ──┬──▶ tx cache
   (BRC-12/30)                     └──▶ submit ──── POST /tx ─────▶ propagation

   subtree lane ─────────▶ :9143 ──┬──▶ object cache
   (BRC-143)                       └──▶ announce ── Kafka ────────▶ subtree
                                            {hash, URL}             validation
                                                                        │
   block lane   ─────────▶ :9144 ──┬──▶ object cache                    │ pull
   (BRC-144)                       └──▶ announce ── Kafka ──▶ block     │
                                            {hash, URL}      validation │
                                                                  │     │
                        :9145 ◀── GET /subtree/{hash} ────────────┴─────┘
                        retrieval  GET /subtree_data/{hash}
                        plane      POST /subtree/{hash}/txs
                                   GET /block/{hash}

   subtree :8726 ◀─────── reverse ◀── encode BRC-143/144 ◀── GET asset /subtree
   block   :8727 ◀─────── submit      ▲                      GET asset /block
                                      │
                                      └── gRPC Subscribe ◀───── blockchain
                                          (Subtree / Block notifications)
```

Both directions use the same codecs (`shard-common/objfmt`, `internal/encode`,
`internal/tnwire`), which is what makes a cluster-produced object re-encode
byte-for-byte identical to one that arrived from the fabric.

## Ingest — the delivery lanes

`internal/lanes` runs one TCP listener per object class. Each lane carries
exactly one class, so the stream is **bare**: no length prefix, no type tag, no
sync marker. Objects are delimited by walking their own structure, which
`objfmt.Reader` does:

| Class | Delimited by | Default bind |
| --- | --- | --- |
| `tx` (BRC-12 raw / BRC-30 extended) | walking version, input/output vectors, locktime | `[::]:8833` |
| `subtree` (BRC-143) | the 40-byte header's `NodeCount` | `[::]:9143` |
| `block` (BRC-144) | the 104-byte prefix's counts | `[::]:9144` |

Anything that writes bare `objfmt` object streams can feed a lane. In the
reference stack that is the multicast fabric's delivery side; for tests,
[`subtx-generator`](https://github.com/lightwebinc/subtx-generator) drives the
subtree and block lanes directly with its BRC-143/144 push senders.

Connection handling reflects the bare framing:

- **Framing error → drop the connection.** There is no resync point, so every
  byte after a codec fault is suspect. The counter `dropped` records this; the
  sender is expected to redial.
- **Handler error → count and continue.** A cluster-side failure on one object
  must not cost the rest of the stream.
- **Clean EOF, mid-stream reset, or a connection that closed before a whole
  object** are all logged as ordinary events, not faults — they are health
  probes, pool rotations and reconnects.
- TCP keepalive is set to 30 s, because senders hold these connections open for
  the life of a link and a peer that vanishes without a FIN is otherwise
  invisible.
- A single object is bounded by `-max-object` (default: the `objfmt` codec
  default of 64 MiB).

### Per-class handling

**Transactions** (`handleTx`) — compute the txid, put the transaction in the tx
cache (so it can serve as a subtree member later), then mark it in the seen
registry. A hash already registered is a re-delivery after a failover or
reconnect and is dropped without a second submit. Otherwise it is POSTed to the
propagation service.

**Subtrees** (`handleSubtree`) — the frame's first 32 bytes are the merkle root
in wire order; the node count follows. The object is **stored before it is
announced**: the cluster does not retry a failed subtree fetch, so the bytes must
be servable the instant the announcement lands.

**Blocks** (`handleBlock`) — the identity is `SHA256d(header[:80])`, exactly as
the chain identifies a block. Stored, then announced.

## Submitting transactions

`internal/submit` uses the propagation service's **HTTP** endpoint (`POST /tx`)
rather than its gRPC API, for three reasons that all matter to a bridge:

- The body is the raw transaction — exactly the bytes that arrived on the lane —
  so nothing is re-encoded.
- HTTP classifies errors correctly. Over gRPC every validator failure flattens
  into one opaque internal error, so a duplicate is indistinguishable from a real
  rejection; over HTTP an already-known transaction is a plain `200` and each
  failure class has its own status.
- It needs no generated stubs, so the bridge does not link the cluster's module
  to send a byte slice.

Extended format is accepted and preserved, so a transaction reaches the
validator with its prevout data intact.

| Status | Outcome | Meaning |
| --- | --- | --- |
| `200` | `accepted` | taken (the handler also answers 200 for an already-known transaction) |
| `409` | `duplicate` | spent / conflicting / locked — the cluster already has this outpoint's spend |
| `400`, `403`, `422` | `rejected` | refused on merits; retrying the same bytes cannot change the answer |
| anything else | `failed` | transport or server fault; retryable |

Multiple `-propagation` endpoints are round-robined per object, which spreads
long-lived flows across a multi-node cluster or a VIP with no per-object work.
`GET /health` on any endpoint satisfies the (non-fatal) startup check.

## Announcing

`internal/announce` produces to the cluster's Kafka with
[franz-go](https://github.com/twmb/franz-go), `RequiredAcks=all-ISR`, one
synchronous produce per object, bounded by a 10 s deadline.

The subtree and block topic messages have the same shape — three string fields:

```
field 1: hash     object hash, hex, display (reversed) order
field 2: URL      base URL of the server that will serve the bytes
field 3: peer_id  originating peer identity (omitted; see below)
```

They are encoded with the low-level protobuf wire encoder rather than generated
stubs: pulling in the cluster's generated package would drag its entire module
into a small binary. Fields are written in ascending order and empty strings are
omitted, which is what proto3's canonical encoder does.

**`peer_id` is deliberately empty.** It is a libp2p peer identity used for
reputation bookkeeping, and an empty value short-circuits every such check. A
non-empty identity the cluster's p2p service does not know is not merely
cosmetic: on the block path an unrecognised or flagged peer causes catchup to be
skipped and fetches to be refused. The bridge is not a libp2p peer, so it claims
no identity.

A duplicate announcement is harmless — the cluster dedups by hash — so nothing
here needs idempotent-producer or transactional semantics.

## Retrieval plane

`internal/retrieval` implements the subset of the asset API that subtree
validation and block validation actually call, and nothing else. Two rules shape
every handler:

- **Never answer `200` with an empty or wrong body.** An empty subtree body is an
  explicit error in the cluster, and a wrong body fails a root-hash check that
  would otherwise be a silent corruption. Unknown object ⇒ `404`.
- **Never answer `5xx` for something simply not held.** On the block path a
  server-fault status is classified as recoverable, so the cluster does not
  commit the Kafka offset and redelivers forever; `404` ends it cleanly and keeps
  the bridge out of the cluster's malicious-peer classification.

| Route | Answer |
| --- | --- |
| `GET {prefix}/subtree/{hash}` | the subtree's node hashes: `numLeaves × 32` raw bytes — precisely the BRC-143 frame with its 40-byte header removed, so no transformation is needed |
| `GET {prefix}/subtree_data/{hash}` | the member transactions concatenated in node order, skipping the coinbase placeholder at node 0 |
| `POST {prefix}/subtree/{hash}/txs` | body is raw 32-byte txids with no count or delimiter; response is the matching transactions concatenated (the cluster re-keys by txid, so order does not matter — but the count must match exactly) |
| `GET {prefix}/block/{hash}` | the block in Teranode's serialization, transcoded from the held BRC-144 frame |
| `GET {prefix}{prefix}/subtree_data/{hash}` | alias — one caller in the cluster appends the API prefix to an already-prefixed base URL; serving it costs nothing and avoids a failure that would look like a missing object |
| everything else | `404` |

A missing member on `subtree_data` yields a clean `404` rather than a partial
body: a partial body fails the cluster's per-index txid check anyway, and the
`404` lets it fall back to the batch route.

Transactions are served in **standard** serialization: if a cached transaction is
in extended format it is converted first. The txid is unchanged either way, and
matching the cluster's own asset behaviour keeps the bridge maximally boring.

Server timeouts: 10 s read-header, 10 min write (a large subtree streams),
120 s idle.

## Cache and seen-registry

`internal/cache` is a hash-keyed LRU with a TTL and a total-bytes ceiling, safe
for concurrent use — ingest writes while retrieval reads. Two independent
instances are created: one for objects (subtrees and blocks), one for
transactions. Storing a key that already exists refreshes it and keeps the
original bytes, which is what makes re-delivery a no-op.

This is a **cache, not a store of record**: the store of record is the cluster's
own, after validation. The working set is delivery rate × validation lag —
seconds of traffic — so eviction is a normal event, not data loss. A pull that
misses returns `404` and the cluster falls back to its ordinary peer-pull path.

`internal/registry` is a TTL'd set of hashes with the direction each was seen in.
It does two jobs that look similar but are not:

- **Down (delivery)** — suppress re-injection of an object already handed to the
  cluster. Failover and reconnects legitimately re-deliver.
- **Up (reverse path)** — decide whether a subtree or block the cluster just
  accepted actually originated here.

Entries expire (default 30 minutes, 2²⁰ entries): the question is only
interesting while an object is in flight. Pruning is bounded — a full sweep
happens only at the ceiling.

## Reverse path — cluster to object plane

`internal/reverse` holds a gRPC `Subscribe` on the cluster's blockchain service
and republishes what the cluster produces. `proto/blockchain_api` is a minimal,
wire-compatible subset of that service: one service, one method, two messages,
no imports. Only the proto package name, service name, method name and field
numbers are load-bearing on the wire.

1. **Learn.** `Subtree` (type 1) fires when a subtree is admitted; `Block`
   (type 2) on block acceptance. The stream is plaintext and unauthenticated in
   this deployment. A lost stream is routine — the cluster rolls its gRPC
   connections — so the subscriber reconnects with backoff from 1 s to 30 s.

2. **Filter by origin.** The notification stream carries **no local-vs-remote
   marker** — the node's own p2p announces every notification to its gossip
   peers, because in gossip any validator is a legitimate serve source. So the
   origin filter is the seen registry: a hash the bridge delivered *into* the
   cluster is remote in origin and must not be pushed back; a hash it already
   submitted is its own push coming back around. What remains is what this
   cluster produced.

   One consequence is worth stating plainly: content this node learned over
   libp2p while the link was down looks unseen and will be pushed up after
   recovery. That duplicate is bounded by the outage and harmless — every
   receiver dedups by hash — and the clean fix is an origin marker on the
   notification, which is an upstream ask.

3. **Fetch and encode.** `internal/tnasset` pulls the object back out of the
   cluster's asset service and `internal/encode` builds the push frame.
   - Subtree: `GET /subtree/{hash}` returns the member hash list; the announced
     hash *is* the merkle root, so the frame's root field and the identity the
     cluster gave us are the same value — no recomputation, and no chance of
     publishing a root that disagrees with the node list.
   - Block: `GET /block/{hash}` returns everything the frame needs, including the
     coinbase and its BUMP, so no part of the block is reconstructed or guessed.
   - `404` means "not ours to publish, or not written yet" and is skipped, not
     failed.
   - The asset API's rate limiter trips readily — a burst of mined blocks
     produces a burst of notifications, each one a fetch — so `429` is retried on
     a `200 ms / 1 s / 3 s / 8 s` ladder. Without it, a catch-up window would
     silently drop objects and nothing downstream could tell the difference
     between "the miner produced nothing" and "we were throttled".

4. **Register, then send.** The hash is marked `submitted` *before* the send,
   because the object comes straight back down the bridge's own delivery lanes
   and must be recognised as ours when it does. The exact bytes are kept so that
   unavoidable echo becomes a free correctness check (below).

5. **Submit.** `internal/submit.UpTunnel` holds one long-lived TCP connection per
   class to the object-plane ingress (`8726` subtree, `8727` block by
   convention). The stream is bare, so a partial write leaves the receiver's
   parser mid-object with no way to resynchronise: the only correct recovery is
   to drop the connection and redial, which is what a failed write does.

Every frame is self-verified against the shared codec before it leaves the
process. A frame that sizes wrong would not merely be rejected — it would
desynchronise the stream and cost every object behind it.

**Submitter role.** Exactly one bridge per cluster should hold `-submitter` for a
given class. A bridge with `-submitter=false` still runs its delivery lanes and
retrieval plane; only the reverse path stays idle. Promotion is manual.

## Echo verification

Own-traffic exclusion covers only the tx class, so everything this cluster
publishes returns to it on its own subtree and block lanes. That is not waste: it
is a free end-to-end proof that what the fabric carried is byte-for-byte what was
sent — across encode, submit, reframe, multicast, strip and deliver.

On arrival, an object whose hash is registered as `submitted` is compared against
the stored copy. Equal ⇒ `echo verified byte-identical` at info level. Different
⇒ `ECHO MISMATCH` at error level, because the object plane is corrupting data.
Either way the object is then dropped as a duplicate.

## Byte order

Bitcoin hashes exist in two orders, and `internal/hashid` is the only place the
bridge converts between them:

- **Internal (wire) order** — inside frames, subtree node lists, block headers.
- **Display order** — the byte-reversed hex form used in RPC output, URL path
  segments and the `hash` field of a Kafka announcement.

Teranode parses announced and requested hashes with a constructor that reverses,
so a hash taken straight off the wire and hex-encoded would be silently wrong:
the announcement would name an object nobody can find. Every conversion goes
through this one package.

## Block frame conversion

`internal/tnwire` converts between Teranode's block serialization and the BRC-144
push frame in both directions, deliberately in one file so they cannot drift. The
two formats carry identical information in identical order and differ only in how
four counts are written:

```
BRC-144:  header[80] | txCount u64BE | size u64BE | subtreeCount u64BE |
          roots[32×M] | coinbase | height u64BE | bumpLen u64BE | bump

Teranode: header[80] | varint txCount | varint size | varint subtreeCount |
          roots[32×M] | coinbase | varint height | varint bumpLen | bump
```

BRC-144's fixed 8-byte big-endian fields let a frame be sized without parsing;
Teranode uses Bitcoin CompactSize varints. The 80-byte header, the subtree roots,
the in-band coinbase and the coinbase BUMP are byte-for-byte the same in both. A
round trip through the pair is lossless, which the tests assert.

Two details are enforced rather than passed through:

- An all-zero subtree root is rejected in both directions — Teranode rejects it
  outright, and catching it here turns an opaque cluster-side parse failure into
  a clear error.
- On decode, the trailing BUMP length is optional: Teranode's own parser tolerates
  a body that ends right after the height, so this does too and yields an empty
  BUMP.

The coinbase is delimited by walking its transaction structure, so it must parse
exactly — a trailing byte would swallow the height field.

## Sink mode

`-mode sink` receives, parses, verifies and counts objects with **no cluster
targets at all**: no propagation submit, no Kafka producer, no retrieval plane,
no reverse path. Every lane still runs, still enforces framing and still fills
the caches — nothing reads them back, because the retrieval plane is not started
either. Sink mode exists to burn in a delivery slot before a cluster is present,
and to isolate object-plane faults from cluster-side ones.

## Failure modes

| Failure | Absorbed by |
| --- | --- |
| Delivery link flaps / fails over | Lanes accept redials; the seen registry drops re-delivered objects; the cache keeps the original bytes |
| Malformed object on a lane | Connection dropped (no resync point exists), `dropped` incremented, sender redials |
| Propagation endpoint down | Round-robin spreads to the remaining endpoints; the object counts as `failed` and is logged |
| Cluster refuses a transaction on merits | `rejected` — not retried; retrying identical bytes cannot change the answer |
| Kafka unreachable | `announce failures` increments; the object stays cached until TTL, and the cluster never learns of it |
| Cache entry evicted before the pull | Pull answers `404`; the cluster falls back to its ordinary peer announce-and-pull path |
| Asset API rate-limits the reverse path | Retry ladder; after exhaustion the object is a `failure` and is not published |
| Blockchain stream lost | Reconnect with backoff; `reconnects` increments |
| Up-tunnel write fails | Connection closed and redialled on the next object; `failures` and `redials` increment |
| Bridge restart | Registry rebuilt lazily; the cluster's hash dedup absorbs re-injection |
| Submitter down | No corruption, just a gap: the fabric misses this cluster's output until a standby is promoted |

## Package layout

```
cmd/teranode-bridge/     entrypoint: flags, wiring, per-class handlers, stats
internal/lanes/          per-class TCP listeners over bare objfmt streams
internal/submit/         tx.go       → propagation HTTP submit + outcome classes
                         uptunnel.go → one long-lived TCP conn per class, upward
internal/announce/       Kafka {hash, URL, peer_id} producer + wire codec
internal/cache/          hash-keyed LRU with TTL and byte ceiling
internal/registry/       TTL'd seen-set with direction (delivered / submitted)
internal/retrieval/      the asset-API subset the cluster pulls from
internal/tnasset/        the mirror: pulls objects back out of the cluster
internal/encode/         BRC-143 / BRC-144 push-frame builders (self-verifying)
internal/tnwire/         BRC-144 ⇄ Teranode block serialization, both directions
internal/hashid/         internal ⇄ display byte order, in exactly one place
proto/blockchain_api/    minimal wire-compatible blockchain Subscribe subset
```

## Observability

The bridge logs structured lines via `log/slog` (text handler, stdout) and emits
a stats block every `-stats-every` (default 60 s, `0` disables):

| Line | Fields |
| --- | --- |
| `lane stats` | per lane: `conns`, `objects`, `bytes`, `errors`, `dropped` |
| `cache stats` | `objects`, `object_bytes`, `txs`, `tx_bytes`, `evicted` |
| `registry stats` | `entries`, `duplicates` |
| `submit stats` | `accepted`, `duplicate`, `rejected`, `failed` |
| `announce stats` | `subtrees`, `blocks`, `failures` |
| `retrieval stats` | `subtree`, `subtree_data`, `txs`, `block`, `miss`, `errors` |
| `reverse stats` | `subtrees_up`, `blocks_up`, `remote_skipped`, `skipped`, `failures`, `reconnects` |
| `up-tunnel stats` | per class: `sent`, `bytes`, `failures`, `redials` |

A final stats block is emitted on clean shutdown. `SIGINT`/`SIGTERM` cancels the
root context, which closes the listeners *and* the open lane connections — the
latter matters because a reader parked on a long-lived connection would otherwise
block shutdown indefinitely.

There is no Prometheus endpoint yet; counters are log-only.

## Resource footprint

Memory is dominated by the two caches (`-cache-bytes` each) plus the seen
registry (≈ 50 bytes per live hash, bounded at 2²⁰ entries ≈ 50 MiB). Per-lane
read buffers grow to at most `-max-object` per open connection. CPU is
negligible: hashing block headers, walking transaction structures, and two
count-format conversions.

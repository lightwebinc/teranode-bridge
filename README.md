# teranode-bridge

[![CI](https://github.com/lightwebinc/teranode-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/teranode-bridge/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/teranode-bridge/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/teranode-bridge/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/teranode-bridge)](https://github.com/lightwebinc/teranode-bridge/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/teranode-bridge.svg)](https://pkg.go.dev/github.com/lightwebinc/teranode-bridge)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project — see the main repository for the full architecture, design docs, and BRC specifications.

A landing-tier shim for **pushed delivery into an unmodified Teranode cluster**.

`teranode-bridge` terminates the per-class object delivery lanes on a machine in
front of a Teranode cluster, hands each object class to the cluster service that
owns it, and submits cluster-produced subtrees and blocks back onto the object
plane — while the Teranode cluster itself stays **vanilla**.

```
   object plane ══push══▶  teranode-bridge                Teranode LAN
                           ├── tx lane      → propagation      (cluster ingest)
                           ├── subtree lane → cache + announce ─────┐
                           ├── block lane   → cache + announce ─────┤
                           ├── retrieval plane   ◀────── asset pull ┘
                           └── reverse: blockchain Subscribe → BRC-143/144 → up
```

## Why an announce shim

Teranode learns about subtrees and blocks by *announcement plus pull*: a Kafka
message carries `{hash, URL}` and the validating service fetches the bytes from
that URL. The bridge exploits exactly that contract — it already **has** the
bytes (they were pushed to it), so it stores them, announces itself as the
source, and serves the pull from the same LAN.

The result is a fully pushed wide-area path with **no fork of Teranode**: no
patched ingest, no new RPC, no changed validation. The only thing that changes is
where the bytes come from — the machine next door instead of a remote peer's
asset service across the wide area.

Writing directly into Teranode's own stores was considered and rejected: store
presence is treated as *already validated*, so pre-writing would bypass
validation entirely. The bridge never weakens validation — the cluster fetches,
validates, and then owns the data exactly as it would from a peer.

## Planes

| Plane | Role | Scales with |
| --- | --- | --- |
| **Ingest** | lane termination, frame parse, cache write, announce produce | inbound object bandwidth (stateless; round-robin delivery may spray objects across any number of bridges) |
| **Cache** | content-addressed, TTL'd store of pushed bytes — a cache, not a store of record | delivery rate × validation lag (seconds) |
| **Retrieval** | serves the cluster's asset-style pulls from the cache | pull concurrency (stateless replicas behind a VIP) |
| **Reverse** | blockchain notifications → BRC-143/144 → upward submit | one submitter per class |

At small scale all four run in one process (`-mode all`, the default). Splitting
them is deployment topology, not a different design.

## Documentation

- [Architecture](docs/architecture.md) — planes, lane framing, announce/pull contract, reverse path, byte order, block frame conversion, failure modes, package layout
- [Configuration](docs/configuration.md) — every flag, defaults, required-flag matrix, deployment examples, reading the stats
- [BRC-143 — Subtree Data](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-143-subtree-data.md)
- [BRC-144 — Block Frame](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-144-block-frame.md)
- [BRC-30 — Transaction Extended Format (EF)](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0030.md) — what the tx lane carries, preserved end to end so the validator gets its prevout data

## Requirements

- Go 1.25 or later
- A reachable Teranode cluster (propagation HTTP, Kafka, asset HTTP, blockchain
  gRPC) — or `-mode sink`, which needs none of them
- Network reachability *from* the cluster back to the bridge's retrieval plane
- For `make ci` / `make ci-*`: Docker (Dagger provisions its engine through it)
- For `make proto`: [`buf`](https://buf.build) plus `protoc-gen-go` and
  `protoc-gen-go-grpc` on `PATH`

## Build

```bash
make build          # static binary at ./teranode-bridge
make test           # go test -race ./...
make lint           # golangci-lint
make ci             # full containerised pipeline (see below)
make help           # list targets
```

Or without the Makefile:

```bash
go build ./cmd/teranode-bridge
```

## Run

```bash
# Delivery only: terminate the lanes, submit transactions, announce and serve
# subtrees and blocks.
./teranode-bridge \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise        'http://[2001:db8:3f::1]:9145' \
  -propagation      'http://192.0.2.10:20833' \
  -kafka            '192.0.2.10:19092'

# Both directions: also publish what this cluster produces back onto the
# object plane.
./teranode-bridge \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise        'http://[2001:db8:3f::1]:9145' \
  -propagation      'http://192.0.2.10:20833' \
  -kafka            '192.0.2.10:19092' \
  -blockchain       '192.0.2.10:20087' \
  -local-asset      'http://192.0.2.10:20090/api/v1' \
  -edge-ingress     '2001:db8:1::1'

# Sink: receive, parse, verify and count with no cluster targets at all.
./teranode-bridge -mode sink -stats-every 10s
```

See [docs/configuration.md](docs/configuration.md) for the full flag reference.

## Default ports

| Port | Direction | Carries |
| --- | --- | --- |
| `8833` | in | transaction lane (BRC-30 extended format only; standard-format transactions are refused) |
| `9143` | in | subtree lane (BRC-143 push frames) |
| `9144` | in | block lane (BRC-144 push frames) |
| `9145` | in | retrieval plane — the cluster's pulls |
| `9146` | in | `/metrics`, `/healthz`, `/readyz`, `/loglevel` |
| `8726` | out | BRC-143 subtree submits to the object-plane ingress |
| `8727` | out | BRC-144 block submits to the object-plane ingress |

## Observability

Prometheus metrics on `-metrics-addr` (default `[::]:9146`) under the `btb_`
prefix, alongside `/healthz`, `/readyz` (ready once every lane is bound) and a
runtime `POST /loglevel`. Structured `log/slog` output carries the same numbers
in a stats block every `-stats-every` (default 60 s).

The alert that matters is **`btb_echo_mismatch_total`** (log line
`ECHO MISMATCH`): non-zero means the object plane returned different bytes than
were published — a data-integrity fault, not a delivery hiccup. Worth watching
too: `btb_lane_connections_dropped_total` (framing faults),
`btb_announce_failures_total`, and `btb_retrieval_misses_total` climbing against
a flat `btb_cache_evicted_total`.

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with a single
static binary at `/usr/local/bin/teranode-bridge`. No in-image `ENV` defaults are
set — the bridge is configured entirely by flags, so pass them as the container
command or Helm `args`.

```bash
docker build --build-arg VERSION=0.3.0 -t teranode-bridge:0.3.0 .
docker run --rm teranode-bridge:0.3.0 -mode sink
```

Published images are gated behind a manual `image-publish` workflow run
(`ghcr.io/lightwebinc/teranode-bridge:<tag>`); there is no automatic push.

## CI

`make ci` runs the whole pipeline in containers via
[Dagger](https://dagger.io) — `tidy` → `lint` → `vuln` → `unit` → `build` →
`image` — so a local run and a GitHub Actions run execute the same steps. Each
stage is also available on its own (`make ci-unit`, `make ci-lint`, …), and
`make ci-shell` drops you into the builder container.

CI resolves `shard-common` from a sibling checkout and applies a local replace,
picking the branch matching the current one when it exists and `main` otherwise —
so a shared-library change can be validated here before it is tagged. The image
build deliberately does *not* use that replace: a published image always resolves
`shard-common` from the module proxy at its committed version.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/teranode-bridge-helm`](https://github.com/lightwebinc/teranode-bridge-helm)
- OCI: `helm install bridge oci://ghcr.io/lightwebinc/charts/teranode-bridge`

`config.advertise`, `config.propagation` and `config.kafka` are effectively
required (the chart warns and the bridge exits without them) unless
`config.mode=sink`. See the chart README for the three-service shape and the
submitter-role scaling rules.

## Layout

```
.
├── cmd/teranode-bridge/     # entrypoint: flags, wiring, per-class handlers
├── internal/
│   ├── lanes/               # per-class TCP listeners over bare object streams
│   ├── submit/              # propagation HTTP submit + upward object submit
│   ├── announce/            # Kafka {hash, URL} producer + wire codec
│   ├── cache/               # hash-keyed LRU with TTL and byte ceiling
│   ├── registry/            # TTL'd seen-set with direction
│   ├── retrieval/           # the asset-API subset the cluster pulls from
│   ├── tnasset/             # the mirror: pulls objects back out of the cluster
│   ├── encode/              # BRC-143 / BRC-144 push-frame builders
│   ├── tnwire/              # BRC-144 ⇄ Teranode block serialization
│   └── hashid/              # internal ⇄ display byte order, in one place
├── proto/blockchain_api/    # minimal wire-compatible Subscribe subset
├── ci/                      # Dagger CI driver
├── docs/                    # architecture + configuration
├── Dockerfile
├── Makefile
└── .github/workflows/{ci,codeql,image-publish,release}.yml
```

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `objfmt`, the push object-frame codecs shared with the rest of the stack
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka producer
- `google.golang.org/grpc`, `google.golang.org/protobuf` — blockchain notification stream

The bridge deliberately does **not** link Teranode's own module. The two contracts
it needs — a three-field announcement message and a one-method notification
stream — are reproduced from their wire definitions instead, so a small shim does
not pull in a full node's dependency tree.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

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

Teranode learns about subtrees and blocks by *announcement plus pull*. The bridge
already **has** the bytes — they were pushed to it — so it stores them, announces
itself as the source, and serves the resulting pull from the same LAN. That buys
a fully pushed wide-area path with **no fork of Teranode**, and the cluster still
fetches and validates exactly as it would from a peer.

[Architecture › Why an announce shim](docs/architecture.md#why-an-announce-shim)
covers the contract in full, including why writing into Teranode's own stores was
rejected.

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
- [Metrics reference](docs/references/prometheusMetrics.md) — every metric, the endpoints, and the alert expressions
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
| `8725` | in | transaction lane (BRC-30 extended format only; standard-format transactions are refused) |
| `9143` | in | subtree lane (BRC-143 push frames) |
| `9144` | in | block lane (BRC-144 push frames) |
| `9145` | in | retrieval plane — the cluster's pulls |
| `9146` | in | `/metrics`, `/health*`, `/healthz`, `/readyz`, `/loglevel`, `/debug/pprof` |
| `8726` | out | BRC-143 subtree submits to the object-plane ingress |
| `8727` | out | BRC-144 block submits to the object-plane ingress |

## Observability

Metrics, health, tracing and profiling are shaped to match Teranode's own, so a
bridge reads like a cluster service rather than a foreign appliance: series are
`teranode_bridge_*` on the same `Namespace`/`Subsystem` grid, histograms use
Teranode's bucket sets verbatim, and `/health`, `/health/readiness` and
`/health/liveness` answer with the same JSON dependency report and the same
`?timeout=` override.

Everything is on `-metrics-addr` (default `[::]:9146`). Structured `log/slog`
output carries the same numbers in a stats block every `-stats-every` (60 s).

The alert that matters is **`teranode_bridge_echo_mismatch_total`** (log line
`ECHO MISMATCH`): non-zero means the object plane returned different bytes than
were published — a data-integrity fault, not a delivery hiccup. Two more are
specific to what a landing shim can get wrong: `teranode_bridge_announce_to_first_pull_seconds`
is the only measurement of whether the announce-shim trick is working at all
(nothing on either side of the bridge measures it), and
`sum(teranode_bridge_submitter_active)` must equal exactly 1 per cluster per
class — 0 means nothing is published upward, 2 means double publish.

- [Metrics reference](docs/references/prometheusMetrics.md) — every series, with the alert expressions worth running
- [Configuration › Observability endpoints](docs/configuration.md#observability-endpoints) — flags, gating vs advisory health, tracing, profiling
- [Configuration › Blockchain connection](docs/configuration.md#blockchain-connection-transport-security-and-liveness) — `security_level_grpc` interop and the keepalive that stops the reverse path wedging

> Series that were `btb_*` are dual-emitted under both names while
> `-metrics-legacy-prefix` is true (the default), so existing dashboards survive
> the cutover.

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with a single
static binary at `/usr/local/bin/teranode-bridge`. No in-image `ENV` defaults are
set — the bridge is configured entirely by flags, so pass them as the container
command or Helm `args`.

```bash
docker build --build-arg VERSION=0.5.0 -t teranode-bridge:0.5.0 .
docker run --rm teranode-bridge:0.5.0 -mode sink
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
│   ├── txpipe/              # batching transaction submit pipeline (POST /txs)
│   ├── announce/            # Kafka {hash, URL} producer + wire codec
│   ├── cache/               # hash-keyed LRU with TTL and byte ceiling
│   ├── registry/            # TTL'd seen-set with direction
│   ├── retrieval/           # the asset-API subset the cluster pulls from
│   ├── tnasset/             # the mirror: pulls objects back out of the cluster
│   ├── reverse/             # blockchain Subscribe → origin filter → publish up
│   ├── encode/              # BRC-143 / BRC-144 push-frame builders
│   ├── tnwire/              # BRC-144 ⇄ Teranode block serialization
│   ├── metrics/             # Prometheus collector over Stats()
│   └── hashid/              # internal ⇄ display byte order, in one place
├── proto/blockchain_api/    # minimal wire-compatible Subscribe subset
├── ci/                      # Dagger CI driver
├── hack/tnbench/            # throughput bench rig (mock propagation + feeder)
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

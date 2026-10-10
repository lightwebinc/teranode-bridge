# teranode-bridge

[![CI](https://github.com/lightwebinc/teranode-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/teranode-bridge/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/teranode-bridge/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/teranode-bridge/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/teranode-bridge)](https://github.com/lightwebinc/teranode-bridge/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/teranode-bridge.svg)](https://pkg.go.dev/github.com/lightwebinc/teranode-bridge)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project — see the main repository for the full architecture, design docs, and BRC specifications.

A landing-tier bridge for **pushed delivery into an unmodified Teranode cluster**.

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

See [Architecture › Why an announce shim](docs/architecture.md#why-an-announce-shim).

Four planes (ingest, cache, retrieval, reverse) run in one process by default
(`-mode all`); splitting them is deployment topology. See
[Architecture](docs/architecture.md).

## Documentation

- [Architecture](docs/architecture.md) — planes, lane framing, announce/pull contract, reverse path, byte order, block frame conversion, failure modes, package layout
- [Configuration](docs/configuration.md) — every flag, defaults, required-flag matrix, deployment examples, reading the stats
- [Metrics reference](docs/references/prometheusMetrics.md) — every metric, the endpoints, and the alert expressions
- [Development](docs/development.md) — build, CI, container image, dependencies
- [BRC-143 — Subtree Data](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-143-subtree-data.md)
- [BRC-144 — Block Frame](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-144-block-frame.md)
- [BRC-30 — Transaction Extended Format (EF)](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0030.md) — what the tx lane carries, preserved end to end so the validator gets its prevout data

## Requirements

- Go 1.26 or later
- A reachable Teranode cluster (propagation HTTP, Kafka, asset HTTP, blockchain
  gRPC) — or `-mode sink`, which needs none of them
- Network reachability *from* the cluster back to the bridge's retrieval plane

## Build

```bash
make build          # static binary at ./teranode-bridge
make test           # go test -race ./...
make ci             # full containerised pipeline
```

[Development](docs/development.md) covers CI, the container image and dependencies.

## Run

```bash
# Delivery only: terminate the lanes, submit transactions, announce and serve
# subtrees and blocks.
./teranode-bridge \
  -retrieval-listen '[2001:db8:3f::1]:9145' \
  -advertise        'http://[2001:db8:3f::1]:9145' \
  -propagation      'http://192.0.2.10:20833' \
  -kafka            '192.0.2.10:19092'

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
| `8726` | out | BRC-143 subtree submits to the edge proxy's object ingress (`-edge-subtree-port`) |
| `8727` | out | BRC-144 block submits to the edge proxy's object ingress (`-edge-block-port`) |

Ports encode the side of the fabric a lane sits on; see
[Configuration › Lane numbers](docs/configuration.md#lane-numbers).

## Observability

Series are `teranode_bridge_*` on `-metrics-addr` (default `[::]:9146`), shaped
like Teranode's own (same `Namespace`/`Subsystem` grid, bucket sets and
`/health*` JSON). The alert that matters is `teranode_bridge_echo_mismatch_total`:
non-zero means the object plane returned different bytes than were published.
`sum(teranode_bridge_submitter_active)` must be exactly 1 per cluster per class.

- [Metrics reference](docs/references/prometheusMetrics.md): every series and the alert expressions
- [Configuration › Observability endpoints](docs/configuration.md#observability-endpoints): health, tracing, profiling, the `btb_*` legacy prefix

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`charts/teranode-bridge`](https://github.com/lightwebinc/charts/tree/main/charts/teranode-bridge)
- OCI: `helm install bridge oci://ghcr.io/lightwebinc/charts/teranode-bridge`

`config.peerId` is required, and `config.advertise`, `config.propagation` and
`config.kafka` unless `config.mode=sink`. See the chart README for scaling rules.

## Layout

```
.
├── cmd/teranode-bridge/     # entrypoint: flags, wiring, per-class handlers
├── lanes/                   # per-class TCP listeners over bare object streams
├── announce/                # Kafka {hash, URL, peer_id} producer + wire codec
├── cache/                   # hash-keyed LRU (objects) + generational (txs), TTL + byte ceiling
├── registry/                # TTL'd seen-set with direction
├── retrieval/               # the asset-API subset the cluster pulls from
├── reverse/                 # blockchain Subscribe → origin filter → publish up; TLS, keepalive, promoter
├── encode/                  # BRC-143 / BRC-144 push-frame builders
├── tnwire/                  # BRC-144 ⇄ Teranode block serialization
├── hashid/                  # internal ⇄ display byte order, in one place
├── internal/                # submit, txpipe, tnasset, health, obs, metrics, tracing
├── proto/blockchain_api/    # minimal wire-compatible Subscribe subset
├── ci/                      # Dagger CI driver
├── deploy/grafana/          # dashboard JSON
├── hack/                    # tnbench and propbench rigs
├── docs/                    # architecture, configuration, development, metrics reference
├── Dockerfile
├── Makefile
└── .github/workflows/{ci,codeql,image-publish,release,vuln}.yml
```

The top-level packages are the public extension seams an importing module
(such as `arcade-bridge`) builds on; `internal/` stays private to this binary.

## Releases

Releases and notes live on [GitHub Releases](https://github.com/lightwebinc/teranode-bridge/releases); there is no CHANGELOG.
Images (`ghcr.io/lightwebinc/teranode-bridge:<tag>`) are published by a manual workflow run.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

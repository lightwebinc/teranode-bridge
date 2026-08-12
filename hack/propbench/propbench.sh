#!/usr/bin/env bash
# propbench — propagation-only Teranode rig (no compose plugin required).
#
#   ./propbench.sh up            # redpanda + blockchain + propagation
#   ./propbench.sh count         # txs propagation has accepted (kafka watermarks)
#   ./propbench.sh down
#
# MEASURED 2026-08-12 (2x18-core host, rig on the same box as the driver):
#   16 conns -> 152,666 tx/s   client-side, and the validatortxs high-watermark
#                              delta AGREES to within 1 tx (4,579,971 in 30s)
#   32 conns -> 123,742 tx/s   past the knee: more concurrency is slower
#
# What that number IS: the ingest edge with a null tx store and no validation.
# It is an UPPER BOUND on what any real cluster can absorb, never a cluster
# throughput claim — everything expensive (validator, UTXO store, block
# assembly) sits on the far side of the validatortxs topic this rig terminates.
# A 15s CPU profile at 32 conns is 29% runtime.(*lfstack).pop + 6% push, i.e.
# the ceiling here is Go allocator/GC contention inside propagation, not the
# bridge and not the network.
#
# Same containers and settings as docker-compose.yml in this directory; see that
# file for why each setting is what it is. Propagation ends up on
# http://127.0.0.1:18833 (/txs, /tx, /health) with metrics on :16060/metrics.
set -euo pipefail

IMAGE="${TERANODE_IMAGE:-teranode:latest}"
NET=propbench
RATELIMIT="${PROPAGATION_HTTP_RATE_LIMIT:-0}"
BATCH_CONCURRENCY="${PROPAGATION_BATCH_CONCURRENCY:-0}"

up() {
  docker network create "$NET" >/dev/null 2>&1 || true

  docker run -d --name propbench-kafka --network "$NET" \
    redpandadata/redpanda:latest \
    redpanda start --smp 4 --overprovisioned --node-id 0 \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr PLAINTEXT://propbench-kafka:9092 >/dev/null
  sleep 8

  docker run -d --name propbench-blockchain --network "$NET" \
    -e SETTINGS_CONTEXT=docker.m \
    -e logLevel=INFO \
    -e blockchain_store="sqlite:///blockchain" \
    -e blockchain_grpcListenAddress=":8087" \
    -e local_test_start_from_state=RUNNING \
    -e kafka_blocksFinalConfig="kafka://propbench-kafka:9092/blocks-final?partitions=1&replication=1&retention=60000" \
    -e prometheusEndpoint=/metrics -e profilerAddr=":6060" \
    "$IMAGE" ./teranode.run -all=0 -blockchain=1 >/dev/null
  sleep 12

  docker run -d --name propbench-propagation --network "$NET" \
    -p 18833:8833 -p 16060:6060 \
    -e SETTINGS_CONTEXT=docker.m \
    -e logLevel=INFO \
    -e blockchain_grpcAddress="propbench-blockchain:8087" \
    -e txstore="null:///" \
    -e useLocalValidator=false \
    -e validator_grpcAddress="127.0.0.1:8081" \
    -e kafka_validatortxsConfig="kafka://propbench-kafka:9092/validatortxs?partitions=8&replication=1&retention=60000&flush_bytes=1048576&flush_messages=10000&flush_frequency=10ms" \
    -e propagation_grpcListenAddress=":8084" \
    -e propagation_httpListenAddress=":8833" \
    -e propagation_httpRateLimit="$RATELIMIT" \
    -e propagation_batchConcurrencyLimit="$BATCH_CONCURRENCY" \
    -e propagation_httpBodyLimit=100MB \
    -e prometheusEndpoint=/metrics -e profilerAddr=":6060" \
    "$IMAGE" ./teranode.run -all=0 -propagation=1 >/dev/null
  sleep 12

  curl -fsS http://127.0.0.1:18833/health >/dev/null && echo "propbench up: http://127.0.0.1:18833/txs"
}

# Sum of validatortxs high-watermarks == transactions propagation accepted.
# count sums the topic's high watermarks. Uses --format json rather than
# column positions: rpk's table layout is not a stable interface, and a silent
# column shift would misreport throughput instead of failing.
count() {
  docker exec propbench-kafka rpk topic describe validatortxs -p --format json \
    | python3 -c '
import json, sys
doc = json.load(sys.stdin)
# rpk emits a LIST of sections; the partition section is the one whose
# "partitions" value is a list of per-partition dicts (the summary section
# reuses the same key for a plain count).
sections = doc if isinstance(doc, list) else [doc]
total = 0
for sec in sections:
    parts = sec.get("partitions")
    if isinstance(parts, list):
        total += sum(p["high_watermark"] for p in parts)
print(total)'
}

down() {
  docker rm -f propbench-kafka propbench-blockchain propbench-propagation >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  echo "propbench down"
}

case "${1:-up}" in
  up) up ;;
  count) count ;;
  down) down ;;
  *) echo "usage: $0 {up|count|down}" >&2; exit 2 ;;
esac

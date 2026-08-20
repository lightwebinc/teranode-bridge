# tnbench

The loopback rig behind the bridge's throughput and batch-contract numbers.
Two commands, run from this directory with `go run .`:

    go run . mock -listen 127.0.0.1:20833 [-faithful]
    go run . feed -addr 127.0.0.1:28725 -conns 24 -dur 30s [-chain]

Start the real bridge between them (lanes on loopback, `-propagation` at the
mock) and read rates off `teranode_bridge_lane_objects_total{lane="tx"}` deltas. Check
`teranode_bridge_lane_objects_rejected_total{lane="tx"}` stays at zero first: the lane
carries BRC-30 EF only, so a feed that emits standard-format transactions is
refused on arrival and every rate read off it measures the reject path.

## The ladder — what each rung proves, and what it does not

| Rig | Measured | Proves | Does NOT prove |
| --- | --- | --- | --- |
| `mock` (blind sink) | **1.48M tx/s** | the bridge is not the bottleneck: parse, hash, one copy, enqueue, batch, write | nothing about a cluster; no error paths are reached |
| `mock -faithful` | **447k tx/s** | the batch body is well-formed, delimits with the real codec, and every txid is computable — and the accounting/retry paths run under load | a ceiling: the mock itself hashes every tx, so this number is largely ITS cost |
| `mock -faithful` + `feed -chain` | **151k tx/s** | **the /txs parent-child contract holds under adversarial load**: 360k dependency seals, **zero** missing-parent errors from a mock that enforces the rule | throughput — a fully-chained stream is the pathological case; real traffic mixes |
| real Teranode, low rate | correctness end to end | the cluster accepts, validates and stores what the bridge sends | high-rate behaviour — the lab cannot run Teranode that hard |

The honest summary: rungs 1-3 bound and exercise **the bridge**; only a real
cluster bounds **the system**, and its propagation service has its own batch
worker pool that will bound it well below these numbers.

Those three rates were measured with the earlier 70-byte standard-format
template, before the tx lane became EF-only. The feed now emits 86-byte EF
transactions — 23% more bytes per transaction, and a longer input walk — so
treat the numbers as the shape of the ladder, not as figures to reproduce
rung-for-rung until they are re-measured.

## Notes

The crafted 86-byte transactions are BRC-30 extended format and codec-validated
(`objfmt.IsEF`, `objfmt.TxSize` == 86). If the template changes, re-validate
before trusting a run — a rig that emits subtly invalid transactions measures
the error path, not the happy path.

`-faithful` keeps a txid set, not a UTXO store: a real spend graph would make
the mock the bottleneck long before the bridge.

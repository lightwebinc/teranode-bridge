# tnbench

The loopback rig behind the bridge's throughput numbers (1.47M tx/s sustained,
2×18-core host). Two subcommands, run from this directory with `go run .`:

    go run . mock -listen 127.0.0.1:20833      # propagation stand-in: /tx, /txs, /health
    go run . feed -addr 127.0.0.1:28833 -conns 24 -dur 30s

Start the real bridge between them (lanes on loopback, `-propagation` at the
mock) and read the rate off `btb_lane_objects_total{lane="tx"}` deltas. The
crafted 70-byte transactions are codec-validated (objfmt.TxSize == 70); if the
template is ever changed, re-validate before trusting a run.

The mock accepts everything, so this measures the BRIDGE's ceiling, not the
cluster's: it proves the bridge is not the bottleneck, nothing more.

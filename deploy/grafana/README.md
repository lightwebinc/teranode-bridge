# Grafana

`teranode-bridge-dashboard.json` — import into Grafana (uid `teranode-bridge`),
or drop it into a provisioning directory alongside Teranode's own dashboards.

It expects a Prometheus data source and picks `$job` / `$instance` from
`teranode_bridge_build_info`, which is present from the first scrape.

Scope `$job` and `$instance` to **one cluster's bridges**. The "Submitters
active" tile sums `teranode_bridge_submitter_active`, an invariant that must
equal exactly 1 per cluster per class — a fleet-wide selection reports every
extra cluster as a double-submitter.

Panels are grouped so the first row answers "is anything wrong", and each
following row answers "where". Two panels have no counterpart anywhere else in
the stack:

- **Announce → first pull** is the only measurement of whether the announce-shim
  design is working. The cluster does not know when it was told; the fabric does
  not know when the cluster acted.
- **Freshness** turns a flat counter into an age. A stalled lane, a dead edge and
  a quiet network are the same unchanging number on every other panel.

Alert rules matching these panels ship in the Helm chart
(`metrics.prometheusRule.enabled=true`) and are listed in
[../../docs/references/prometheusMetrics.md](../../docs/references/prometheusMetrics.md#alerting).

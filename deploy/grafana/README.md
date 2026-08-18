# Grafana

`teranode-bridge-dashboard.json` — import into Grafana (uid `teranode-bridge`),
or drop it into a provisioning directory alongside Teranode's own dashboards.

It expects a Prometheus data source with **uid `prometheus`** — the
kube-prometheus-stack default, and the convention every dashboard in this fleet
uses. On import Grafana will let you remap it if yours differs.

## Requirements

- **teranode-bridge >= 0.6.0.** That release renamed the series from `btb_*` to
  `teranode_bridge_*`; the dashboard queries only the new names, so against a
  0.5.x bridge every panel reads "No data" while the target is perfectly up.
- A **`cluster` label** on the bridge targets. The "Submitters active" tile sums
  `teranode_bridge_submitter_active` **by cluster**, an invariant that must equal
  exactly 1 per cluster per class — without the label every bridge collapses into
  one bucket and a healthy fleet reads as a double-submitter. The Helm chart's
  ServiceMonitor does not add one; use `metrics.serviceMonitor.relabelings`, or
  stamp it in a static scrape config.

The `cluster` and `instance` pickers are sourced from
`up{job=~"teranode-bridge.*"}`, not from a bridge metric. `up` exists for a
scraped target whatever the process inside is exporting, so the pickers populate
across the metric rename and while a target is **down** — which is exactly when
someone opens this dashboard.

## Layout

The first row answers "is anything wrong"; each following row answers "where".
Two panels have no counterpart anywhere else in the stack:

- **Announce → first pull** is the only measurement of whether the announce-shim
  design is working. The cluster does not know when it was told; the fabric does
  not know when the cluster acted.
- **Freshness** turns a flat counter into an age. A stalled lane, a dead edge and
  a quiet network are the same unchanging number on every other panel.

Alert rules matching these panels ship in the Helm chart
(`metrics.prometheusRule.enabled=true`) and are listed in
[../../docs/references/prometheusMetrics.md](../../docs/references/prometheusMetrics.md#alerting).

## Fleet deployment

This file is byte-identical to
`1bsv-ops/deploy/charts/observability/dashboards/teranode-bridge.json`, which is
what the fleet actually installs — as a sidecar-watched ConfigMap on tiers
running kube-prometheus-stack, and by file provisioning on devnet's metrics VM.
Change both together.

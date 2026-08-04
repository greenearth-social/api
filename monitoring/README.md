# Load Test & Bottleneck Attribution dashboard

Cloud Monitoring dashboard, defined as code, that answers one question during a
load test or a regression: **which layer owns the latency?** It is laid out to be
read top-to-bottom against the attribution playbook below.

- Template: [`dashboards/bottleneck.json.tmpl`](dashboards/bottleneck.json.tmpl)
- Deploy: [`deploy.sh`](deploy.sh)
- Deployed dashboard ids (written by `deploy.sh`): `dashboards/ids.env`
- Design context: [issue #343](https://github.com/greenearth-social/api/issues/343)
  (attribution plan, metric inventory, and the 07-31 load-test timeline used as
  the worked example below)

## Deploying

```bash
./monitoring/deploy.sh stage --dry-run    # render + validate JSON, no gcloud calls
./monitoring/deploy.sh stage              # create or update the stage dashboard
./monitoring/deploy.sh prod               # create or update the prod dashboard
```

`deploy.sh` renders the template with `sed`, validates it with
`python3 -m json.tool`, then looks the dashboard up by `displayName` and either
`gcloud monitoring dashboards update`s or `create`s it. On success it prints the
console URL and writes the resource id to `dashboards/ids.env`:

```
DASHBOARD_ID_PROD=projects/<project-number>/dashboards/<uid>
DASHBOARD_ID_STAGE=projects/<project-number>/dashboards/<uid>
```

That file is committed so the load-test tooling can build a deep link into the
exact burst window without another API round trip.

### Template tokens

| Token | stage | prod | Used by |
|---|---|---|---|
| `${ENV}` | `stage` | `prod` | dashboard title, Cloud Run `service_name` (`greenearth-api-${ENV}`) |
| `${NAMESPACE}` | `stage` | `prod` | api/inference custom metrics — `generic_task` resource label `namespace` |
| `${CLUSTER}` | `greenearth-stage-cluster` | `greenearth-prod-cluster` | Elasticsearch Prometheus exporter queries |
| `${K8S_NAMESPACE}` | `greenearth-stage` | `greenearth-prod` | GKE page-cache PromQL (major faults, evictable memory) |

Only those four exact tokens are substituted, so Cloud Monitoring's own legend
syntax (`${metric.labels.traffic}`, `${resource.labels.task_id}`) passes through
untouched.

### Cluster-scope note

Every row is per-environment. **Rows 4 and 5 show the environment's own
Elasticsearch cluster** — each environment has one
(`greenearth-stage-cluster` / `greenearth-prod-cluster`), both reporting
exporter and GKE metrics (verified 2026-08-02), and the api's
`scripts/deploy.sh` wires each api environment to its matching cluster.
**Row 6 (`ingex/*`) uses the environment's own ingest pipeline**: series
exist for both `namespace=stage` and `namespace=prod`
(`resource.label."namespace"`), and every row-6 chart filters on
`${NAMESPACE}` so the stage dashboard shows stage ingest and the prod
dashboard shows prod ingest.

### Percentile aggregation

All `custom.googleapis.com/*` metrics (greenearth-api, greenearth-inference,
ingex) are `metricKind=CUMULATIVE, valueType=DISTRIBUTION`. The Monitoring
API rejects a `perSeriesAligner` of `ALIGN_PERCENTILE_*` on that combination
("The aligner cannot be applied to metrics with kind CUMULATIVE and value
type DISTRIBUTION"), so every percentile chart on those metrics uses
`perSeriesAligner: ALIGN_DELTA` with `crossSeriesReducer:
REDUCE_PERCENTILE_95` / `REDUCE_PERCENTILE_50` instead — the reducer
computes the percentile across the (possibly grouped) series. Cloud Run's
`cpu/utilizations` and `memory/utilizations` are the exception: they are
`DELTA DISTRIBUTION`, which the `ALIGN_PERCENTILE_95` per-series aligner
accepts directly, so those two charts are left as-is.

A percentile is also only as precise as the histogram's buckets — a value
inside a bucket is interpolated across that bucket's full width. The api and
inference services therefore set explicit boundaries per metric family
(`src/app/lib/metrics.py`, `histogram_boundaries`) rather than taking the
OTel defaults, which leave just four buckets above 1s. Every baseline
threshold below is an exact bucket edge in the relevant set, so the estimate
is precise at the value it is being compared against. Changing a threshold to
a value that is *not* a boundary reintroduces interpolation error at exactly
the point that matters; add the boundary alongside the threshold.

## Attribution playbook

Read a regression off the dashboard as a decision table. The 2026-07-31 load-test
bursts (timeline in [issue #343](https://github.com/greenearth-social/api/issues/343))
are the worked example.

| Pattern | Diagnosis | Where on the dashboard | 07-31 evidence |
|---|---|---|---|
| All stage p95s spike together, incl. external calls; `eventloop.lag_ms` ↑; ES `took` ≈ flat; instance count rising during burst | **api instance saturation / scale-up lag** | rows 1+2+3 | Burst 1: two_tower, followed_users, heavy_ranker, perspective all ~4.6 s from 1 instance; clean at 2–4 instances (burst 2) |
| `es.query.took_ms` ↑ + major faults/s ↑ + device read MB/s ↑ | **ES page-cache churn (cold reads)** | rows 4+5 | Mean search 5→38 ms with 2.5k faults/s, 78 MB/s reads |
| `es.query.took_ms` ↑ + search thread-pool queue/rejected ↑, faults flat | **ES CPU/concurrency ceiling** | row 4 | — (not yet observed) |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` ↑ | **inference-service capacity** | row 2 gap chart | — |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` flat | **api-side queuing to inference** | row 2 gap chart + row 3 client charts | — |
| `perspective` duration ↑ with 429s | **external rate limit** | row 2 | — |
| `es.query.duration − took` gap ↑ on every `op`, `took` flat, `eventloop.lag_ms` flat, CPU well below 100%; `es.client.in_flight` pinned at the pool cap | **ES client connection-pool starvation** (client-side queuing per dependency, not loop-wide) | row 4 gap chart + row 3 lag/CPU + row 3 in-flight chart | #344 cause 1: pool of 10 exhausted by kNN, trivial terms lookups queued to p50 ≈ 918 ms while ES `took` ≈ 10 ms, CPU ≈ 64 %. Pool now 100 (`GE_ES_CONNECTIONS_PER_NODE`) |
| Any dependency's client-side duration ↑ with its server-side signal flat; that client's `pool.wait_ms` ↑ or `in_flight` pinned at its cap | **client-side queuing for that dependency** — the general #344-class pattern (Perspective session, shared httpx pool, any capped parallel workflow) | row 3 queue-position + in-flight charts, paired with the matching row-2/row-4 backend series | #250: Perspective head-of-line blocking under `asyncio.gather` bursts (pre-dates these metrics) |
| Connection-class failures (`status_code=connection` / `error=connection`) spike on ≥2 dependencies at once; backend `took` / server latencies flat | **process-wide client/transport pathology** (event-loop or fd bookkeeping, e.g. uvloop fd race) | row 3 dependency-failure chart | #344 cause 2: `Bad file descriptor` storms hit Perspective, inference, and ES together during both 150 qpm windows; fixed by `--loop asyncio` |
| `freshness_sec` ↑ during serving load test | **serving load starving ingest (shared ES)** | row 6 | — |

The two #344 root causes are deliberately separable without ad-hoc queries: cause
1 is *one* dependency queuing client-side (its own queue-position chart) with a
flat backend; cause 2 is *several* dependencies failing on `connection` at the
same instant with all backends flat.

## Baseline threshold lines

These are **chart annotations, not alert policies**. They are eyeball baselines
derived from the 2026-07-29..31 measured data so a reader can tell "normal" from
"this is the regression" at a glance, and they are cheap to adjust.

**Reviewed 2026-08.** Re-review after any capacity change (instance sizing,
ES cluster shape, pool caps) or the next load-test campaign.

| Row / chart | Threshold | Source measurement |
|---|---|---|
| 1 — `feed.render` p50/p95 by traffic | **2500 ms** | Steady-state probe p95 1.6–2.2 s in the clean 07-31 burst 2; burst 1 pinned at the 10 s ceiling. 2500 ms sits just above healthy, well below broken. |
| 1 — Failures + degraded + 5xx per min | **1 / min** | Clean burst 2 ran ~0 failures, trace degraded, ≤0.5 5xx/min; burst 1 ran 2–3 failures + 15–18 degraded + 7–9 5xx per minute. |
| 3 — `eventloop.lag_ms` p95 per instance | **100 ms** | Healthy asyncio loop lags <10 ms; a saturated instance lags into the seconds. 100 ms is an order of magnitude above healthy and an order below saturated. No 07-31 data (metric postdates the test). |
| 3 — Dependency failures/min by class | **1 / min** | Dependency failures are ≈0 in steady state; any sustained non-zero class is a finding. Same basis as the row-1 failure line. |
| 3 — `client.pool.wait_ms` p95 | **10 ms** | A non-contended connector hands out a connection in well under 1 ms; 10 ms of queuing means the pool is the bottleneck for that client. |
| 3 — `client.in_flight` / `es.client.in_flight` p95 | **100** | The pool caps: `GE_HTTP_MAX_CONNECTIONS` (default 100, `lib/http_client.py`) for the shared httpx client, and `GE_ES_CONNECTIONS_PER_NODE` (default 100, introduced in PR #346) for the ES client. Either series flattening at its cap is the client-side starvation signal. |
| 4 — ES search thread pool rejected/s | **0** (any rejection) | Rejections are never normal; the line exists so a non-zero series is visually unambiguous. |
| 4 — ES mean search latency | **10 ms** | ~5 ms on a clean cluster, 17–38 ms during the 07-31 nightly cold-read storm. 10 ms separates the two regimes. |
| 6 — `ingex/freshness_sec` p95 | **600 s** | Matches the existing "Megastream/Jetstream P50 Lag SLA" alert policies (p50 > 600 s over 30 m, verified 2026-08-02). This chart plots p95, and p95 ≥ p50 always, so the p95 series crossing 600 s strictly leads the alert — an early-warning line consistent with the SLA rather than a second invented number. |

### Layout and serialization constraints

Settled against the live stage dashboard on 2026-08-03; all 26 tiles and 20
charts round-tripped. Each of these was a silent or fatal failure first time:

- **A `sectionHeader` with a subtitle must be height 1** in a 12-column
  layout — any other height is rejected outright at deploy
  (`INVALID_ARGUMENT`). The mosaic is stacked around height-1 headers, so
  inserting a row means re-stacking every `yPos` below it.
- **A `0`-valued threshold is dropped on the round trip.** proto3 omits
  default-valued scalars, so the row-4 rejected-count line silently vanished
  from the deployed dashboard. It is now `0.001` — below any real rejection
  rate (that series is a per-second rate over 5m, so one rejection is
  ~0.0033/s) and above a flat zero. Never set a threshold to `0`.
- **`instance_count` needs `${metric.labels.state}` in its legend.** The chart
  groups by `state`, and a static legend rendered active and idle as two
  indistinguishable series.
- **Updates need an etag.** Cloud Monitoring rejects an update whose config
  carries no `etag` (the optimistic-concurrency token). `deploy.sh` reads it
  from the deployed resource and splices it into the rendered JSON.

Still open: whether the `analyze.py` deep-link's `;startTime=…;endTime=…`
matrix-param format actually pre-sets the console's time range, rather than
landing on the default window. A stage dashboard now exists, so this is
checkable by running `analyze.py` against any results file and opening the
printed link.

## Chart inventory

Six rows, 20 charts, one section header per row.

| Row | Chart | Query type |
|---|---|---|
| 1 Load & UX | Renders/min by `traffic` | `feed.render.success_count` + `.failure_count`, ALIGN_DELTA/60 s, REDUCE_SUM by `traffic` |
| 1 | `feed.render` p50/p95 by `traffic` | ALIGN_PERCENTILE_50/95, REDUCE_MEAN by `traffic`, `timeshiftDuration: 86400s` |
| 1 | Failures + degraded + 5xx per min | `.failure_count` by `status_code`, `.degraded_count`, Cloud Run `request_count` 5xx |
| 2 Stages | `candidates.generate` p95 by `generator_name` | ALIGN_PERCENTILE_95 |
| 2 | `rank.model` p95 vs `inference.predict` p95 | two percentile series (api + inference metric prefixes) |
| 2 | `perspective.score` p95 + failures/min by `status_code` | percentile on Y1, counter delta on Y2 |
| 3 api saturation | `eventloop.lag_ms` p95 per instance | ALIGN_PERCENTILE_95, grouped by `resource.label.task_id` (no cross-instance mean) |
| 3 | Cloud Run instances / CPU / memory | `instance_count` on Y1, `cpu/utilizations` + `memory/utilizations` p95 on Y2 |
| 3 | Dependency failures/min by class | stacked `es.query.error_count` (by `error`), `perspective.score.failure_count`, `bsky.follows.failure_count`, `rank.model.failure_count` (by `status_code`) |
| 3 | Client queue position | `client.pool.wait_ms` p95 by `client` + `client.connect.duration_ms` p95 by `client` |
| 3 | Client in-flight vs caps | `client.in_flight` p95 by `host` + `es.client.in_flight` p95 by `op` |
| 4 ES | `es.query.duration_ms` vs `took_ms` p95 by `op` | two percentile series per `op` |
| 4 | Search thread-pool queue + rejected | PromQL `elasticsearch_thread_pool_queue_count` / `_rejected_count` |
| 4 | ES mean search latency | PromQL — mean search latency, data nodes |
| 5 Page cache | Major faults/s per pod | PromQL — major page faults/s (cache-miss rate) |
| 5 | Device read MB/s per node | PromQL — device read KB/s (cold-read throughput) |
| 5 | Evictable bytes per pod | PromQL — evictable container memory (page-cache size) |
| 6 Blast radius | `ingex/freshness_sec` p95 by source | percentile, grouped by `resource.label.job` (`jetstream-ingest` / `megastream-ingest`) |
| 6 | `ingex/es.bulk_index_*.took_ms` p95 | one percentile series per bulk op (posts, likes, inferences, tombstones, like_tombstones) |
| 6 | JVM GC/s + breakers tripped/s + PD IO latency | PromQL exporter series on Y1, `compute.googleapis.com` disk IO latency on Y2 |

Row 2's `rank.model` p95 vs `inference.predict` p95 gap chart pairs series across
repos by `model_name`: api's `rank.model.duration_ms` values `two_tower` /
`heavy_ranker` correspond to inference-service's `inference.predict.duration_ms`
values `user-tower` / `ranker` respectively — the two services don't share a
naming convention for the same model, so match the pairs by that mapping, not by
literal label equality, when reading the gap chart.

### Deliberate omissions

Two charts from the original design layout are intentionally left out of v1:
Cloud Run request concurrency (row 3) and PD IO queue depth (row 6). Neither
had a clear finding to hang off of during the 07-31 case study, so they were
cut to keep the dashboard to the charts that earned their place. Add them back
if a future regression raises an instance-level saturation question (row 3) or
a disk-queue-depth question (row 6) that the existing charts can't answer.

## Window comparison

Two mechanisms, no custom tooling:

- The `feed.render` p95 chart carries `timeshiftDuration: 86400s`, so it renders
  yesterday's same-hour curve alongside today's.
- For every other chart, use the Cloud Monitoring console's **compare to past**
  toggle in the time-range picker, or edit the PromQL charts to add `offset 1d`
  for a fixed window-vs-window read (pre/post-deploy, pre/post-migration).

## Granularity note

The api exports metrics every 60 s (`GE_METRICS_EXPORT_INTERVAL_SEC`), and every
chart aligns at 60 s. A burst shorter than ~3 minutes yields only 2–3 points —
prefer ≥3-minute bursts, or lower the export interval for a test session.

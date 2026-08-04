# API performance dashboard

The standing view of how the API is performing, and — when it is performing
badly — which layer owns the latency. It is laid out to be read top to bottom
against the attribution playbook below: user experience first, then the stage
that owns the time, then whether the cause is the api process, Elasticsearch,
or something downstream.

It is not a load-test-only tool. Load tests are simply the case where you know
in advance which window to look at; the same rows answer "why was the feed slow
at 09:00" on an ordinary day.

- Template: [`dashboards/bottleneck.json.tmpl`](dashboards/bottleneck.json.tmpl)
- Deploy: [`deploy.sh`](deploy.sh)
- Deployed dashboard ids (written by `deploy.sh`): `dashboards/ids.env`

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

### One dashboard per environment

`deploy.sh` deploys a separate dashboard for stage and prod rather than one
dashboard with an environment picker. A Cloud Monitoring dashboard filter binds
a single label key to a single value, and the four families of series here
identify their environment four different ways:

| Series | Label | stage value |
|---|---|---|
| api / inference custom metrics | `namespace` (resource) | `stage` |
| Cloud Run built-ins | `service_name` (resource) | `greenearth-api-stage` |
| Elasticsearch exporter | `cluster` (metric) | `greenearth-stage-cluster` |
| GKE container metrics | `namespace_name` (metric) | `greenearth-stage` |

No single filter value drives all four, and a dashboard filter cannot rewrite a
value into another format, so the environment is resolved at deploy time by
template substitution instead. Every row is per-environment, including ingest.

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

Read a regression off the dashboard as a decision table.

| Pattern | Diagnosis | Where on the dashboard |
| --- | --- | --- |
| All stage p95s spike together, incl. external calls; `eventloop.lag_ms` ↑; ES `took` ≈ flat; instance count rising during burst | **api instance saturation / scale-up lag** | rows 1+2+3 |
| `es.query.took_ms` ↑ + major faults/s ↑ + device read MB/s ↑ | **ES page-cache churn (cold reads)** | rows 4+5 |
| `es.query.took_ms` ↑ + search thread-pool queue/rejected ↑, faults flat | **ES CPU/concurrency ceiling** | row 4 |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` ↑ | **inference-service capacity** | row 2 gap chart |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` flat | **api-side queuing to inference** | row 2 gap chart + row 3 client charts |
| `perspective` duration ↑ with 429s | **external rate limit** | row 2 |
| `es.query.duration − took` gap ↑ on every `op`, `took` flat, `eventloop.lag_ms` flat, CPU well below 100%; `es.client.in_flight` pinned at the pool cap | **ES client connection-pool starvation** (client-side queuing per dependency, not loop-wide) | row 4 gap chart + row 3 lag/CPU + row 3 in-flight chart |
| Any dependency's client-side duration ↑ with its server-side signal flat; that client's `in_flight` pinned at its cap | **client-side queuing for that dependency** (any pooled client or capped parallel workflow) | row 3 in-flight chart, paired with the matching row-2/row-4 backend series |
| Connection-class failures (`status_code=connection` / `error=connection`) spike on ≥2 dependencies at once; backend `took` / server latencies flat | **process-wide client/transport pathology** (event-loop or fd bookkeeping, e.g. uvloop fd race) | row 3 dependency-failure chart |
| `freshness_sec` ↑ during serving load test | **serving load starving ingest (shared ES)** | row 6 |

The last two rows are deliberately separable: one dependency queuing
client-side against a flat backend is a pool problem for that dependency;
several dependencies failing on `connection` at the same instant, with every
backend flat, is a process-wide transport problem.
## Baseline threshold lines

These are **chart annotations, not alert policies** — reference lines that let
a reader tell "normal" from "this is the regression" at a glance. Re-review
them after any capacity change (instance sizing, ES cluster shape, pool caps).

| Row / chart | Threshold | Source measurement |
|---|---|---|
| 1 — `feed.render` p50/p95 by traffic | **2500 ms** | Healthy steady-state probe p95 is 1.6–2.2 s; a saturated instance pins at the 10 s client ceiling. 2,500 ms sits just above healthy and well below broken. |
| 1 — Failure, degradation and 5xx rate | **1%** | Expressed as a share of renders rather than a count per minute, so the goal is comparable between a quiet night and a load test. |
| 3 — `eventloop.lag_ms` p95 per instance | **100 ms** | A healthy asyncio loop lags <10 ms; a saturated instance lags into the seconds. 100 ms is an order of magnitude above healthy and an order below saturated. |
| 3 — Dependency failures/min by class | **1 / min** | Dependency failures are ≈0 in steady state; any sustained non-zero class is a finding. Same basis as the row-1 failure line. |
| 3 — `client.in_flight` / `es.client.in_flight` p95 | **100** | The pool caps: `GE_HTTP_MAX_CONNECTIONS` (default 100, `lib/http_client.py`) for the shared httpx client, and `GE_ES_CONNECTIONS_PER_NODE` (default 100, introduced in PR #346) for the ES client. Either series flattening at its cap is the client-side starvation signal. |
| 4 — ES search thread pool rejected/s | **0.001** (any rejection) | Rejections are never normal; the line exists so a non-zero series is visually unambiguous. |
| 4 — ES mean search latency | **10 ms** | ~5 ms with a warm page cache, 17–38 ms while the working set is being evicted. 10 ms separates the two regimes. |
| 6 — `ingex/freshness_sec` p95 | **600 s** | Matches the "Megastream/Jetstream P50 Lag SLA" alert policies (p50 > 600 s over 30 m). This chart plots p95, and p95 ≥ p50 always, so the p95 series crossing 600 s strictly leads the alert — an early-warning line consistent with the SLA rather than a second invented number. |

### Template constraints

Cloud Monitoring enforces these; each one fails silently or at deploy time:

- A `sectionHeader` carrying a subtitle must be **height 1** in a 12-column
  layout. The mosaic is stacked around height-1 headers, so inserting a row
  means re-stacking every `yPos` below it.
- A threshold **must not be `0`** — proto3 omits default-valued scalars, so a
  zero-valued threshold is dropped on write and the line silently disappears.
  Use a small positive value instead.
- A chart grouped by a label must name that label in its `legendTemplate`
  (`${metric.labels.<key>}`), or every group renders with the same legend.
- Updating a dashboard requires the current `etag`; `deploy.sh` reads it from
  the deployed resource and splices it into the rendered config.

## Chart inventory

Six rows, 20 charts, one section header per row.

p50 is charted alongside p95 only where the shape of the distribution is the
question — feed render latency and Perspective scoring. Everywhere else the
tail is the signal (a saturation threshold, a pool cap, a slow generator) and
a median line would only add series to read past.

| Row | Chart | Series | Query |
|---|---|---|---|
| 1 | Feed renders per minute by traffic class | 2 | PromQL |
| 1 | Feed render latency by traffic class | 2 | PromQL |
| 1 | Failure, degradation and 5xx rate (% of renders) | 3 | PromQL |
| 2 | Candidate generation latency by generator | 1 | PromQL |
| 2 | Ranking latency: api client-side vs inference server-side | 2 | PromQL |
| 2 | Perspective scoring latency and failures per minute | 3 | PromQL |
| 3 | Event-loop scheduling lag per instance | 1 | PromQL |
| 3 | Cloud Run instances, CPU and memory utilization | 3 | builder |
| 3 | Dependency failures per minute by cause | 4 | PromQL |
| 3 | Outbound connection setup time by client | 1 | PromQL |
| 3 | Concurrent outbound requests in flight vs pool cap | 2 | PromQL |
| 4 | Elasticsearch query latency: api client-side vs cluster-reported | 2 | PromQL |
| 4 | Elasticsearch search thread pool: queued and rejected | 2 | PromQL |
| 4 | Elasticsearch mean search latency, data nodes | 1 | PromQL |
| 5 | Major page faults per second per data node (cache misses) | 1 | PromQL |
| 5 | Disk read throughput per data node (cold reads) | 1 | PromQL |
| 5 | Page-cache memory per data node | 1 | PromQL |
| 6 | Ingest freshness by source | 1 | builder |
| 6 | Ingest bulk-index latency by index | 5 | builder |
| 6 | Cluster health: JVM GC, circuit breakers and disk latency | 3 | mixed |

Row 2's ranking gap chart pairs series across repos by `model_name`: api's
`rank.model.duration_ms` values `two_tower` / `heavy_ranker` correspond to
inference-service's `inference.predict.duration_ms` values `user-tower` /
`ranker` respectively — the two services don't share a naming convention for
the same model, so match the pairs by that mapping rather than by literal label
equality.

### Smoothing

Most latency and rate charts are PromQL with a **5-minute sliding window**
(`rate(...[5m])`). A sliding window averages five minutes of samples behind
every point while the points themselves stay at the chart's own step, so the
line smooths without losing temporal resolution — which simply widening the
alignment period cannot do. Five minutes is the compromise that keeps a
ten-minute load test legible: it shows a ramp and a plateau rather than one
blended average.

Percentiles are computed with `histogram_quantile` over the exported histogram
buckets, so the percentile is taken across merged buckets rather than by
averaging per-instance percentiles.

The charts still on the query builder (Cloud Run built-ins, ingest) align over
120s; distributions there are noisier at 60s, which is one export per bucket.

A percentile is only as stable as the number of samples behind it. At low
traffic a p95 is estimated from few requests and stays jumpy no matter the
window — widen the time range rather than reading minute-to-minute movement.

### Deliberate omissions

Cloud Run request concurrency and disk queue depth are not charted. Add them if
a regression raises an instance-level saturation or disk-queue question the
existing charts can't answer.

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

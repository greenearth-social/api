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
| `${CLUSTER}` | `greenearth-prod-cluster` | `greenearth-prod-cluster` | Elasticsearch Prometheus exporter queries |
| `${K8S_NAMESPACE}` | `greenearth-prod` | `greenearth-prod` | GKE page-cache PromQL (Appendix A queries 1 and 3) |

Only those four exact tokens are substituted, so Cloud Monitoring's own legend
syntax (`${metric.labels.traffic}`, `${resource.labels.task_id}`) passes through
untouched.

### Cluster-scope caveat (important when reading the stage dashboard)

**Rows 4, 5 and 6 always show the prod Elasticsearch cluster, on both
dashboards.** Stage api reads prod ES; there is no separate stage cluster, and
the GKE/ES exporter metrics only exist for `greenearth-prod-cluster` /
`greenearth-prod`. Only rows 1–3 (api custom metrics + Cloud Run built-ins)
differ between the two dashboards. So on the stage dashboard, row 4/5/6 movement
is *shared-cluster context*, not something stage caused — cross-check the prod
dashboard's rows 1–3 before blaming a stage load test for ES numbers.

Row 6 (`ingex/*`) is likewise a single ingest pipeline writing to that same
cluster, so it appears identically on both dashboards.

## Attribution playbook

Read a regression off the dashboard as a decision table (design doc §4.3). The
07-31 bursts are the worked example; Appendix B has the full case study.

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
| 1 — `feed.render` p50/p95 by traffic | **2500 ms** | Steady-state probe p95 1.6–2.2 s in the clean burst 2 (Appendix B); burst 1 pinned at the 10 s ceiling. 2500 ms sits just above healthy, well below broken. |
| 1 — Failures + degraded + 5xx per min | **1 / min** | Clean burst 2 ran ~0 failures, trace degraded, ≤0.5 5xx/min; burst 1 ran 2–3 failures + 15–18 degraded + 7–9 5xx per minute. |
| 3 — `eventloop.lag_ms` p95 per instance | **100 ms** | Healthy asyncio loop lags <10 ms; a saturated instance lags into the seconds. 100 ms is an order of magnitude above healthy and an order below saturated. No 07-31 data (metric postdates the test). |
| 3 — Dependency failures/min by class | **1 / min** | Dependency failures are ≈0 in steady state; any sustained non-zero class is a finding. Same basis as the row-1 failure line. |
| 3 — `client.pool.wait_ms` p95 | **10 ms** | A non-contended connector hands out a connection in well under 1 ms; 10 ms of queuing means the pool is the bottleneck for that client. |
| 3 — `client.in_flight` / `es.client.in_flight` p95 | **100** | The pool caps: `GE_HTTP_MAX_CONNECTIONS` (default 100, `lib/http_client.py`) for the shared httpx client, and `GE_ES_CONNECTIONS_PER_NODE` (default 100, introduced in PR #346) for the ES client. Either series flattening at its cap is the client-side starvation signal. |
| 4 — ES search thread pool rejected/s | **0** (any rejection) | Rejections are never normal; the line exists so a non-zero series is visually unambiguous. |
| 4 — ES mean search latency | **10 ms** | ~5 ms on a clean cluster, 17–38 ms during the 07-31 cold-read storm (Appendix A/B). 10 ms separates the two regimes. |
| 6 — `ingex/freshness_sec` p95 | **300 s** | Intended to mirror the existing prod freshness alert policy. **Not yet verified** — see note below. |

### Freshness policy check (outstanding)

The intent is for this line to mirror the existing prod freshness **alert
policy** rather than invent a second number. That lookup could not be completed
on this branch (the `gcloud` credentials in the working environment needed an
interactive re-login), so the design doc's fallback of **300 s** is committed.

Before relying on it, run:

```bash
gcloud alpha monitoring policies list --project greenearth-471522 --format json \
  | grep -B5 -A20 -i freshness
```

and, if the policy's `thresholdValue` differs, update the `300` in the row-6
`freshness_sec` chart's `thresholds` block and the table above.

### Post-deploy verification checklist

Deploying the template renders JSON that hasn't all been eyeballed against a
live dashboard yet. After `deploy.sh stage` / `deploy.sh prod`, confirm:

- The row-6 freshness threshold matches the prod alert policy (see the
  freshness policy check above).
- The zero-value threshold line on the row-4 rejected-count chart survives the
  proto3 round trip (a `0` threshold can serialize as an absent field).
- The four convention-derived series names (ES exporter thread-pool/GC/circuit
  breaker metrics, GCE disk IO latency) resolve to real time series rather than
  empty charts.
- The row-3 `instance_count` state-label legend distinguishes instance states
  rather than rendering one indistinguishable series.
- The `analyze.py` deep-link's `;startTime=…;endTime=…` matrix-param format
  actually pre-sets the console's time range when opened, rather than landing
  on the default window.

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
| 4 | ES mean search latency | PromQL, Appendix A query 4 |
| 5 Page cache | Major faults/s per pod | PromQL, Appendix A query 1 |
| 5 | Device read MB/s per node | PromQL, Appendix A query 2 |
| 5 | Evictable bytes per pod | PromQL, Appendix A query 3 |
| 6 Blast radius | `ingex/freshness_sec` p95 by source | percentile, grouped by `resource.label.job` (`jetstream_ingest` / `megastream_ingest`) |
| 6 | `ingex/es.bulk_index_*.took_ms` p95 | one percentile series per bulk op (posts, likes, inferences, tombstones, like_tombstones) |
| 6 | JVM GC/s + breakers tripped/s + PD IO latency | PromQL exporter series on Y1, `compute.googleapis.com` disk IO latency on Y2 |

Row 2's `rank.model` p95 vs `inference.predict` p95 gap chart pairs series across
repos by `model_name`: api's `rank.model.duration_ms` values `two_tower` /
`heavy_ranker` correspond to inference-service's `inference.predict.duration_ms`
values `user-tower` / `ranker` respectively — the two services don't share a
naming convention for the same model, so match the pairs by that mapping, not by
literal label equality, when reading the gap chart.

### Deliberate omissions

Two charts from the design doc's §4.2 layout are intentionally left out of v1:
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

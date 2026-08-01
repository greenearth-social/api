# Load-Test Regression Assessment via Cloud Monitoring

**Status:** v4 (decisions resolved) · 2026-08-01
**Context:** api#189 (simulated user load testing), api#312 (ES page-cache churn), PR api#338 (feeds datastore redesign — defines the query patterns under test)
**Scope:** How we measure performance/system regressions — during load tests and continuously — backed by metrics queried from GCP Cloud Monitoring. Load generation itself (`scripts/load_test/run.py`) is out of scope; it exists and works.

---

## 1. Summary

The load-test tooling (select → run → analyze → cleanup) exists, and every api
metric already carries a `traffic` label (`real` | `probe` | `load_test`), so
test traffic is cleanly separable in Monitoring. What's missing is the
*assessment* half: the metrics that would attribute a bottleneck don't exist
yet, and the metrics that do exist aren't arranged anywhere a human can read
a burst against a baseline.

The 2026-07-31 load-test bursts (02:30 and 02:54 UTC) sharpened the
requirements. Burst 1 saturated a single Cloud Run instance: every pipeline
stage's p95 — including calls to *external* services — spiked in lockstep,
probes hit the 10s ceiling, and real serving degraded. That diagnosis
("api-side saturation, not ES") was only reachable by human inference across
five separate ad-hoc queries. Two capabilities are therefore explicit design
goals:

1. **Correlate** infrastructure signals (page-cache churn, memory, I/O,
   GC, thread-pool pressure) with user-experience signals (latency,
   failures, degradation) over the same windows.
2. **Disambiguate the bottleneck**: api service (instance/event-loop
   saturation, external-call queuing) vs backend (ES server-side work, cold
   reads, search-queue pressure) vs downstream (inference-service,
   Perspective) — from metrics alone, without replaying logs.

**Approach: instrument, then read from a dashboard.** Cloud Monitoring
retains these metrics for 24 months, so any prior window is permanently
queryable — comparison against "before" needs no snapshot machinery, just
native compare-to-past overlays and PromQL `offset`. Assessment is a human
reading a purpose-built dashboard with the attribution playbook (§4.3);
there is no scorecard, snapshot, or baseline-diff tooling.

| Step | Ships | Outcome |
|---|---|---|
| 1 | Attribution instrumentation (small PRs) | Client-vs-server split on every dependency, event-loop lag, external-call outcomes |
| 2 | Bottleneck dashboard, as code in this repo | UX + infra families on one time axis, organized by playbook row; window-vs-window comparison built in |
| 3 | `analyze.py` deep-link + client-side report (trim) | One click from a run to its exact window on the dashboard |

Alert policies are deferred (see §4.5): the dashboard's key charts instead
carry **baseline threshold lines** marking current known-good levels, so a
regression is visible at a glance without alerting infrastructure.

Step 1 requires deploys and should land **before** the next big load test —
without it, "two_tower got slow" cannot distinguish inference time from ES
kNN time from event-loop starvation.

---

## 2. What Cloud Monitoring captures today (reviewed 2026-07-30)

### api — `custom.googleapis.com/greenearth-api/*`

Resource `generic_task`, `namespace` = `stage`|`prod`. All metrics carry
`endpoint` and `traffic` labels via ContextVars (`lib/request_context.py`).
Export interval 60s (`GE_METRICS_EXPORT_INTERVAL_SEC`).

| Area | Metrics | Extra labels |
|---|---|---|
| Feed serving | `feed.render.duration_ms` (dist), `.success_count`, `.failure_count`, `.degraded_count` | `feed_name`, `status_code` |
| Candidates | `candidates.generate.duration_ms`, `.success_count`, `.failure_count`, `.retrieved_share` | `generator_name`, `is_infill`, `outcome` |
| Ranking | `rank.model.duration_ms`, `rank.predict.dropped_candidates_count` | `model_name` |
| Slate/quality | `feed.slate.cutoff_count`, `.empty_after_cutoff_count`, `.exclusion_size`, `.kept_share`, `feed.mean_similarity_score` | `reason`, `batch` |
| Perspective | `perspective.score.duration_ms` | — |

### ingex — `custom.googleapis.com/ingex/*`

Per-operation ES write latencies with both client-side `duration_ms` **and**
server-side `took_ms` (the pattern §4.1 adopts for the api), `error_count`s,
`freshness_sec` (jetstream + megastream), expiry/extract run stats,
`inference.request.*`.

### Elasticsearch — Prometheus `elasticsearch_*` exporter (140 metrics)

Cluster health, per-node CPU/JVM/disk, query/request-cache counters,
search/fetch time totals, **filesystem io_stats device read/write counters**,
**thread-pool queue/rejected counts**, breakers, indexing pressure, GC.

### Platform built-ins

Cloud Run (`run.googleapis.com/*`): request count/latencies by response
code, instance count, CPU/memory utilization — api and inference-service.
GKE (`kubernetes.io/*`): container memory by type (**evictable = page
cache**), **major/minor page-fault counters**, node memory. GCE
(`compute.googleapis.com/*`): PD read/write bytes/ops, IO latency and queue
depth per device.

### Alert policies

Freshness (prod), ES disk usage, GKE bulletins. **Nothing** on serving-path
latency, error rate, or degradation.

---

## 3. Gaps

1. **No bottleneck disambiguation.** Client-side stage timings exist, but
   nothing separates "time ES spent working" from "time the api spent
   waiting to run" — the 07-31 burst-1 pattern (all stages spike together)
   had to be diagnosed by inference. No event-loop lag signal, no ES `took`
   metric, no server-side inference latency.
2. **Infra↔UX correlation is manual.** The infra proxies exist (Appendix A)
   but live in different metric families with no shared labels; nothing
   places them on one time axis next to UX metrics.
3. **The five workload rows of #338 §2 are log-only** — api ES call sites
   use `timed()` without `record_metric=True`.
4. **inference-service is a blind spot** beyond Cloud Run built-ins.
5. **No ingest blast-radius view** during load tests (`freshness_sec`,
   `es.bulk_*.took_ms` are the canaries).

---

## 4. Design

### 4.1 Attribution instrumentation (goal 2 — small PRs, deploy first)

The principle: **every dependency call gets a client-side duration and a
server-side duration; the gap is queuing/scheduling on our side.** ingex
already does this for writes (`duration_ms` vs `took_ms`); the api adopts it
for reads.

- **`es.query.duration_ms` + `es.query.took_ms`** — recorded in the
  `es_client.py` search wrapper (the single choke point; it already measures
  elapsed and drops the response's `took`). Label `op` (`likes`, `hydrate`,
  `knn`, `popularity`, `author_scan`) supplied **explicitly by callers** at
  each call site (decided over index-name inference in the wrapper —
  explicit beats murky for multi-index queries); instantiates
  #338 §2's workload table as time series and gives the Memorystore
  shadow-mode comparison its baseline.
  *Read:* `took` ↑ ⇒ ES is slow. `duration − took` ↑ with flat `took` ⇒ the
  api is slow (event loop, connection pool, network).
- **`eventloop.lag_ms`** — background task measures asyncio scheduling
  overshoot (e.g. target 100ms sleep, record overshoot) per instance. The
  direct "api instance saturated" signal that would have named burst 1
  immediately. Near-zero cost.
- **inference-service metrics** — port api's `lib/metrics.py`;
  `inference.predict.duration_ms` by model. Pairs with the api's
  client-side `rank.model.duration_ms` for the same gap analysis.
- **External-call outcomes** — `perspective.score.failure_count` with
  `status_code` (429 = rate limit vs timeouts), same for Bluesky
  `get_follows`. Distinguishes "they throttled us" from "we couldn't
  schedule the response."

### 4.2 The bottleneck dashboard (goal 1 — dashboard as code)

One dashboard per environment ("Load Test & Bottleneck Attribution"),
defined as JSON **in this repo** (`monitoring/dashboards/bottleneck.json`;
decided over a separate monitoring repo — the queries reference api metric
names) and deployed via `gcloud monitoring dashboards update` — the
queries are versioned and reviewable, which is what the abandoned scorecard
was really for. Layout mirrors the playbook so a human reads top-to-bottom:

1. **Load & UX row** — renders/min by `traffic`; `feed.render` p50/p95 by
   `traffic` (probe = "are real users hurting" canary, validated 07-31);
   failure/degraded/5xx rates.
2. **Stage row** — `candidates.generate` p95 by generator; `rank.model`
   p95 by model paired with `inference.predict` p95 (gap chart);
   `perspective` p95 + outcome codes.
3. **api saturation row** — `eventloop.lag_ms` p95 per instance; Cloud Run
   instance count, CPU/mem utilization, concurrency.
4. **ES row** — `es.query.took_ms` vs `duration_ms` by `op` (gap chart);
   search thread-pool queue/rejected; mean search latency.
5. **Page-cache row** — Appendix A queries: major faults/s, device read
   MB/s, evictable GB per pod.
6. **Blast-radius row** — `freshness_sec` p95, `es.bulk_index_*.took_ms`
   p95, JVM GC rate, breakers, PD IO latency/queue.

**Window comparison**, two mechanisms, no custom tooling:

- Cloud Monitoring's native **compare-to-past** overlay (yesterday / last
  week) for "is this burst worse than the same hour before."
- **PromQL `offset`** variants on key charts for fixed comparisons (e.g.
  p95 now vs `offset 1d`), for arbitrary window-vs-window questions —
  including pre/post-deploy and pre/post-Memorystore-migration reads.

Granularity note: 60s export interval means bursts shorter than ~3 min
yield 2–3 points; prefer ≥3-min bursts, or lower
`GE_METRICS_EXPORT_INTERVAL_SEC` for a test session.

### 4.3 Attribution playbook

Read regressions off the dashboard as a decision table. Patterns, with the
07-31 bursts as the worked example:

| Pattern | Diagnosis | 07-31 evidence |
|---|---|---|
| All stage p95s spike together, incl. external calls; `eventloop.lag_ms` ↑; ES `took` ≈ flat; instance count rising during burst | **api instance saturation / scale-up lag** | Burst 1: two_tower, followed_users, heavy_ranker, perspective all ~4.6s from 1 instance; clean at 2–4 instances (burst 2) |
| `es.query.took_ms` ↑ + major faults/s ↑ + device read MB/s ↑ | **ES page-cache churn (cold reads)** | Mean search 5→38ms with 2.5k faults/s, 78 MB/s reads |
| `es.query.took_ms` ↑ + search thread-pool queue/rejected ↑, faults flat | **ES CPU/concurrency ceiling** | — (not yet observed) |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` ↑ | **inference-service capacity** | — |
| `rank.model.duration_ms` ↑ with `inference.predict.duration_ms` flat | **api-side queuing to inference** | — |
| `perspective` duration ↑ with 429s | **external rate limit** | — |
| `freshness_sec` ↑ during serving load test | **serving load starving ingest (shared ES)** | — |

The playbook lives next to the dashboard config and in the load-test README.

### 4.4 `analyze.py`: trim to what the dashboard can't do

- Keep the client-side JSONL report (latency by feed × phase × cohort) —
  that data never reaches Cloud Monitoring.
- Replace the server-side metrics section with a **dashboard deep-link**:
  the run JSONL knows the exact burst window, so print a URL with the time
  range pre-set. One click from "what I ran" to "what the system did."
- Drop the planned snapshot/baseline machinery entirely. If #189's
  automation story later needs mechanical pass/fail, a thin checker can
  query the same PromQL the dashboard config already contains.

### 4.5 Continuous assessment between tests

**Decision: no alert policies for now.** Instead, key dashboard charts
carry **baseline threshold lines** (Cloud Monitoring chart thresholds)
marking the current known-good level, so any drift past today's baseline is
visible at a glance. Charts that get a threshold line, with the baseline
taken from prod steady-state readings (clean windows, outside load tests):

- `feed.render` p95 (real + probe traffic) — the primary "don't regress"
  line.
- `feed.render` failure rate and degraded rate.
- `freshness_sec` p95 (ingest blast radius).
- ES mean search latency (Appendix A query 4).
- `eventloop.lag_ms` p95 and `es.query.took_ms` p95 by `op` — lines added
  once the new metrics have a week of prod data to baseline against.

Exact values are read off prod steady-state when the dashboard is built
(retroactive data is available) and recorded alongside the dashboard JSON.
Alert policies can be layered on later using the same queries; the
threshold lines make the eventual alert thresholds an informed choice
rather than a guess.

---

## 5. Sequencing and validation

1. Step 1 PRs (instrumentation) — validate metric arrival on stage; deploy
   to prod **before** the next load-test ladder.
2. Step 2 dashboard — build against the 07-31 burst windows retroactively
   (data is retained): the dashboard is correct when a reader can reach
   Appendix B's diagnosis from the charts alone.
3. Step 3 `analyze.py` trim + deep-link.
4. Rerun the #189 ladder (1–5×) in prod **outside the 00:00–03:00 UTC
   cold-read storm window** (or bracket it deliberately to measure the
   storm's contribution), with min-instances pre-warmed for at least one
   run to separate scale-up lag from steady-state capacity.

**Resolved decisions (2026-08-01):**

- `op` label for `es.query.*`: **explicit at each call site** (not inferred
  from index names in the wrapper).
- Dashboard JSON lives **in this repo** (`monitoring/dashboards/`).
- **Alerts omitted for now**; baseline threshold lines on dashboard charts
  (§4.5) carry the "don't regress" duty instead.
- The **nightly cold-read storm** (Appendix A) is out of scope for this
  work. If the dashboards surface it, it gets its own investigation.

**Remaining open question (out of scope here):**

- Whether `eventloop.lag_ms` should also gate degradation (shed load when
  saturated) — the metric enables it.

---

## Appendix A — Page-cache residency proxies (validated 2026-07-30)

ES mmaps Lucene files, so a read that misses the page cache surfaces as a
**major page fault** in the container cgroup and as a **device read** in ES's
own io_stats. Both are already collected; at hourly grain over 48h of prod
data they agree at Spearman 0.94. Cloud Monitoring PromQL
(`/v1/projects/greenearth-471522/location/global/prometheus/api/v1/*`):

```promql
# 1. Cache-miss rate (major faults/s) per ES data pod — the primary signal
sum by (pod_name)(rate(kubernetes_io:container_memory_page_fault_count{
  monitored_resource="k8s_container", namespace_name="greenearth-prod",
  fault_type="major"}[30m]))

# 2. Cold-read throughput (MB/s) per node — cross-check, ES-reported
sum by (name)(rate(elasticsearch_filesystem_io_stats_device_read_size_kilobytes_sum{
  cluster="greenearth-prod-cluster"}[30m])) / 1024

# 3. Page-cache size (GB) per ES data pod — the residency budget
sum by (pod_name)(kubernetes_io:container_memory_used_bytes{
  monitored_resource="k8s_container", namespace_name="greenearth-prod",
  memory_type="evictable"})

# 4. Mean search latency (ms), data nodes — dilute; use for trend only,
#    the cold-read damage lives in the tail (slow_es_query logs)
1000 * sum(rate(elasticsearch_indices_search_query_time_seconds{
  cluster="greenearth-prod-cluster", es_data_node="true"}[30m]))
     / sum(rate(elasticsearch_indices_search_query_total{
  cluster="greenearth-prod-cluster", es_data_node="true"}[30m]))
```

Measured facts (prod, 2026-07-29/31):

- Page cache per data node swings **2–40 GB** within 48h (near-total
  eviction events occur) against a **~1.35 TB** store per node — resident
  fraction ≈ 1–3 %. This is #312's mechanism, now visible as a time series.
- A **nightly cold-read storm at 00:00–03:00 UTC** (40–50 MB/s sustained
  across all 4 data nodes, major-fault peaks 1–2.3k/s) is **not** explained
  by ingex extract (runs all day, peaks 09:00–15:00 UTC) or expiry (zero
  deletes in window). Candidates: ES snapshots, ILM rollover, merges.
  Follow-up owed; it evicts the working set every night, and the 07-31 load
  tests ran inside it (ES numbers pessimistic).
- Proxy↔impact correlation is intentionally loose (hourly Pearson ~0.2–0.3
  vs `slow_es_query` counts): misses only hurt when queries arrive during a
  miss window. Assess **miss rate** and **user impact** as separate
  dashboard rows; the playbook (§4.3) joins them.

Limits: these are miss counters, not hit-rate (reads served from cache are
invisible), and misses can't be attributed per index/query. Per-query
warm/cold attribution still needs `slow_es_query` logs +
`profile_es_queries.py`.

## Appendix B — 2026-07-31 load-test case study (attribution ground truth)

Bursts at 02:30–02:36 and 02:54–03:04 UTC, ~50–120 renders/min vs ~3/min
overnight baseline; run inside the nightly cold-read storm.

| Signal | Burst 1 (1→2 instances) | Burst 2 (2→4 instances) |
|---|---|---|
| feed.render p95 (load_test) | 10s ceiling | ~4.2s |
| feed.render p95 (probe) | 10s ceiling — **real serving degraded** | 1.6–2.2s — unaffected |
| Failures / degraded / 5xx per min | 2–3 / 15–18 / 7–9 | ~0 / trace / ≤0.5 |
| Stage p95s | **All** ~4.2–4.7s simultaneously (incl. perspective, heavy_ranker) | two_tower *improved* to 1–2.2s (cache warmed) |
| ES mean search / faults / reads | 38ms / 2.5k/s / 78 MB/s | 17–26ms / 1.5k/s / 58 MB/s |

Diagnosis: burst 1 = api instance saturation + autoscaler lag (cold-scale
artifact), not steady-state capacity; burst 2 shows the same load absorbed
cleanly with instances warm. This is the worked example behind §4.3's first
two playbook rows and the pre-warm guidance in §5.

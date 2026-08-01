# Load-Test Regression Assessment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement api#343 — attribution instrumentation (ES client/server split, event-loop lag, external-call outcomes, inference-service metrics), a bottleneck-attribution dashboard as code with baseline threshold lines, and an `analyze.py` trim with a dashboard deep-link.

**Architecture:** Per `docs/design/load-test-regression-assessment.md` (v4). Every dependency call gets a client-side and a server-side duration; the gap is queuing on our side. Metrics flow through the existing `MetricCollector` (OTel → Cloud Monitoring). The dashboard is JSON checked into `monitoring/dashboards/`, deployed via `gcloud monitoring dashboards`. No alerts; key charts carry baseline threshold lines.

**Tech Stack:** Python 3.13, FastAPI, pytest, OpenTelemetry + Cloud Monitoring exporter, Cloud Monitoring dashboard JSON (MQL/PromQL), gcloud CLI.

## Global Constraints

- All env vars prefixed `GE_` (workspace convention).
- Metric names follow existing dotted style ending in `_ms` (histogram) or `_count` (counter) — the suffix selects the OTel instrument type in `MetricCollector.record` (`src/app/lib/metrics.py:128`).
- Metric labels must be low-cardinality (no URIs, DIDs, counts).
- `op` label values for `es.query.*`: **explicit at each call site** (user decision 2026-08-01). Taxonomy: `likes`, `hydrate`, `knn`, `popularity`, `author_scan`, `random`, `search`. Wrapper defaults to `unlabeled` so missed call sites are visible in Monitoring rather than silent.
- Dashboard JSON lives in this repo under `monitoring/dashboards/` (user decision).
- **No alert policies** — baseline threshold lines on charts instead (user decision).
- Nightly cold-read storm: out of scope entirely (user decision).
- Tests: `pipenv run pytest src/... -q` from the worktree root. Test files sit next to sources (`foo.py` / `foo_test.py`) — follow that convention.
- Commits: single-line messages per user's format. Small PRs — task groups below map to PRs (A–F).
- inference-service tasks (Task 5) happen in the separate `/Users/max/Projects/greenearth/inference-service` repo, not this worktree.

## File Structure

| File | Responsibility |
|---|---|
| `src/app/lib/es_client.py` (modify) | Record `es.query.duration_ms` + `es.query.took_ms` with `op` label in the search wrapper; keep slow-query logging |
| `src/app/lib/elasticsearch.py`, `lib/candidates/{es_candidates,popularity,followed_users,random_posts,network_likes}.py`, `routers/skylight.py` (modify) | Pass explicit `op=` at each `es.search` call site |
| `src/app/lib/eventloop_monitor.py` (create) | Background task measuring asyncio scheduling overshoot → `eventloop.lag_ms` |
| `src/app/main.py` (modify) | Start/stop the event-loop monitor in lifespan |
| `src/app/lib/perspective.py` (modify) | `perspective.score.failure_count` with `status_code` label |
| `src/app/lib/bsky.py` (modify) | `bsky.follows.failure_count` with `status_code` label |
| `inference-service/metrics.py` (create, other repo) | Trimmed port of api `MetricCollector` (prefix `greenearth-inference`) |
| `inference-service/app.py` (modify, other repo) | Collector in lifespan; `inference.predict.duration_ms` by `model_name` |
| `monitoring/dashboards/bottleneck.json.tmpl` (create) | Dashboard template (env-parameterized) with threshold lines |
| `monitoring/deploy.sh` (create) | Render template per env and create/update the dashboard via gcloud |
| `monitoring/README.md` (create) | Attribution playbook (§4.3 of design doc) + baseline threshold values table |
| `scripts/load_test/analyze.py`, `lib.py` (modify) | Drop server-metrics/logs sections; print dashboard deep-link for the run window |

---

### Task 1: `es.query.duration_ms` + `es.query.took_ms` in the ES search wrapper (PR A)

**Files:**
- Modify: `src/app/lib/es_client.py`
- Test: `src/app/lib/es_client_test.py`

**Interfaces:**
- Consumes: `MetricCollector` via `from .metrics import get_metric_collector` (may be `None`); existing `SlowQueryLoggingES.search`.
- Produces: `SlowQueryLoggingES.search(*args, op: str = "unlabeled", **kwargs)` — `op` is consumed by the wrapper (never forwarded to ES). Metrics `es.query.duration_ms` and `es.query.took_ms` (histograms), label `op`. Task 2 relies on the `op=` kwarg existing.

- [ ] **Step 1: Write the failing tests**

Add to `src/app/lib/es_client_test.py` (follow the file's existing fake-client pattern; adapt names if they collide):

```python
class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.mark.asyncio
async def test_search_records_duration_and_took_metrics(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            return {"took": 42, "hits": {"hits": []}}

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client, "get_metric_collector", lambda: collector)

    wrapped = es_client.SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="likes", op="likes")

    names = {name: attrs for name, _, attrs in collector.records}
    assert names["es.query.duration_ms"] == {"op": "likes"}
    assert names["es.query.took_ms"] == {"op": "likes"}
    took = [v for n, v, _ in collector.records if n == "es.query.took_ms"]
    assert took == [42]


@pytest.mark.asyncio
async def test_search_does_not_forward_op_to_client(monkeypatch):
    seen_kwargs = {}

    class _FakeES:
        async def search(self, **kwargs):
            seen_kwargs.update(kwargs)
            return {"took": 1}

    monkeypatch.setattr(es_client, "get_metric_collector", lambda: None)
    wrapped = es_client.SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="posts", op="hydrate")
    assert "op" not in seen_kwargs


@pytest.mark.asyncio
async def test_search_defaults_op_to_unlabeled_and_tolerates_missing_took(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            return {"hits": {"hits": []}}  # no "took" key

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client, "get_metric_collector", lambda: collector)

    wrapped = es_client.SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="posts")

    by_name = {n: attrs for n, _, attrs in collector.records}
    assert by_name["es.query.duration_ms"] == {"op": "unlabeled"}
    assert "es.query.took_ms" not in by_name


@pytest.mark.asyncio
async def test_search_records_duration_on_timeout(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            raise ConnectionTimeout("boom")

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client, "get_metric_collector", lambda: collector)

    wrapped = es_client.SlowQueryLoggingES(_FakeES())
    with pytest.raises(ConnectionTimeout):
        await wrapped.search(index="posts", op="knn")

    names = [n for n, _, _ in collector.records]
    assert "es.query.duration_ms" in names
    assert "es.query.took_ms" not in names
```

Notes for the implementer: import `es_client` as a module (`from app.lib import es_client`) so `monkeypatch.setattr(es_client, "get_metric_collector", ...)` patches the name the implementation looks up. The implementation must therefore import `get_metric_collector` lazily or reference it as a module attribute (see Step 3). `ObjectApiResponse` responses expose `.body`; use `unwrap`-style access — see Step 3 for the exact helper.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pipenv run pytest src/app/lib/es_client_test.py -q`
Expected: new tests FAIL (unexpected keyword `op` / missing metric records); pre-existing tests PASS.

- [ ] **Step 3: Implement in `es_client.py`**

Replace the `search` method (keep `_log_timeout_search` / `_log_slow_search` untouched):

```python
def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def _extract_took(resp) -> float | None:
    body = getattr(resp, "body", resp)
    if isinstance(body, dict):
        took = body.get("took")
        if isinstance(took, (int, float)):
            return float(took)
    return None


def _record_query_metrics(op: str, elapsed_ms: float, took_ms: float | None) -> None:
    collector = get_metric_collector()
    if collector is None:
        return
    collector.record("es.query.duration_ms", elapsed_ms, op=op)
    if took_ms is not None:
        collector.record("es.query.took_ms", took_ms, op=op)


class SlowQueryLoggingES:
    ...
    async def search(self, *args, op: str = "unlabeled", **kwargs):
        start = time.monotonic()
        timed_out = False
        took_ms: float | None = None
        try:
            resp = await self._wrapped.search(*args, **kwargs)
            took_ms = _extract_took(resp)
            return resp
        except ConnectionTimeout:
            timed_out = True
            elapsed_ms = (time.monotonic() - start) * 1000
            _log_timeout_search(elapsed_ms, args, kwargs)
            _record_query_metrics(op, elapsed_ms, None)
            raise
        finally:
            if not timed_out:
                elapsed_ms = (time.monotonic() - start) * 1000
                _record_query_metrics(op, elapsed_ms, took_ms)
                if elapsed_ms >= _slow_threshold_ms():
                    _log_slow_search(elapsed_ms, args, kwargs)
```

Update the module docstring to mention the metrics. Note `record()` auto-attaches `endpoint`/`traffic` labels from ContextVars — nothing to do here.

- [ ] **Step 4: Run tests**

Run: `pipenv run pytest src/app/lib/es_client_test.py src/app/lib/metrics_test.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/app/lib/es_client.py src/app/lib/es_client_test.py
git commit -m "record es.query.duration_ms and es.query.took_ms with op label in ES search wrapper"
```

---

### Task 2: Explicit `op` labels at every ES call site (PR A)

**Files:**
- Modify: `src/app/lib/elasticsearch.py` (4 call sites), `src/app/lib/candidates/es_candidates.py`, `src/app/lib/candidates/popularity.py`, `src/app/lib/candidates/followed_users.py`, `src/app/lib/candidates/random_posts.py`, `src/app/lib/candidates/network_likes.py` (2 call sites), `src/app/routers/skylight.py`
- Test: `src/app/lib/elasticsearch_test.py` (spot-check; other files' existing fakes must tolerate the kwarg)

**Interfaces:**
- Consumes: `op: str` kwarg on `SlowQueryLoggingES.search` from Task 1.
- Produces: every production `es.search(` call carries an explicit `op=`.

Op assignment (from the design doc taxonomy):

| Call site | `op` |
|---|---|
| `elasticsearch.py:106` `fetch_recent_liked_post_uris` | `likes` |
| `elasticsearch.py:170` `fetch_recent_liked_post_uris_and_times` | `likes` |
| `elasticsearch.py:218` `fetch_post_embeddings` | `hydrate` |
| `elasticsearch.py:279` `fetch_post_embeddings_and_metadata` | `hydrate` |
| `candidates/es_candidates.py:78` `knn_search_posts` | `knn` |
| `candidates/popularity.py:120` | `popularity` |
| `candidates/followed_users.py:79` | `author_scan` |
| `candidates/random_posts.py:43` | `random` |
| `candidates/network_likes.py:74` (likes page) | `likes` |
| `candidates/network_likes.py:131` (posts by at_uri) | `hydrate` |
| `routers/skylight.py:106` | `search` |

- [ ] **Step 1: Write the failing test**

Add to `src/app/lib/elasticsearch_test.py` (reuse the file's existing fake-ES pattern for the other assertions if simpler):

```python
@pytest.mark.asyncio
async def test_fetch_recent_liked_post_uris_passes_likes_op():
    captured = {}

    class _FakeES:
        async def search(self, **kwargs):
            captured.update(kwargs)
            return {"took": 1, "hits": {"hits": []}}

    await elasticsearch.fetch_recent_liked_post_uris(_FakeES(), "did:plc:x")
    assert captured["op"] == "likes"


@pytest.mark.asyncio
async def test_fetch_post_embeddings_passes_hydrate_op():
    captured = {}

    class _FakeES:
        async def search(self, **kwargs):
            captured.update(kwargs)
            return {"took": 1, "hits": {"hits": []}}

    await elasticsearch.fetch_post_embeddings(_FakeES(), ["at://x"])
    assert captured["op"] == "hydrate"
```

Important: in production the client is wrapped exactly once, in `main.py`'s lifespan (`app.state.es = SlowQueryLoggingES(es)`, `src/app/main.py:126`), and every call site receives that wrapped client — so `op=` is consumed by the wrapper and never reaches the real ES client. Existing *tests*, however, often pass bare fakes directly to these functions; fakes defined as `async def search(self, **kwargs)` simply capture the extra kwarg, but any fake with a fixed signature must gain `op=None`, and any test asserting exact kwargs must add `op` to the expectation. Run the full suite in Step 4 to catch these.

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest src/app/lib/elasticsearch_test.py -q`
Expected: the two new tests FAIL with KeyError `'op'`.

- [ ] **Step 3: Add `op=` to all 11 call sites**

Example (`elasticsearch.py:106`):

```python
resp = await es.search(
    index="likes",
    op="likes",
    query=query,
    size=limit,
    sort=[{"created_at": "desc"}],
    _source=["subject_uri"],
    routing=",".join(user_dids),
)
```

Repeat per the table above. Nothing else changes at these sites.

- [ ] **Step 4: Run the full suite**

Run: `pipenv run pytest -q`
Expected: PASS. Fix any fixed-signature fakes by adding `op=None`.

- [ ] **Step 5: Verify no call site is missed**

Run: `grep -rn "\.search(" src/app --include="*.py" | grep -v _test | grep -v es_client.py`
Expected: every hit is either in the table above (now with `op=`) or not an ES search.

- [ ] **Step 6: Commit**

```bash
git add -A src/app
git commit -m "supply explicit op label at every ES search call site"
```

---

### Task 3: `eventloop.lag_ms` background monitor (PR B)

**Files:**
- Create: `src/app/lib/eventloop_monitor.py`
- Test: `src/app/lib/eventloop_monitor_test.py`
- Modify: `src/app/main.py` (lifespan)

**Interfaces:**
- Consumes: `get_metric_collector` from `.metrics`.
- Produces: `start_eventloop_monitor(interval_sec: float = 0.25) -> asyncio.Task` and `stop_eventloop_monitor(task: asyncio.Task) -> Awaitable[None]`. Metric `eventloop.lag_ms` (histogram, no extra labels — `endpoint`/`traffic` are absent for background tasks, which is correct: this is a per-instance signal).

- [ ] **Step 1: Write the failing test**

Create `src/app/lib/eventloop_monitor_test.py`:

```python
import asyncio

import pytest

from app.lib import eventloop_monitor


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.mark.asyncio
async def test_monitor_records_lag_samples(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.08)
    await eventloop_monitor.stop_eventloop_monitor(task)

    lag_records = [r for r in collector.records if r[0] == "eventloop.lag_ms"]
    assert len(lag_records) >= 3
    for _, value, attrs in lag_records:
        assert value >= 0
        assert attrs == {}


@pytest.mark.asyncio
async def test_monitor_measures_overshoot(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    # Block the loop synchronously for ~50ms; the next sample must see it.
    await asyncio.sleep(0.02)
    import time as _time
    _time.sleep(0.05)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)

    lags = [v for n, v, _ in collector.records if n == "eventloop.lag_ms"]
    assert max(lags) >= 30  # ms — blocked well past the 10ms interval


@pytest.mark.asyncio
async def test_stop_is_clean(monkeypatch):
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: None)
    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)
    assert task.cancelled() or task.done()
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest src/app/lib/eventloop_monitor_test.py -q`
Expected: FAIL — `ModuleNotFoundError` / `ImportError`.

- [ ] **Step 3: Implement `src/app/lib/eventloop_monitor.py`**

```python
"""Event-loop lag monitor: the direct "api instance saturated" signal.

A background task sleeps for a fixed interval and records how far past the
deadline it actually woke up. Under a healthy loop the overshoot is ~0-2 ms;
when coroutines or sync work monopolize the loop, every pending callback —
including responses already received from ES/inference/Perspective — waits
this long to run. See docs/design/load-test-regression-assessment.md §4.1.
"""

from __future__ import annotations

import asyncio
import contextlib
import time


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


DEFAULT_INTERVAL_SEC = 0.25


async def _monitor_loop(interval_sec: float) -> None:
    while True:
        target = time.monotonic() + interval_sec
        await asyncio.sleep(interval_sec)
        lag_ms = max(0.0, (time.monotonic() - target) * 1000)
        collector = get_metric_collector()
        if collector is not None:
            collector.record("eventloop.lag_ms", lag_ms)


def start_eventloop_monitor(interval_sec: float = DEFAULT_INTERVAL_SEC) -> asyncio.Task:
    return asyncio.create_task(_monitor_loop(interval_sec), name="eventloop-monitor")


async def stop_eventloop_monitor(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
```

- [ ] **Step 4: Run tests**

Run: `pipenv run pytest src/app/lib/eventloop_monitor_test.py -q`
Expected: PASS.

- [ ] **Step 5: Wire into `main.py` lifespan**

In the lifespan function, after `set_metric_collector(metrics)` (`src/app/main.py:114`):

```python
from .lib.eventloop_monitor import start_eventloop_monitor, stop_eventloop_monitor
...
eventloop_task = start_eventloop_monitor()
```

and in the shutdown path, before `set_metric_collector(None)` (`src/app/main.py:160`):

```python
await stop_eventloop_monitor(eventloop_task)
```

Match the surrounding lifespan code's structure (try/finally or yield ordering — read it first).

- [ ] **Step 6: Run the full suite**

Run: `pipenv run pytest -q`
Expected: PASS (any lifespan-exercising test still passes).

- [ ] **Step 7: Commit**

```bash
git add src/app/lib/eventloop_monitor.py src/app/lib/eventloop_monitor_test.py src/app/main.py
git commit -m "add eventloop.lag_ms background monitor wired into app lifespan"
```

---

### Task 4: External-call outcome counters with `status_code` (PR C)

**Files:**
- Modify: `src/app/lib/perspective.py` (`score_candidates._score_one` exception paths + quota path)
- Modify: `src/app/lib/bsky.py` (`get_followed_user_dids` failure paths)
- Test: `src/app/lib/perspective_test.py`, `src/app/lib/bsky_test.py`

**Interfaces:**
- Consumes: `get_metric_collector` (lazy import inside each module, same monkeypatch-friendly indirection as Tasks 1/3).
- Produces: counters `perspective.score.failure_count` and `bsky.follows.failure_count`, label `status_code` — values: HTTP status as string (`"429"`, `"503"`), `"timeout"`, or `"other"`. Success paths record nothing (success duration already exists for Perspective; follows volume is visible via candidates metrics).

Semantics: count every scoring attempt that yields no score for an *external* reason — including 429s and quota exhaustion, which are deliberately **not** degradation events today but are exactly what the dashboard's "external rate limit" playbook row needs. `PerspectiveLanguageNotSupportedError` is expected content behavior: not counted.

- [ ] **Step 1: Write the failing tests**

Add a `get_metric_collector` indirection to each module (Step 3) and tests. In `perspective_test.py` (reuse its existing client/monkeypatch fixtures where they exist — read the file first; the sketch below shows intent, adapt to its patterns):

```python
@pytest.mark.asyncio
async def test_score_candidates_counts_429_failures(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(perspective, "get_metric_collector", lambda: collector)

    async def _raise_429(self, content):
        raise aiohttp.ClientResponseError(request_info=None, history=(), status=429)

    monkeypatch.setattr(perspective.PerspectiveClient, "score", _raise_429)
    monkeypatch.setattr(perspective, "_client", perspective.PerspectiveClient.__new__(perspective.PerspectiveClient))

    result = await perspective.score_candidates([_candidate(at_uri="at://a", content="hi")])
    assert result == {"at://a": None}
    assert ("perspective.score.failure_count", 1, {"status_code": "429"}) in collector.records


@pytest.mark.asyncio
async def test_score_candidates_counts_timeout_failures(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(perspective, "get_metric_collector", lambda: collector)

    async def _raise_timeout(self, content):
        raise TimeoutError()

    monkeypatch.setattr(perspective.PerspectiveClient, "score", _raise_timeout)
    monkeypatch.setattr(perspective, "_client", perspective.PerspectiveClient.__new__(perspective.PerspectiveClient))

    await perspective.score_candidates([_candidate(at_uri="at://a", content="hi")])
    assert ("perspective.score.failure_count", 1, {"status_code": "timeout"}) in collector.records


@pytest.mark.asyncio
async def test_score_candidates_does_not_count_language_not_supported(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(perspective, "get_metric_collector", lambda: collector)

    async def _raise_lang(self, content):
        raise perspective.PerspectiveLanguageNotSupportedError("ja")

    monkeypatch.setattr(perspective.PerspectiveClient, "score", _raise_lang)
    monkeypatch.setattr(perspective, "_client", perspective.PerspectiveClient.__new__(perspective.PerspectiveClient))

    await perspective.score_candidates([_candidate(at_uri="at://a", content="hi")])
    assert not [r for r in collector.records if r[0] == "perspective.score.failure_count"]
```

(`_candidate` = the file's existing CandidatePost factory helper, or construct `CandidatePost` directly matching existing tests. `TimeoutError` propagates out of `client.score` after retries and is caught by `_score_one`'s generic `except Exception` today — the implementation must count it there.)

In `bsky_test.py` (again: reuse the file's existing httpx-mocking pattern):

```python
@pytest.mark.asyncio
async def test_get_followed_user_dids_counts_http_status_failures(monkeypatch, respx_or_fake):
    collector = _RecordingCollector()
    monkeypatch.setattr(bsky, "get_metric_collector", lambda: collector)
    # Arrange the mocked transport to return 503 for the follows URL
    # (follow bsky_test.py's existing mechanism for faking get_http_client()).
    with pytest.raises(bsky.FollowedUsersLookupError):
        await bsky.get_followed_user_dids("did:plc:x", limit=10)
    assert ("bsky.follows.failure_count", 1, {"status_code": "503"}) in collector.records


@pytest.mark.asyncio
async def test_get_followed_user_dids_counts_timeouts(monkeypatch, respx_or_fake):
    collector = _RecordingCollector()
    monkeypatch.setattr(bsky, "get_metric_collector", lambda: collector)
    # Arrange the mocked transport to raise httpx.ConnectTimeout.
    with pytest.raises(bsky.FollowedUsersLookupError):
        await bsky.get_followed_user_dids("did:plc:x", limit=10)
    assert ("bsky.follows.failure_count", 1, {"status_code": "timeout"}) in collector.records
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest src/app/lib/perspective_test.py src/app/lib/bsky_test.py -q`
Expected: new tests FAIL (no such metric records / no `get_metric_collector` attribute).

- [ ] **Step 3: Implement**

Shared helper shape (duplicate the small indirection in each module rather than inventing a new shared module):

```python
def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def _status_code_label(exc: BaseException) -> str:
    if isinstance(exc, TimeoutError):
        return "timeout"
    status = getattr(exc, "status", None) or getattr(getattr(exc, "response", None), "status_code", None)
    return str(status) if status else "other"


def _count_failure(metric: str, exc: BaseException) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(metric, 1, status_code=_status_code_label(exc))
```

`perspective.py` — in `_score_one`:
- `except PerspectiveLanguageNotSupportedError`: unchanged, no count.
- `except aiohttp.ClientResponseError as exc`: `_count_failure("perspective.score.failure_count", exc)` in both the 429 branch and the general branch (before existing handling).
- `except Exception as exc`: `_count_failure(...)` (covers `TimeoutError` → `"timeout"`, connection errors → `"other"`).
- Quota-exhausted branch (`not await _rate_limit_acquire()`): `collector.record("perspective.score.failure_count", 1, status_code="quota")`.

Note for `aiohttp.ClientResponseError`: `.status` is the attribute (`_status_code_label` handles it). For httpx (`bsky.py`), `httpx.HTTPStatusError` carries `.response.status_code` (also handled); `httpx.TimeoutException` is **not** a Python `TimeoutError` subclass — extend the helper in `bsky.py`:

```python
def _status_code_label(exc: BaseException) -> str:
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return str(status) if status else "other"
```

`bsky.py` — count once per failed *lookup* (not per retry): in `get_followed_user_dids`, in each of the three failure paths that either `raise` or return partial results (the inner `except (...) as exc` block, the `except TimeoutError as exc` block, and the outer `except (httpx.HTTPError, ValueError) as exc` block), call `_count_failure("bsky.follows.failure_count", exc)`.

- [ ] **Step 4: Run the full suite**

Run: `pipenv run pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/app/lib/perspective.py src/app/lib/perspective_test.py src/app/lib/bsky.py src/app/lib/bsky_test.py
git commit -m "count perspective and bsky follows failures with status_code label"
```

---

### Task 5: inference-service server-side metrics (PR D — separate repo)

**Files (all in `/Users/max/Projects/greenearth/inference-service`, its own git repo/branch):**
- Create: `metrics.py`
- Create: `metrics_test.py`
- Modify: `app.py` (lifespan + predict endpoint)
- Modify: `Pipfile` (add `opentelemetry-sdk`, `opentelemetry-exporter-gcp-monitoring` — copy the exact package names/versions from api's `Pipfile`)

**Interfaces:**
- Consumes: nothing from api at runtime; `metrics.py` is a trimmed copy of api's `src/app/lib/metrics.py`.
- Produces: metric prefix `custom.googleapis.com/greenearth-inference/`; `inference.predict.duration_ms` (histogram) with label `model_name`. `metrics.MetricCollector`, `metrics.set_metric_collector`, `metrics.get_metric_collector` — same signatures as api's.

Branch setup: `cd /Users/max/Projects/greenearth/inference-service && git checkout -b issue.343-metrics` (adapt to that repo's branch conventions — check `git log`/`git branch -a` there first; it may use a different issue numbering since api#343 is an api-repo issue).

- [ ] **Step 1: Copy and trim `metrics.py`**

Copy api's `src/app/lib/metrics.py` with three changes:
1. Exporter prefix → `custom.googleapis.com/greenearth-inference`.
2. Delete the `request_context` import and the `endpoint`/`traffic` ContextVar block in `record()` — attributes pass through as-is:

```python
def record(self, name: str, value: float, **attributes: str) -> None:
    attrs = dict(attributes) or None
    if name.endswith("_count"):
        self._get_counter(name).add(int(value), attrs)
    elif name.endswith("_rate"):
        self._get_gauge(name).set(value, attrs)
    else:
        self._get_histogram(name).record(value, attrs)
```

3. Meter name → `"greenearth/inference"`.

Environment detection: `app.py` reads its env via `GE_ENVIRONMENT` (verify with `grep -n "GE_ENVIRONMENT\|ENVIRONMENT" app.py` — use whatever it already uses; if nothing exists, read `os.environ.get("GE_ENVIRONMENT", "dev")`).

- [ ] **Step 2: Write the failing test**

Create `metrics_test.py` (mirror api's `metrics_test.py` structure — copy the `_from_reader` in-memory-reader test showing a histogram record round-trip, minus the endpoint/traffic ContextVar tests):

```python
def test_record_histogram_with_model_label():
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    reader = InMemoryMetricReader()
    collector = MetricCollector._from_reader(reader, "inference", "test")
    collector.record("inference.predict.duration_ms", 12.5, model_name="user_tower")
    data = reader.get_metrics_data()
    [metric] = [
        m
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
    ]
    assert metric.name == "inference.predict.duration_ms"
    [point] = list(metric.data.data_points)
    assert point.attributes == {"model_name": "user_tower"}
```

Run: `pipenv run pytest metrics_test.py -q` (in inference-service). Expected: FAIL (module missing), then PASS after Step 1 is complete — order Steps 1↔2 per strict TDD by writing the test first.

- [ ] **Step 3: Wire into `app.py`**

Lifespan (`app.py:34`):

```python
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    ensure_models_loaded()
    collector = MetricCollector(
        service_name="greenearth-inference",
        env=os.environ.get("GE_ENVIRONMENT", "dev"),
        export_interval_sec=int(os.environ.get("GE_METRICS_EXPORT_INTERVAL_SEC", "60")),
    )
    set_metric_collector(collector)
    try:
        yield
    finally:
        set_metric_collector(None)
        await collector.shutdown()
```

Predict endpoint (`app.py:1215`, `POST /models/{model_name}/predict`): wrap the handler body:

```python
start = time.monotonic()
try:
    ...existing body...
finally:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(
            "inference.predict.duration_ms",
            (time.monotonic() - start) * 1000,
            model_name=model_name,
        )
```

Add an endpoint test in `app_test.py` following its existing TestClient pattern: call predict with a loaded test model (reuse the file's existing predict-endpoint fixtures) and assert a `inference.predict.duration_ms` record lands in a `_RecordingCollector` installed via `set_metric_collector`.

- [ ] **Step 4: Run inference-service tests**

Run: `cd /Users/max/Projects/greenearth/inference-service && pipenv run pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit (inference-service repo)**

```bash
git add metrics.py metrics_test.py app.py app_test.py Pipfile Pipfile.lock
git commit -m "add server-side metrics: inference.predict.duration_ms by model_name"
```

Deploy-order note (goes in the PR description): deploy inference-service before the api dashboard PR is used in anger; the gap chart needs both sides.

---

### Task 6: Bottleneck dashboard as code with baseline threshold lines (PR E)

**Files:**
- Create: `monitoring/dashboards/bottleneck.json.tmpl`
- Create: `monitoring/deploy.sh`
- Create: `monitoring/README.md`
- Test: `monitoring/render_test.py` is overkill for a shell renderer — validation is `deploy.sh --dry-run` (below) plus `python -m json.tool` on the rendered output.

**Interfaces:**
- Consumes: metric names from Tasks 1–5 plus existing metrics (design doc §2) and Appendix A PromQL.
- Produces: one Cloud Monitoring dashboard per env named `Load Test & Bottleneck Attribution (stage|prod)`; `monitoring/deploy.sh [stage|prod] [--dry-run]`.

Template mechanism: keep it dependency-free — `bottleneck.json.tmpl` contains literal `${ENV}` / `${NAMESPACE}` / `${CLUSTER}` tokens; `deploy.sh` renders with `sed` and calls:

```bash
gcloud monitoring dashboards list --project greenearth-471522 \
  --filter "displayName='Load Test & Bottleneck Attribution (${ENV})'" --format 'value(name)'
# if found: gcloud monitoring dashboards update <name> --config-from-file=<rendered>
# else:     gcloud monitoring dashboards create --config-from-file=<rendered>
```

`--dry-run` renders and `python3 -m json.tool`-validates without calling gcloud.

Dashboard layout (design doc §4.2 — six rows, `mosaicLayout` with section headers). Chart inventory with query type and thresholds:

| Row | Chart | Query (sketch) | Threshold line |
|---|---|---|---|
| 1 Load & UX | Renders/min by `traffic` | MQL: `feed.render.success_count` + `failure_count` rate, group by `traffic` | — |
| 1 | `feed.render` p50/p95 by `traffic` | distribution percentile, filter `namespace=${NAMESPACE}` | **2500 ms** (probe/real p95 baseline; see README table) |
| 1 | Failures + degraded + 5xx /min | `feed.render.failure_count`, `.degraded_count`, Cloud Run `request_count` 5xx | **1/min** |
| 2 Stage | `candidates.generate` p95 by `generator_name` | distribution percentile | — |
| 2 | `rank.model` p95 vs `inference.predict` p95 (gap) | two series, same chart: `greenearth-api/rank.model.duration_ms` + `greenearth-inference/inference.predict.duration_ms` | — |
| 2 | `perspective.score` p95 + `failure_count` by `status_code` | histogram p95 + stacked counter rate | — |
| 3 api saturation | `eventloop.lag_ms` p95 per instance | percentile, no reducer across instances | **100 ms** |
| 3 | Cloud Run instances / CPU / memory | `run.googleapis.com/container/instance_count`, `cpu/utilizations`, `memory/utilizations` | — |
| 4 ES | `es.query.took_ms` vs `duration_ms` p95 by `op` (gap) | two percentile series per op | — |
| 4 | Search thread-pool queue + rejected | PromQL `elasticsearch_thread_pool_queue_count{type="search"}`, `_rejected_count` rate | **rejected > 0** |
| 4 | ES mean search latency | Appendix A query 4 | **10 ms** |
| 5 Page cache | Major faults/s per pod | Appendix A query 1 | — |
| 5 | Device read MB/s per node | Appendix A query 2 | — |
| 5 | Evictable GB per pod | Appendix A query 3 | — |
| 6 Blast radius | `ingex/freshness_sec` p95 by source | distribution percentile | **300 s** |
| 6 | `ingex/es.bulk_index_*.took_ms` p95 | percentile | — |
| 6 | JVM GC rate + breakers + PD IO latency | PromQL exporter + `compute.googleapis.com` | — |

Threshold values above are **starting points from the 07-29..31 measured data** (design doc Appendix A/B): steady-state probe p95 1.6–2.2 s → 2500 ms line; mean ES search ~5 ms clean / 17–38 ms churned → 10 ms line; failure ≈ 0 → 1/min line; lag: healthy loop <10 ms, saturated ~seconds → 100 ms line; freshness: alert policy already exists for prod freshness — mirror its threshold (check with `gcloud alpha monitoring policies list --project greenearth-471522 --format json | grep -A5 -i freshness` at implementation time; fall back to 300 s). Record whatever is used in `monitoring/README.md` with a "reviewed 2026-08" date stamp so drift is auditable. These are chart annotations, not alerts — cheap to adjust.

Implementation notes:
- Thresholds in dashboard JSON: each `xyChart` takes `"thresholds": [{"value": 2500, "label": "baseline p95", "targetAxis": "Y1"}]`.
- Custom-metric distribution percentiles in MQL: `fetch generic_task | metric 'custom.googleapis.com/greenearth-api/feed.render.duration_ms' | filter resource.namespace == '${NAMESPACE}' | align delta(1m) | every 1m | group_by [metric.traffic], [val: percentile(value.duration_ms, 95)]`.
- PromQL charts use `"prometheusQuery"` in the widget's `timeSeriesQuery`; copy the Appendix A queries verbatim, replacing the hardcoded `greenearth-prod` namespace/cluster with `${NAMESPACE}`/`${CLUSTER}` tokens. **Note:** ES + GKE metrics exist only for the prod cluster (stage api still reads prod ES) — for `ENV=stage`, rows 4–6 still point at the prod cluster; only the `greenearth-api` namespace filter differs. `deploy.sh` therefore substitutes `NAMESPACE=stage|prod` for api metrics and always `greenearth-prod` for cluster-scoped queries.
- Compare-to-past: not expressible in dashboard JSON for all chart types; where supported use `timeshiftDuration: "86400s"` on the key UX charts (`feed.render` p95). Otherwise the README documents the console's "compare to past" toggle.
- Build the JSON against the **2026-07-31 02:00–04:00 UTC window** (issue's timeline table): after `deploy.sh stage --dry-run` passes, deploy to stage project scope and open the dashboard at that window; acceptance = the six rows reproduce Appendix B's diagnosis (burst 1 saturation visible in rows 1+3, clean burst 2, ES churn in rows 4–5).

- [ ] **Step 1: Write `bottleneck.json.tmpl`** — full JSON, all 16 charts from the table, `${ENV}`/`${NAMESPACE}`/`${CLUSTER}` tokens, threshold lines per the table.
- [ ] **Step 2: Write `deploy.sh`** (bash, `set -euo pipefail`, args `stage|prod` + `--dry-run`, render → validate JSON → create-or-update, print dashboard URL + ID on success; write the returned dashboard resource ID into `monitoring/dashboards/ids.env` as `DASHBOARD_ID_STAGE=...`/`DASHBOARD_ID_PROD=...` — Task 7 reads this file).
- [ ] **Step 3: Validate render**: `./monitoring/deploy.sh stage --dry-run && ./monitoring/deploy.sh prod --dry-run`. Expected: both print "rendered OK".
- [ ] **Step 4: Write `monitoring/README.md`** — the §4.3 attribution playbook table (copy from the design doc, including the 07-31 evidence column), the baseline-threshold table (chart, value, source measurement, review date), deploy instructions, and the stage/prod cluster-scope caveat.
- [ ] **Step 5: Deploy to stage and validate against the 07-31 window** (needs `gcloud` auth): `./monitoring/deploy.sh stage`, open printed URL, set window 2026-07-31 02:00–04:00 UTC, check the Appendix-B signals appear. Adjust queries until they do. This step is interactive/visual — record findings in the PR description.
- [ ] **Step 6: Commit**

```bash
git add monitoring/
git commit -m "add bottleneck-attribution dashboard as code with baseline threshold lines; deploy script and playbook"
```

---

### Task 7: `analyze.py` trim + dashboard deep-link (PR F)

**Files:**
- Modify: `scripts/load_test/analyze.py`
- Modify: `scripts/load_test/lib.py`
- Test: `scripts/load_test/analyze_test.py`, `scripts/load_test/lib_test.py`

**Interfaces:**
- Consumes: `monitoring/dashboards/ids.env` (Task 6) — `DASHBOARD_ID_STAGE`/`DASHBOARD_ID_PROD` lines; run-window `start`/`end` datetimes already computed in `analyze.run`.
- Produces: `lib.dashboard_url(environment: str, start: datetime, end: datetime) -> str | None` (None when ids.env missing or env absent — analyze prints a hint instead of a link).

- [ ] **Step 1: Write the failing test**

In `scripts/load_test/lib_test.py`:

```python
def test_dashboard_url_builds_console_link(tmp_path, monkeypatch):
    ids = tmp_path / "ids.env"
    ids.write_text("DASHBOARD_ID_STAGE=projects/12345/dashboards/abcd-ef\n")
    monkeypatch.setattr(lib, "DASHBOARD_IDS_FILE", str(ids))

    start = datetime(2026, 7, 31, 2, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 31, 4, 0, tzinfo=timezone.utc)
    url = lib.dashboard_url("stage", start, end)

    assert "console.cloud.google.com/monitoring/dashboards/builder/abcd-ef" in url
    assert "project=greenearth-471522" in url
    # time range encoded as start/end ISO timestamps
    assert "2026-07-31T02:00" in url and "2026-07-31T04:00" in url


def test_dashboard_url_returns_none_without_ids_file(tmp_path, monkeypatch):
    monkeypatch.setattr(lib, "DASHBOARD_IDS_FILE", str(tmp_path / "missing.env"))
    assert lib.dashboard_url("stage", datetime.now(timezone.utc), datetime.now(timezone.utc)) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest scripts/load_test/lib_test.py -q`
Expected: FAIL — `dashboard_url` not defined.

- [ ] **Step 3: Implement `lib.dashboard_url`**

In `scripts/load_test/lib.py`:

```python
DASHBOARD_IDS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "monitoring", "dashboards", "ids.env",
)


def dashboard_url(environment: str, start: datetime, end: datetime) -> str | None:
    """Console deep-link to the bottleneck dashboard, time range pre-set."""
    try:
        with open(DASHBOARD_IDS_FILE) as f:
            ids = dict(
                line.strip().split("=", 1)
                for line in f
                if "=" in line and not line.startswith("#")
            )
    except FileNotFoundError:
        return None
    resource = ids.get(f"DASHBOARD_ID_{environment.upper()}")
    if not resource:
        return None
    dashboard_id = resource.rsplit("/", 1)[-1]
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    s = start.astimezone(timezone.utc).strftime(fmt)
    e = end.astimezone(timezone.utc).strftime(fmt)
    return (
        f"https://console.cloud.google.com/monitoring/dashboards/builder/{dashboard_id}"
        f";startTime={s};endTime={e}?project={GCP_PROJECT}"
    )
```

- [ ] **Step 4: Trim `analyze.py`**

- Delete `_server_metrics`, `_server_logs`, `build_percentile_request`, `_PERCENTILE_ALIGNERS`, `ENV_RESOURCE_LABEL`, `METRIC_PREFIX`, and the `--no-server` flag (and their tests in `analyze_test.py` — check `grep -n "server\|percentile" scripts/load_test/analyze_test.py` and remove/port accordingly).
- In `run()`, after `_client_summary(records)`:

```python
url = dashboard_url(args.environment, start, end)
if url:
    console.print(f"\n[bold]Dashboard (run window pre-set):[/bold] {url}")
else:
    console.print(
        "\n[yellow]No dashboard ID recorded for this environment — "
        "run monitoring/deploy.sh first.[/yellow]"
    )
```

- Update the module docstring and `scripts/load_test/README.md` (server-side analysis now lives on the dashboard; link to `monitoring/README.md` playbook).

- [ ] **Step 5: Run the full suite**

Run: `pipenv run pytest -q && pipenv run pytest scripts/load_test -q` (check how scripts tests are collected — if the root `pytest` run already includes them, the second command is redundant).
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/load_test monitoring/dashboards/ids.env
git commit -m "analyze.py: replace server-metrics sections with dashboard deep-link"
```

---

### Task 8: Design doc + stage validation wrap-up

**Files:**
- Commit: `docs/design/load-test-regression-assessment.md` (v4, already updated in this worktree) — include in PR A so reviewers of the instrumentation see the design.

- [ ] **Step 1: Commit the design doc** (first commit on the branch, before Task 1, so every PR can reference it):

```bash
git add docs/design/load-test-regression-assessment.md docs/superpowers/plans/2026-08-01-load-test-regression-assessment.md
git commit -m "add regression-assessment design doc v4 and implementation plan"
```

- [ ] **Step 2: Stage metric-arrival validation** (after PRs A–D deploy to stage; needs gcloud): render a few feeds against stage, then confirm each new metric type exists:

```bash
for m in es.query.duration_ms es.query.took_ms eventloop.lag_ms; do
  gcloud monitoring metrics-descriptors describe \
    "custom.googleapis.com/greenearth-api/$m" --project greenearth-471522 \
    && echo "OK $m"
done
gcloud monitoring metrics-descriptors describe \
  "custom.googleapis.com/greenearth-inference/inference.predict.duration_ms" \
  --project greenearth-471522
```

Expected: all four descriptors exist and `es.query.*` carries the `op` label with only taxonomy values (no `unlabeled` — if `unlabeled` appears, a call site was missed; find it via Task 2 Step 5's grep).

- [ ] **Step 3: Deploy order reminder** (PR descriptions): inference-service (PR D) deploys before the next load test; instrumentation PRs A–C deploy to prod **before** the next big load-test ladder (design doc §5).

## PR map

| PR | Repo | Tasks | Branch |
|---|---|---|---|
| A | api | 8.1, 1, 2 | `issue.343` (this worktree) |
| B | api | 3 | stacked on A or same branch, user's call |
| C | api | 4 | 〃 |
| D | inference-service | 5 | new branch in that repo |
| E | api | 6 | after A–D deployed (dashboard references the new metrics) |
| F | api | 7 | after E (needs dashboard ID) |

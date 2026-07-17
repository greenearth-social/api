# Diversity Score Metric Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** For every cursor response from `GET /xrpc/app.bsky.feed.getFeedSkeleton`, compute the mean MMR diversity score of the served page and emit it as a `feed.mean_diversity_score` metric tagged with feed name and batch number.

**Architecture:** `mmr_rerank()` stamps a `diversity_score` onto each returned `CandidatePost`. Those scores travel with the URI list through the pipeline return and are stored alongside URIs in `FeedCacheDocument`. At each serve point in `getFeedSkeleton`, the scores for the current page are sliced and averaged to produce the metric.

**Tech Stack:** Python 3.13, FastAPI, Pydantic v2, OpenTelemetry (`MetricCollector.record()`), pytest + pytest-asyncio

## Global Constraints

- All test files live next to the module they test (e.g. `src/app/lib/diversify_test.py` beside `src/app/lib/diversify.py`)
- Test command from repo root: `pytest src/` (pythonpath is `["src", "scripts"]` per pyproject.toml)
- Line length: 100 chars (ruff)
- No comments explaining what code does; only add a comment when the *why* is non-obvious
- `CandidatePost` is Pydantic; use `.model_copy(update={...})` to produce modified instances
- `FeedCache` is an ABC; both `FirestoreFeedCache` (prod) and `InMemoryFeedCache` (in xrpc_test.py) must implement the updated interface

---

### Task 1: Stamp `diversity_score` on `CandidatePost` in `mmr_rerank()`

**Files:**
- Modify: `src/app/models.py` — add `diversity_score` field to `CandidatePost`
- Modify: `src/app/lib/diversify.py` — stamp score on each selected candidate before return
- Create: `src/app/lib/diversify_test.py` — unit tests for diversity score values

**Interfaces:**
- Produces: `CandidatePost.diversity_score: float | None` — set to `1.0` for the first MMR pick; `max(0.0, 1.0 - max_sim[best])` for each subsequent pick; `None` on candidates that were not MMR-reranked.
- `mmr_rerank(candidates) -> list[CandidatePost]` signature unchanged; return value now has `diversity_score` set on every element.

- [ ] **Step 1: Write failing tests for `mmr_rerank` diversity scores**

Create `src/app/lib/diversify_test.py`:

```python
"""Unit tests for mmr_rerank diversity score stamping."""

import math

import pytest

from app.lib.diversify import mmr_rerank
from app.models import CandidatePost


def _post(uri: str, score: float, author: str) -> CandidatePost:
    return CandidatePost(at_uri=uri, score=score, author_did=author)


class TestMmrRerankDiversityScore:
    def test_single_candidate_gets_score_1(self):
        posts = [_post("at://a/1", 1.0, "did:plc:a")]
        result = mmr_rerank(posts)
        assert len(result) == 1
        assert result[0].diversity_score == 1.0

    def test_first_pick_always_scores_1(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://b/1", 0.5, "did:plc:b"),
        ]
        result = mmr_rerank(posts)
        assert result[0].diversity_score == 1.0

    def test_same_author_reduces_diversity(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://a/2", 0.9, "did:plc:a"),  # same author — high similarity penalty
            _post("at://b/1", 0.5, "did:plc:b"),
        ]
        result = mmr_rerank(posts)
        # The second pick from the same author should have a lower diversity score
        # than a post from a different author.
        scores_by_uri = {c.at_uri: c.diversity_score for c in result}
        assert scores_by_uri["at://a/1"] == 1.0
        same_author_score = scores_by_uri["at://a/2"]
        diff_author_score = scores_by_uri["at://b/1"]
        assert same_author_score is not None
        assert diff_author_score is not None
        assert same_author_score < diff_author_score

    def test_all_scores_in_unit_range(self):
        posts = [_post(f"at://a/{i}", float(i), "did:plc:a") for i in range(5)]
        result = mmr_rerank(posts)
        for c in result:
            assert c.diversity_score is not None
            assert 0.0 <= c.diversity_score <= 1.0

    def test_scores_parallel_to_output_order(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://b/1", 0.8, "did:plc:b"),
            _post("at://c/1", 0.6, "did:plc:c"),
        ]
        result = mmr_rerank(posts)
        assert all(c.diversity_score is not None for c in result)
        assert len(result) == 3
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest src/app/lib/diversify_test.py -v
```

Expected: `AttributeError` or `AssertionError` — `diversity_score` attribute does not exist yet.

- [ ] **Step 3: Add `diversity_score` field to `CandidatePost` in `src/app/models.py`**

After the `external_uri` field (line 75), add:

```python
    diversity_score: float | None = Field(
        default=None,
        description="Per-item diversity score from MMR reranking (0=least diverse, 1=most diverse)",
    )
```

- [ ] **Step 4: Stamp `diversity_score` in `mmr_rerank()` in `src/app/lib/diversify.py`**

Replace the `while remaining:` block's selection logic and the final return so that each selected candidate gets its diversity score stamped. The full replacement spans the loop and the return statement:

```python
    diversity_scores: list[float] = []

    while remaining:
        if not selected:
            best = max(remaining, key=lambda i: (1 - BETA) * norm_scores[i])
            diversity = 1.0
            if diag is not None:
                diag.append(
                    (
                        candidates[best].at_uri or "",
                        norm_scores[best],
                        (1 - BETA) * norm_scores[best],
                        0.0,
                        0.0,
                    )
                )
        else:
            best = max(
                remaining,
                key=lambda i: (1 - BETA) * norm_scores[i] - BETA * max_sim[i],
            )
            diversity = max(0.0, 1.0 - max_sim[best])
            if diag is not None:
                author_match, cosine = max_components[best]
                author_penalty = BETA * AUTHOR_WEIGHT * author_match
                content_penalty = BETA * (1 - AUTHOR_WEIGHT) * cosine
                score = (1 - BETA) * norm_scores[best] - BETA * max_sim[best]
                diag.append(
                    (
                        candidates[best].at_uri or "",
                        norm_scores[best],
                        score,
                        author_penalty,
                        content_penalty,
                    )
                )

        diversity_scores.append(diversity)
        selected.append(best)
        remaining.remove(best)

        for i in remaining:
            s, author_match, cosine = _similarity_components(
                author_dids[i], author_dids[best], vecs[i], vecs[best]
            )
            if s > max_sim[i]:
                max_sim[i] = s
                max_components[i] = (author_match, cosine)

    if rec is not None and diag is not None:
        rec.record_diversification(diag)

    return [
        candidates[i].model_copy(update={"diversity_score": s})
        for i, s in zip(selected, diversity_scores)
    ]
```

- [ ] **Step 5: Run tests to verify they pass**

```
pytest src/app/lib/diversify_test.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 6: Run full test suite to check for regressions**

```
pytest src/ -v
```

Expected: all existing tests pass (the `diversity_score` field defaults to `None`, so nothing breaks).

- [ ] **Step 7: Commit**

```bash
git add src/app/models.py src/app/lib/diversify.py src/app/lib/diversify_test.py
git commit -m "add diversity_score field to CandidatePost; stamp in mmr_rerank"
```

---

### Task 2: Thread diversity scores through the feed cache

**Files:**
- Modify: `src/app/documents.py` — add `diversity_scores` field to `FeedCacheDocument`
- Modify: `src/app/lib/feed_cache.py` — add `CachedFeed` dataclass; update ABC and `FirestoreFeedCache`
- Modify: `src/app/lib/feed_cache_test.py` — update existing tests + add score round-trip tests

**Interfaces:**
- Produces: `CachedFeed` dataclass with `items: list[str]` and `diversity_scores: list[float] | None`
- `FeedCache.store(key, items, scores=None, ttl_seconds=DEFAULT_TTL_SECONDS) -> None`
- `FeedCache.retrieve(key) -> CachedFeed | None`
- `FeedCache.append(key, new_items, new_scores=None) -> CachedFeed | None`

- [ ] **Step 1: Write failing tests for the updated cache interface**

Add to `src/app/lib/feed_cache_test.py` (append after existing tests):

```python
# ---------------------------------------------------------------------------
# Diversity score threading
# ---------------------------------------------------------------------------

class TestDiversityScores:
    @pytest.mark.asyncio
    async def test_store_and_retrieve_scores(self):
        db, col_ref, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1", "at://a/2"],
            "diversity_scores": [1.0, 0.5],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is not None
        assert result.items == ["at://a/1", "at://a/2"]
        assert result.diversity_scores == [1.0, 0.5]

    @pytest.mark.asyncio
    async def test_retrieve_without_scores_returns_none_scores(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is not None
        assert result.diversity_scores is None

    @pytest.mark.asyncio
    async def test_store_persists_scores(self):
        db, col_ref, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        await cache.store("key1", ["at://a/1", "at://a/2"], scores=[1.0, 0.7])

        stored = doc_ref.set.call_args[0][0]
        assert stored["diversity_scores"] == [1.0, 0.7]

    @pytest.mark.asyncio
    async def test_append_merges_scores(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "diversity_scores": [1.0],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"], new_scores=[0.6])
        assert result is not None
        assert result.items == ["at://a/1", "at://a/2"]
        assert result.diversity_scores == [1.0, 0.6]

    @pytest.mark.asyncio
    async def test_append_drops_scores_when_existing_has_none(self):
        """If the existing cache entry has no scores, merged result also has none."""
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"], new_scores=[0.6])
        assert result is not None
        assert result.diversity_scores is None

    @pytest.mark.asyncio
    async def test_append_drops_scores_when_new_scores_none(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "diversity_scores": [1.0],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"], new_scores=None)
        assert result is not None
        assert result.diversity_scores is None
```

- [ ] **Step 2: Run new tests to verify they fail**

```
pytest src/app/lib/feed_cache_test.py::TestDiversityScores -v
```

Expected: `AttributeError` — `retrieve` returns `list` not a `CachedFeed`; `store` signature mismatch.

- [ ] **Step 3: Add `diversity_scores` to `FeedCacheDocument` in `src/app/documents.py`**

Replace the `FeedCacheDocument` class (lines 54–61):

```python
class FeedCacheDocument(BaseModel):
    """Cached feed result set used by cursor pagination.

    The document ID is an opaque cache key generated per feed request.
    """

    items: list[str] = Field(default_factory=list, description="Cached AT URI list")
    diversity_scores: list[float] | None = Field(
        default=None,
        description="Per-item MMR diversity scores parallel to items; None when diversification was disabled",
    )
    expires_at: datetime = Field(..., description="UTC expiration timestamp for this cache entry")
```

- [ ] **Step 4: Add `CachedFeed` dataclass and update `FeedCache` in `src/app/lib/feed_cache.py`**

Replace the full contents of `src/app/lib/feed_cache.py`:

```python
"""Feed result cache — stores pre-materialised feed pages for cursor pagination.

The abstract :class:`FeedCache` interface intentionally hides the storage
backend so it can be swapped (e.g. to Redis) without touching callers.

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ..documents import FeedCacheDocument

logger = logging.getLogger(__name__)

FEED_CACHE_COLLECTION = "feed_cache"
DEFAULT_TTL_SECONDS = 600  # 10 minutes


@dataclass
class CachedFeed:
    """Items and optional diversity scores retrieved from the cache."""

    items: list[str]
    diversity_scores: list[float] | None = field(default=None)


class FeedCache(ABC):
    """Backend-agnostic interface for storing and retrieving feed pages."""

    @abstractmethod
    async def store(
        self,
        key: str,
        items: list[str],
        scores: list[float] | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        """Persist *items* (AT URIs) and optional *scores* under *key* with the given TTL."""
        ...

    @abstractmethod
    async def retrieve(self, key: str) -> CachedFeed | None:
        """Fetch the cached feed for *key*, or ``None`` if missing/expired."""
        ...

    @abstractmethod
    async def append(
        self,
        key: str,
        new_items: list[str],
        new_scores: list[float] | None = None,
    ) -> CachedFeed | None:
        """Append *new_items* (and optionally *new_scores*) to an existing entry.

        Returns the full updated :class:`CachedFeed`, or ``None`` if the entry
        is missing or expired.  When either the existing or new scores are
        ``None``, the merged ``diversity_scores`` is also ``None``.
        """
        ...


class FirestoreFeedCache(FeedCache):
    """Firestore-backed feed cache.

    Each cached result set is stored as a document in the ``feed_cache``
    collection.  An ``expires_at`` timestamp is written alongside the data
    and checked on reads so that stale entries are never returned even if
    Firestore's TTL policy hasn't run yet.
    """

    def __init__(self, db: AsyncClient) -> None:
        self._db = db

    async def store(
        self,
        key: str,
        items: list[str],
        scores: list[float] | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        cache_doc = FeedCacheDocument(items=items, diversity_scores=scores, expires_at=expires_at)
        await (
            self._db.collection(FEED_CACHE_COLLECTION)
            .document(key)
            .set(cache_doc.model_dump())
        )

    async def retrieve(self, key: str) -> CachedFeed | None:
        doc = await self._db.collection(FEED_CACHE_COLLECTION).document(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None

        try:
            cache_doc = FeedCacheDocument.model_validate(data)
        except Exception:
            logger.warning("Invalid feed cache document shape for key=%s", key)
            return None

        expires_at = cache_doc.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None

        return CachedFeed(items=cache_doc.items, diversity_scores=cache_doc.diversity_scores)

    async def append(
        self,
        key: str,
        new_items: list[str],
        new_scores: list[float] | None = None,
    ) -> CachedFeed | None:
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)
        doc = await ref.get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None

        try:
            cache_doc = FeedCacheDocument.model_validate(data)
        except Exception:
            logger.warning("Invalid feed cache document shape for key=%s", key)
            return None

        expires_at = cache_doc.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None

        updated_items = cache_doc.items + new_items
        if cache_doc.diversity_scores is not None and new_scores is not None:
            updated_scores: list[float] | None = cache_doc.diversity_scores + new_scores
        else:
            updated_scores = None

        update_data: dict = {"items": updated_items}
        if updated_scores is not None:
            update_data["diversity_scores"] = updated_scores
        await ref.update(update_data)
        return CachedFeed(items=updated_items, diversity_scores=updated_scores)
```

- [ ] **Step 5: Update existing `feed_cache_test.py` tests to match new `retrieve`/`append` return type**

The existing tests that call `retrieve` or `append` currently assert on the raw list. Update them to unpack the `CachedFeed`:

In `TestFirestoreFeedCacheRetrieve`:
- `assert result == ["at://a/1"]` → `assert result is not None` + `assert result.items == ["at://a/1"]`

In `TestFirestoreFeedCacheAppend`:
- `assert result == ["at://a/1", "at://a/2", "at://a/3"]` → `assert result is not None` + `assert result.items == ["at://a/1", "at://a/2", "at://a/3"]`
- `doc_ref.update.assert_awaited_once_with({"items": [...]})` — this still holds (no scores in that test)

Full updated `TestFirestoreFeedCacheRetrieve` class:

```python
class TestFirestoreFeedCacheRetrieve:
    @pytest.mark.asyncio
    async def test_returns_items_when_not_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is not None
        assert result.items == ["at://a/1"]

    @pytest.mark.asyncio
    async def test_returns_none_when_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) - timedelta(minutes=1),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_document_missing(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = False
        doc_ref.get.return_value = snap

        result = await cache.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_to_dict_is_none(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = None
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_handles_naive_datetime_from_firestore(self):
        """Firestore sometimes returns naive datetimes; they should be treated as UTC."""
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is not None
        assert result.items == ["at://a/1"]
```

Full updated `TestFirestoreFeedCacheAppend` class:

```python
class TestFirestoreFeedCacheAppend:
    @pytest.mark.asyncio
    async def test_appends_items_to_existing_doc(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2", "at://a/3"])
        assert result is not None
        assert result.items == ["at://a/1", "at://a/2", "at://a/3"]
        doc_ref.update.assert_awaited_once_with({"items": ["at://a/1", "at://a/2", "at://a/3"]})

    @pytest.mark.asyncio
    async def test_returns_none_when_document_missing(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = False
        doc_ref.get.return_value = snap

        result = await cache.append("missing", ["at://a/1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) - timedelta(minutes=1),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"])
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_to_dict_is_none(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = None
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"])
        assert result is None
```

- [ ] **Step 6: Run full cache test suite**

```
pytest src/app/lib/feed_cache_test.py -v
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/app/documents.py src/app/lib/feed_cache.py src/app/lib/feed_cache_test.py
git commit -m "add diversity_scores to FeedCacheDocument and FeedCache interface"
```

---

### Task 3: Thread scores through the pipeline and emit the metric in `getFeedSkeleton`

**Files:**
- Modify: `src/app/routers/xrpc.py` — update `_run_ranking_pipeline`, `_run_pipeline_capturing`, `getFeedSkeleton`, and `InMemoryFeedCache` (lives in `xrpc_test.py`)
- Modify: `src/app/routers/xrpc_test.py` — update `InMemoryFeedCache`; add metric emission tests

**Interfaces:**
- Consumes: `mmr_rerank()` → `list[CandidatePost]` with `diversity_score` stamped (Task 1)
- Consumes: `FeedCache.store(key, items, scores, ttl_seconds)`, `retrieve(key) -> CachedFeed | None`, `append(key, new_items, new_scores) -> CachedFeed | None` (Task 2)
- `_run_ranking_pipeline(feed_cfg, gen_request, es) -> tuple[list[str], list[float] | None]`
- `_run_pipeline_capturing(...) -> tuple[list[str], list[float] | None]`
- Metric emitted: `metric_collector.record("feed.mean_diversity_score", mean, feed_name=feed_name, batch=str(batch_num))`

- [ ] **Step 1: Write failing metric tests**

Add to `src/app/routers/xrpc_test.py` (append at the bottom):

```python
# ---------------------------------------------------------------------------
# Diversity score metric
# ---------------------------------------------------------------------------

from ..lib.metrics import set_metric_collector


class FakeMetricCollector:
    def __init__(self):
        self.calls: list[tuple[str, float, dict]] = []

    def record(self, name: str, value: float, **attributes: str) -> None:
        self.calls.append((name, value, dict(attributes)))


class TestDiversityScoreMetric:
    def _make_scored_candidates(self, n: int) -> list[CandidatePost]:
        """Candidates with diversity_score set, as mmr_rerank would produce."""
        return [
            CandidatePost(
                at_uri=f"at://test/{i}",
                score=1.0 - i * 0.1,
                author_did=f"did:plc:{i}",
                diversity_score=1.0 if i == 0 else 0.8,
            )
            for i in range(n)
        ]

    def test_metric_emitted_on_initial_request(self, monkeypatch):
        collector = FakeMetricCollector()
        set_metric_collector(collector)

        candidates = self._make_scored_candidates(35)
        with _patch_unranked_your_feed_generators(candidates):
            client = TestClient(app)
            resp = client.get(
                f"/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI_FROM_APPVIEW}&limit=5"
            )

        set_metric_collector(None)
        assert resp.status_code == 200
        metric_calls = [c for c in collector.calls if c[0] == "feed.mean_diversity_score"]
        assert len(metric_calls) == 1
        name, value, attrs = metric_calls[0]
        assert attrs["feed_name"] == FEED_RKEY
        assert attrs["batch"] == "0"
        assert 0.0 <= value <= 1.0

    def test_metric_emitted_on_cursor_request(self, monkeypatch):
        collector = FakeMetricCollector()
        set_metric_collector(collector)

        candidates = self._make_scored_candidates(35)
        with _patch_unranked_your_feed_generators(candidates):
            client = TestClient(app)
            # Initial request
            resp1 = client.get(
                f"/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI_FROM_APPVIEW}&limit=5"
            )
            assert resp1.status_code == 200
            cursor = resp1.json().get("cursor")
            assert cursor is not None

            # Cursor follow-up
            resp2 = client.get(
                f"/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI_FROM_APPVIEW}&limit=5&cursor={cursor}"
            )

        set_metric_collector(None)
        assert resp2.status_code == 200
        metric_calls = [c for c in collector.calls if c[0] == "feed.mean_diversity_score"]
        assert len(metric_calls) == 2
        batches = [c[2]["batch"] for c in metric_calls]
        assert "0" in batches
        assert "1" in batches

    def test_metric_not_emitted_when_no_scores(self, monkeypatch):
        """When diversification is off (no diversity_score on candidates), no metric fires."""
        collector = FakeMetricCollector()
        set_metric_collector(collector)

        # Use random feed — no diversification
        candidates = [
            CandidatePost(at_uri=f"at://test/{i}", score=float(i), author_did=f"did:plc:{i}")
            for i in range(5)
        ]
        random_gen = AsyncMock()
        random_gen.generate.return_value = CandidateResult(
            generator_name="random_posts", candidates=candidates
        )
        with patch("app.lib.candidates.generate.get_generator", return_value=random_gen):
            client = TestClient(app)
            resp = client.get(
                f"/xrpc/app.bsky.feed.getFeedSkeleton?feed={RANDOM_FEED_URI}&limit=5"
            )

        set_metric_collector(None)
        assert resp.status_code == 200
        metric_calls = [c for c in collector.calls if c[0] == "feed.mean_diversity_score"]
        assert len(metric_calls) == 0
```

- [ ] **Step 2: Run new tests to confirm they fail**

```
pytest src/app/routers/xrpc_test.py::TestDiversityScoreMetric -v
```

Expected: `ImportError` on `set_metric_collector` or assertion failures — metric not yet emitted.

- [ ] **Step 3: Update `InMemoryFeedCache` in `xrpc_test.py` to match the new interface**

Replace the `InMemoryFeedCache` class (lines ~96–114):

```python
class InMemoryFeedCache(FeedCache):
    """Trivial in-memory feed cache for tests."""

    def __init__(self):
        from ..lib.feed_cache import CachedFeed as _CachedFeed
        self._CachedFeed = _CachedFeed
        self._store: dict[str, _CachedFeed] = {}

    async def store(
        self,
        key: str,
        items: list[str],
        scores: list[float] | None = None,
        ttl_seconds: int = 600,
    ) -> None:
        self._store[key] = self._CachedFeed(items=items, diversity_scores=scores)

    async def retrieve(self, key: str):
        return self._store.get(key)

    async def append(
        self,
        key: str,
        new_items: list[str],
        new_scores: list[float] | None = None,
    ):
        existing = self._store.get(key)
        if existing is None:
            return None
        if existing.diversity_scores is not None and new_scores is not None:
            updated_scores = existing.diversity_scores + new_scores
        else:
            updated_scores = None
        updated = self._CachedFeed(
            items=existing.items + new_items,
            diversity_scores=updated_scores,
        )
        self._store[key] = updated
        return updated
```

Also add `from ..lib.metrics import set_metric_collector` to the imports at the top of `xrpc_test.py` (if not already there via the new test class).

- [ ] **Step 4: Update `_run_ranking_pipeline` in `src/app/routers/xrpc.py` to return scores**

Change the function signature and return type, and extract diversity scores from the final candidates:

```python
async def _run_ranking_pipeline(
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    es,
) -> tuple[list[str], list[float] | None]:
```

At the end of `_run_ranking_pipeline`, replace:
```python
        final = mmr_rerank(ordered) if feed_cfg.diversify else ordered
        final_uris = [c.at_uri for c in final if c.at_uri]
        if rec is not None:
            rec.record_final_order(final_uris)
        return final_uris
```

with:
```python
        final = mmr_rerank(ordered) if feed_cfg.diversify else ordered
        final_uris = [c.at_uri for c in final if c.at_uri]
        if rec is not None:
            rec.record_final_order(final_uris)
        if feed_cfg.diversify:
            diversity_scores: list[float] | None = [
                c.diversity_score for c in final if c.at_uri and c.diversity_score is not None
            ]
            if len(diversity_scores) != len(final_uris):
                diversity_scores = None
        else:
            diversity_scores = None
        return final_uris, diversity_scores
```

- [ ] **Step 5: Update `_run_pipeline_capturing` to thread the tuple**

Change the function signature and both return sites:

```python
async def _run_pipeline_capturing(
    request: Request,
    db,
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    *,
    feed_name: str,
    user_did: str,
    request_id: str,
    regenerated: bool,
    debug_enabled: bool,
) -> tuple[list[str], list[float] | None]:
    if not debug_enabled:
        return await _run_ranking_pipeline(feed_cfg, gen_request, request.app.state.es)

    recorder = FeedDebugRecorder(feed_name=feed_name, regenerated=regenerated)
    generated_at = datetime.now(timezone.utc)
    with feed_debug_scope(recorder):
        uris, scores = await _run_ranking_pipeline(feed_cfg, gen_request, request.app.state.es)
    _spawn_background(_write_feed_debug(request, db, recorder, request_id, user_did, generated_at))
    return uris, scores
```

- [ ] **Step 6: Add `_log_diversity_metric` helper and update `getFeedSkeleton` to thread scores and emit the metric**

Add this small helper near the pagination helpers section in `xrpc.py` (around line 355):

```python
def _log_diversity_metric(
    scores: list[float] | None,
    all_items: list[str],
    page: list[str],
    feed_name: str,
    batch: int,
) -> None:
    """Emit mean diversity score for a served page when scores are available."""
    if scores is None or len(scores) != len(all_items) or not page:
        return
    from ..lib.metrics import get_metric_collector
    mc = get_metric_collector()
    if mc is None:
        return
    offset = all_items.index(page[0]) if page[0] in all_items else None
    if offset is None:
        return
    page_scores = scores[offset : offset + len(page)]
    if page_scores:
        mc.record("feed.mean_diversity_score", sum(page_scores) / len(page_scores), feed_name=feed_name, batch=str(batch))
```

Actually, computing the offset from `all_items.index(page[0])` is fragile (duplicate URIs, cache slicing). A cleaner approach is to thread the absolute offset through directly. Here is the updated serve logic instead — update the three serve points directly in `getFeedSkeleton`:

**Serve point 1 — cached page in range** (around line 716–732). Replace:
```python
            if cached_uris is not None:
                if parsed.offset < len(cached_uris):
                    # Serve from the existing cached batch.
                    page = cached_uris[parsed.offset : parsed.offset + limit]
```
with:
```python
            if cached is not None:
                cached_uris = cached.items
                cached_scores = cached.diversity_scores
                if parsed.offset < len(cached_uris):
                    # Serve from the existing cached batch.
                    page = cached_uris[parsed.offset : parsed.offset + limit]
```
And after building `page`, before the `return`, add:
```python
                    _log_diversity_metric(
                        cached_scores,
                        cached_uris,
                        parsed.offset,
                        len(page),
                        feed_name,
                        batch=parsed.offset // limit,
                    )
```

**Serve point 2 — regeneration append** (around line 759–772). Replace:
```python
                new_uris = await _run_pipeline_capturing(...)
                if new_uris:
                    async with timed(logger, "feedcache_append", cache_id=parsed.id):
                        updated = await feed_cache.append(parsed.id, new_uris)
                    if updated is not None:
                        page = new_uris[:limit]
```
with:
```python
                new_uris, new_scores = await _run_pipeline_capturing(...)
                if new_uris:
                    async with timed(logger, "feedcache_append", cache_id=parsed.id):
                        updated = await feed_cache.append(parsed.id, new_uris, new_scores)
                    if updated is not None:
                        page = new_uris[:limit]
                        _log_diversity_metric(
                            new_scores,
                            new_uris,
                            0,
                            len(page),
                            feed_name,
                            batch=parsed.offset // limit,
                        )
```

**Serve point 3 — initial fresh batch** (around line 793–813). Replace:
```python
        all_uris = await _run_pipeline_capturing(...)
        ...
        page = all_uris[:limit]
        ...
        if len(all_uris) > limit:
            async with timed(logger, "feedcache_store", cache_id=request_id):
                await feed_cache.store(request_id, all_uris)
```
with:
```python
        all_uris, all_scores = await _run_pipeline_capturing(...)
        ...
        page = all_uris[:limit]
        _log_diversity_metric(all_scores, all_uris, 0, len(page), feed_name, batch=0)
        ...
        if len(all_uris) > limit:
            async with timed(logger, "feedcache_store", cache_id=request_id):
                await feed_cache.store(request_id, all_uris, all_scores)
```

Define `_log_diversity_metric` so it takes the slice parameters explicitly (no fragile index lookup):

```python
def _log_diversity_metric(
    scores: list[float] | None,
    all_items: list[str],
    page_offset: int,
    page_len: int,
    feed_name: str,
    batch: int,
) -> None:
    from ..lib.metrics import get_metric_collector
    if scores is None or len(scores) != len(all_items) or page_len == 0:
        return
    mc = get_metric_collector()
    if mc is None:
        return
    page_scores = scores[page_offset : page_offset + page_len]
    if page_scores:
        mc.record(
            "feed.mean_diversity_score",
            sum(page_scores) / len(page_scores),
            feed_name=feed_name,
            batch=str(batch),
        )
```

Also update the `retrieve` call at line 715:
```python
            # before:  cached_uris = await feed_cache.retrieve(parsed.id)
            # after:
            cached = await feed_cache.retrieve(parsed.id)
```

And all downstream references to `cached_uris` in that block use the local `cached_uris = cached.items` assignment made at serve point 1.

- [ ] **Step 7: Add `from ..lib.metrics import get_metric_collector` guard**

`get_metric_collector` is already imported in places; the `_log_diversity_metric` helper does a local import to avoid circular imports at module level. No top-level import change needed for this function.

- [ ] **Step 8: Run the new metric tests**

```
pytest src/app/routers/xrpc_test.py::TestDiversityScoreMetric -v
```

Expected: all 3 tests PASS.

- [ ] **Step 9: Run full test suite**

```
pytest src/ -v
```

Expected: all tests PASS.

- [ ] **Step 10: Commit**

```bash
git add src/app/routers/xrpc.py src/app/routers/xrpc_test.py
git commit -m "thread diversity scores through pipeline and cache; emit feed.mean_diversity_score metric"
```

---

## Final check

After all tasks are done, run:
```
pytest src/ -v
```
All tests should pass. The metric `feed.mean_diversity_score` will be a Float64Histogram in GCP Cloud Monitoring (no `_count`/`_rate` suffix), with labels `endpoint`, `feed_name`, and `batch`.

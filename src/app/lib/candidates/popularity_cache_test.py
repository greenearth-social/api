"""Tests for the shared popularity candidate pool cache."""

from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from ...documents import PopularityCacheDocument
from ...models import CandidatePost
from .popularity_cache import (
    MAX_PAYLOAD_BYTES,
    PAYLOAD_FORMAT,
    POPULARITY_CACHE_COLLECTION,
    PopularityCache,
    PopularityPool,
    _LocalEntry,
    _now_monotonic,
    _serialize_within_limit,
    deserialize_pool,
    get_popularity_cache,
    pool_key,
    serialize_pool,
    set_popularity_cache,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _cache_windows(monkeypatch):
    """Pin the cache windows so tests don't depend on the shipped defaults."""
    monkeypatch.setenv("GE_POPULARITY_CACHE_TTL_SEC", "300")
    monkeypatch.setenv("GE_POPULARITY_CACHE_LOCAL_TTL_SEC", "30")
    monkeypatch.setenv("GE_POPULARITY_CACHE_LEASE_SEC", "60")
    monkeypatch.setenv("GE_POPULARITY_CACHE_POOL_SIZE", "50")


def _candidates(n: int, *, prefix: str = "at://post/") -> list[CandidatePost]:
    return [
        CandidatePost(
            at_uri=f"{prefix}{i}",
            author_did=f"did:plc:author{i}",
            content=f"post number {i}",
            score=float(n - i),
            generator_name="popularity",
            like_count=n - i,
        )
        for i in range(n)
    ]


def _cache() -> tuple[PopularityCache, AsyncMock]:
    """A cache over a mock Firestore, plus the single document ref it uses."""
    db = MagicMock()
    doc_ref = AsyncMock()
    collection_ref = MagicMock()
    collection_ref.document.return_value = doc_ref
    db.collection.return_value = collection_ref
    return PopularityCache(db), doc_ref


def _snapshot(data: dict | None):
    snap = MagicMock()
    snap.exists = data is not None
    snap.to_dict.return_value = data
    return snap


def _document(candidates: list[CandidatePost], *, age_seconds: float = 0.0, **overrides) -> dict:
    doc = PopularityCacheDocument(
        generated_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        payload=serialize_pool(candidates),
        payload_format=PAYLOAD_FORMAT,
        count=len(candidates),
    )
    return {**doc.model_dump(), **overrides}


def _local_entry(candidates: list[CandidatePost], *, age_seconds: float) -> _LocalEntry:
    return _LocalEntry(
        pool=PopularityPool(
            candidates=candidates,
            generated_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        ),
        fetched_at=_now_monotonic(),
    )


class FakeTransaction:
    """Stand-in for a Firestore transaction: records writes, applies nothing."""

    def __init__(self):
        self.writes: list[tuple] = []

    def set(self, ref, data, merge=False):
        self.writes.append((ref, data, merge))


def _direct_transactions(cache: PopularityCache) -> FakeTransaction:
    """Run claim bodies against a fake transaction instead of Firestore."""
    transaction = FakeTransaction()

    async def _run(body):
        return await body(transaction)

    cache._run_transaction = _run  # type: ignore[method-assign]
    return transaction


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_roundtrip_preserves_candidates(self):
        candidates = _candidates(5)
        restored = deserialize_pool(serialize_pool(candidates))
        assert restored == candidates

    def test_payload_is_compressed(self):
        candidates = _candidates(200)
        payload = serialize_pool(candidates)
        raw_text = sum(len(c.content or "") + len(c.at_uri or "") for c in candidates)
        assert len(payload) < raw_text

    def test_oversized_pool_is_trimmed_to_fit(self):
        # Incompressible content, so the pool blows past the Firestore document
        # limit and the least-popular entries have to be dropped.
        candidates = [
            CandidatePost(at_uri=f"at://post/{i}", content=secrets.token_hex(1500))
            for i in range(500)
        ]

        payload, stored = _serialize_within_limit(candidates)

        assert len(payload) <= MAX_PAYLOAD_BYTES
        assert 0 < len(stored) < len(candidates)
        assert stored[0].at_uri == "at://post/0"


class TestPoolKey:
    def test_distinguishes_freshness_and_video(self):
        assert pool_key(video_only=False, max_age_hours=168) == "168h"
        assert pool_key(video_only=True, max_age_hours=168) == "168h-video"
        assert pool_key(video_only=False, max_age_hours=24) != pool_key(
            video_only=False, max_age_hours=168
        )


# ---------------------------------------------------------------------------
# get_pool
# ---------------------------------------------------------------------------

class TestGetPool:
    @pytest.mark.asyncio
    async def test_returns_fresh_entry_without_refreshing(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(3), age_seconds=10))
        fetch = AsyncMock()

        pool = await cache.get_pool(video_only=False, max_age_hours=168, fetch=fetch)

        assert pool is not None
        assert [c.at_uri for c in pool.candidates] == [
            "at://post/0",
            "at://post/1",
            "at://post/2",
        ]
        cache._db.collection.assert_called_with(POPULARITY_CACHE_COLLECTION)
        await cache.drain()
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_entry_returns_none_and_populates_in_background(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(None)
        _direct_transactions(cache)
        fetch = AsyncMock(return_value=_candidates(4))

        pool = await cache.get_pool(video_only=False, max_age_hours=168, fetch=fetch)
        assert pool is None

        await cache.drain()
        fetch.assert_awaited_once_with(50)  # GE_POPULARITY_CACHE_POOL_SIZE
        doc_ref.set.assert_awaited_once()
        stored = doc_ref.set.call_args[0][0]
        assert stored["count"] == 4
        assert stored["payload_format"] == PAYLOAD_FORMAT
        assert stored["refresh_started_at"] is None

    @pytest.mark.asyncio
    async def test_stale_entry_is_served_while_refreshing(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(3), age_seconds=600))
        _direct_transactions(cache)
        fetch = AsyncMock(return_value=_candidates(6, prefix="at://fresh/"))

        pool = await cache.get_pool(video_only=False, max_age_hours=168, fetch=fetch)

        # The caller gets the stale pool immediately — it never awaits the query.
        assert pool is not None
        assert pool.candidates[0].at_uri == "at://post/0"

        await cache.drain()
        fetch.assert_awaited_once()
        assert doc_ref.set.call_args[0][0]["count"] == 6

    @pytest.mark.asyncio
    async def test_unknown_payload_format_is_a_miss(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(
            _document(_candidates(3), payload_format="gzip-json-v99")
        )
        _direct_transactions(cache)

        pool = await cache.get_pool(
            video_only=False, max_age_hours=168, fetch=AsyncMock(return_value=[])
        )

        assert pool is None
        await cache.drain()

    @pytest.mark.asyncio
    async def test_lease_only_document_is_a_miss(self):
        """A document holding just a refresh lease has no pool to serve."""
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot({"refresh_started_at": datetime.now(timezone.utc)})
        _direct_transactions(cache)

        pool = await cache.get_pool(
            video_only=False, max_age_hours=168, fetch=AsyncMock(return_value=[])
        )

        assert pool is None
        await cache.drain()

    @pytest.mark.asyncio
    async def test_corrupt_payload_is_a_miss(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(3), payload=b"not gzip at all"))
        _direct_transactions(cache)

        pool = await cache.get_pool(
            video_only=False, max_age_hours=168, fetch=AsyncMock(return_value=[])
        )

        assert pool is None
        await cache.drain()

    @pytest.mark.asyncio
    async def test_firestore_failure_degrades_to_none(self):
        cache, doc_ref = _cache()
        doc_ref.get.side_effect = RuntimeError("firestore is down")
        fetch = AsyncMock()

        pool = await cache.get_pool(video_only=False, max_age_hours=168, fetch=fetch)

        assert pool is None
        # A read failure must not also kick off a refresh that would fail on
        # the same Firestore; the caller falls back to querying ES directly.
        await cache.drain()
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_second_call_is_served_from_memory(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(3), age_seconds=5))

        await cache.get_pool(video_only=False, max_age_hours=168, fetch=AsyncMock())
        await cache.get_pool(video_only=False, max_age_hours=168, fetch=AsyncMock())

        assert doc_ref.get.await_count == 1

    @pytest.mark.asyncio
    async def test_stale_local_entry_rereads_firestore(self):
        """A stale in-process copy must not hide another instance's refresh."""
        cache, doc_ref = _cache()
        cache._local[pool_key(video_only=False, max_age_hours=168)] = _local_entry(
            _candidates(2), age_seconds=600
        )
        doc_ref.get.return_value = _snapshot(
            _document(_candidates(3, prefix="at://other-instance/"), age_seconds=5)
        )

        pool = await cache.get_pool(video_only=False, max_age_hours=168, fetch=AsyncMock())

        assert pool is not None
        assert pool.candidates[0].at_uri == "at://other-instance/0"

    @pytest.mark.asyncio
    async def test_keys_are_cached_independently(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(3), age_seconds=5))

        await cache.get_pool(video_only=False, max_age_hours=168, fetch=AsyncMock())
        await cache.get_pool(video_only=False, max_age_hours=24, fetch=AsyncMock())

        assert doc_ref.get.await_count == 2


# ---------------------------------------------------------------------------
# Refresh single-flighting
# ---------------------------------------------------------------------------

class TestRefreshLease:
    @pytest.mark.asyncio
    async def test_claims_when_entry_is_stale(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(1), age_seconds=600))
        transaction = _direct_transactions(cache)

        assert await cache._claim_refresh("168h") is True
        assert transaction.writes[0][1]["refresh_started_at"] is not None
        assert transaction.writes[0][2] is True  # merged, so the payload survives

    @pytest.mark.asyncio
    async def test_claims_when_entry_is_missing(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(None)
        _direct_transactions(cache)

        assert await cache._claim_refresh("168h") is True

    @pytest.mark.asyncio
    async def test_does_not_claim_when_another_instance_already_refreshed(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(1), age_seconds=5))
        transaction = _direct_transactions(cache)

        assert await cache._claim_refresh("168h") is False
        assert transaction.writes == []

    @pytest.mark.asyncio
    async def test_does_not_claim_under_a_live_lease(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(
            _document(
                _candidates(1),
                age_seconds=600,
                refresh_started_at=datetime.now(timezone.utc) - timedelta(seconds=5),
            )
        )
        _direct_transactions(cache)

        assert await cache._claim_refresh("168h") is False

    @pytest.mark.asyncio
    async def test_claims_after_an_expired_lease(self):
        """An instance that died mid-refresh must not freeze the entry forever."""
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(
            _document(
                _candidates(1),
                age_seconds=600,
                refresh_started_at=datetime.now(timezone.utc) - timedelta(seconds=120),
            )
        )
        _direct_transactions(cache)

        assert await cache._claim_refresh("168h") is True

    @pytest.mark.asyncio
    async def test_naive_timestamps_from_firestore_are_treated_as_utc(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(
            _document(
                _candidates(1),
                generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        )
        _direct_transactions(cache)

        # A naive "now" must read as fresh, not as 1970.
        assert await cache._claim_refresh("168h") is False

    @pytest.mark.asyncio
    async def test_concurrent_stale_reads_spawn_one_refresh(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(2), age_seconds=600))
        _direct_transactions(cache)
        fetch = AsyncMock(return_value=_candidates(3))

        await asyncio.gather(
            *(cache.get_pool(video_only=False, max_age_hours=168, fetch=fetch) for _ in range(5))
        )
        await cache.drain()

        fetch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_query_result_is_not_stored(self):
        """Never overwrite a usable pool with an empty one."""
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(2), age_seconds=600))
        _direct_transactions(cache)

        await cache.get_pool(
            video_only=False, max_age_hours=168, fetch=AsyncMock(return_value=[])
        )
        await cache.drain()

        doc_ref.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refresh_failure_leaves_the_stale_entry_servable(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(_document(_candidates(2), age_seconds=600))
        _direct_transactions(cache)

        await cache.get_pool(
            video_only=False,
            max_age_hours=168,
            fetch=AsyncMock(side_effect=RuntimeError("elasticsearch is down")),
        )
        await cache.drain()

        # The key is released, so a later request can retry the refresh.
        assert cache._refreshing == set()
        pool = await cache.get_pool(
            video_only=False, max_age_hours=168, fetch=AsyncMock(return_value=[])
        )
        assert pool is not None


    @pytest.mark.asyncio
    async def test_refresh_writes_under_the_requested_key(self):
        cache, doc_ref = _cache()
        doc_ref.get.return_value = _snapshot(None)
        _direct_transactions(cache)

        await cache.get_pool(
            video_only=True, max_age_hours=24, fetch=AsyncMock(return_value=_candidates(7))
        )
        await cache.drain()

        cache._db.collection.return_value.document.assert_called_with("24h-video")
        assert doc_ref.set.call_args[0][0]["count"] == 7


# ---------------------------------------------------------------------------
# Process-level accessor
# ---------------------------------------------------------------------------

class TestAccessor:
    def test_set_and_get(self):
        cache, _doc_ref = _cache()
        assert get_popularity_cache() is None
        set_popularity_cache(cache)
        try:
            assert get_popularity_cache() is cache
        finally:
            set_popularity_cache(None)
        assert get_popularity_cache() is None

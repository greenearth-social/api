"""Exercise Explore history reuse through both model consumers."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from ..models import CandidatePost
from . import user_history_cache as history_module
from .average_user_embedding import AverageUserEmbedding
from .candidates import two_tower
from .embeddings import encode_float32_b64
from .inference import UserEmbeddingResult
from .rankers import heavy_ranker
from .request_cache import request_cache_scope
from .user_history_cache import UserHistory, UserHistoryCache, UserHistoryCacheEntry


class _MemoryHistoryCache(UserHistoryCache):
    def __init__(self):
        super().__init__()
        self.entry: UserHistoryCacheEntry | None = None
        self.reads = 0
        self.writes: list[UserHistory] = []

    async def retrieve(self, user_did: str) -> UserHistoryCacheEntry | None:
        self.reads += 1
        return self.entry

    async def store(self, user_did: str, history: UserHistory) -> None:
        self.writes.append(history)
        self.entry = UserHistoryCacheEntry(history, datetime.now(UTC))


@pytest.mark.asyncio
async def test_explore_consumers_wait_for_shared_history_then_reuse_it(monkeypatch):
    # Keep both consumers and the history loader real. Only persistence,
    # Elasticsearch, and model inference are replaced with local test doubles.
    cache = _MemoryHistoryCache()
    monkeypatch.setattr(history_module, "get_user_history_cache", lambda: cache)
    started = asyncio.Event()
    release = asyncio.Event()
    liked_at = "2026-11-01T00:00:00Z"

    async def fetch_likes(es, user_did, *, limit):
        started.set()
        await release.wait()
        return ["at://liked/1"], [liked_at]

    async def hydrate_history(es, uris, *, index):
        return [("at://liked/1", [0.0, 1.0], "did:plc:author", 12)] if index == "posts" else []

    likes = AsyncMock(side_effect=fetch_likes)
    hydrate = AsyncMock(side_effect=hydrate_history)
    monkeypatch.setattr(history_module, "fetch_recent_liked_post_uris_and_times", likes)
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)

    prior = AverageUserEmbedding((1.0, 0.0), 2, "1" * 32, "2" * 32, "test-run", 2)
    monkeypatch.setattr(two_tower, "get_average_user_embedding", lambda: prior)
    monkeypatch.setattr(two_tower, "get_average_user_embedding_error", lambda: None)
    for module in (two_tower, heavy_ranker):
        monkeypatch.setattr(module, "get_inference_settings", lambda: ("http://inference", "key"))

    prediction = AsyncMock(
        return_value=UserEmbeddingResult(1, 1, [0.0, 1.0], "1" * 32, "2" * 32)
    )
    candidate = CandidatePost(
        at_uri="at://candidate/1",
        minilm_l12_embedding=encode_float32_b64([1.0, 0.0]),
        author_did="did:plc:candidate-author",
        like_count=3,
    )
    search = AsyncMock(return_value=[candidate])
    rank_prediction = AsyncMock(return_value=[0.5])
    candidate_hydration = AsyncMock()
    monkeypatch.setattr(two_tower, "predict_user_embedding", prediction)
    monkeypatch.setattr(two_tower, "knn_search_posts", search)
    monkeypatch.setattr(heavy_ranker, "predict_heavy_ranker_single_user", rank_prediction)
    monkeypatch.setattr(heavy_ranker, "fetch_post_embeddings_and_metadata", candidate_hydration)
    tower = two_tower.TwoTowerCandidateGenerator("two_tower", "actual")
    ranker = heavy_ranker.HeavyRanker("heavy_ranker", "actual")
    es = object()

    async def run_consumers():
        with history_module.user_history_policy_scope("explore"):
            async with request_cache_scope():
                return await asyncio.gather(
                    tower.generate(es, "did:plc:user"),
                    ranker.predict(es, "did:plc:user", [candidate]),
                )

    consumers = asyncio.create_task(run_consumers())
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not consumers.done()
        assert cache.reads == 1
        assert cache.writes == []
        likes.assert_awaited_once()
        hydrate.assert_not_awaited()
        prediction.assert_not_awaited()
        search.assert_not_awaited()
        rank_prediction.assert_not_awaited()
        release.set()
        generated, ranked = await asyncio.wait_for(consumers, timeout=1)
    finally:
        release.set()
        await asyncio.gather(consumers, return_exceptions=True)
        await cache.drain()

    assert generated.candidates == [candidate]
    assert ranked.result.rankings[0].at_uri == candidate.at_uri
    assert len(cache.writes) == 1
    history = cache.writes[0]
    assert history.liked_uris == ["at://liked/1"]
    prediction.assert_awaited_once_with(history, base_url="http://inference", api_key="key")
    assert rank_prediction.await_args is not None
    assert rank_prediction.await_args.args[:4] == (
        [[0.0, 1.0]], ["did:plc:author"], [liked_at], [12]
    )
    await run_consumers()

    # A new request consults the shared cache once for both consumers and
    # reuses the history that already personalized the first preview.
    assert cache.reads == 2
    assert len(cache.writes) == 1
    likes.assert_awaited_once()
    assert hydrate.await_count == 2
    assert prediction.await_count == 2
    assert rank_prediction.await_count == 2
    assert rank_prediction.await_args is not None
    assert rank_prediction.await_args.args[:4] == (
        [[0.0, 1.0]], ["did:plc:author"], [liked_at], [12]
    )
    candidate_hydration.assert_not_awaited()

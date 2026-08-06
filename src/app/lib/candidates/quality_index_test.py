"""Tests for routing two-tower kNN at the lean quality corpus.

Background (greenearth-social/ingex#442): ``like_count>=20`` matches ~4.6% of
``posts_recent``, which puts Lucene past its filtered-kNN work budget — it
abandons the HNSW graph and exact-scans every matching vector (~320k per query
at the 96h cap). ``posts_recent_quality`` contains *only* posts at or above that
threshold, so the same filter matches ~100% of the index, neither Lucene
fallback triggers, and the graph search is used again.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ..elasticsearch import (
    POSTS_KNN_INDEX,
    POSTS_QUALITY_KNN_INDEX,
    two_tower_knn_index,
)
from ..embeddings import GE_POST_EMBEDDING_FIELD
from .es_candidates import knn_search_posts
from .es_candidates_test import FakeEs
from .two_tower import MIN_LIKE_COUNT, TwoTowerCandidateGenerator

INFERENCE_SETTINGS = ("https://inference", "api-key")
GET_INFERENCE_SETTINGS = "app.lib.candidates.two_tower.get_inference_settings"
COMPUTE_USER_EMBEDDING = "app.lib.candidates.two_tower.compute_user_embedding"
GET_CACHED_POST_TOWER_UUID = "app.lib.candidates.two_tower.get_cached_post_tower_uuid"
KNN_SEARCH_POSTS = "app.lib.candidates.two_tower.knn_search_posts"


class TestTwoTowerKnnIndexSelection:
    def test_defaults_to_the_quality_corpus(self, monkeypatch):
        monkeypatch.delenv("GE_TWO_TOWER_KNN_INDEX", raising=False)
        assert two_tower_knn_index() == POSTS_QUALITY_KNN_INDEX

    def test_env_var_can_pin_it_back_to_posts_recent(self, monkeypatch):
        # The escape hatch for rollout: if the quality corpus is not yet
        # backfilled in an environment, this restores the previous behaviour
        # without a code change.
        monkeypatch.setenv("GE_TWO_TOWER_KNN_INDEX", POSTS_KNN_INDEX)
        assert two_tower_knn_index() == POSTS_KNN_INDEX

    def test_blank_env_var_falls_back_to_the_default(self, monkeypatch):
        monkeypatch.setenv("GE_TWO_TOWER_KNN_INDEX", "   ")
        assert two_tower_knn_index() == POSTS_QUALITY_KNN_INDEX


class TestKnnSearchPostsIndexParam:
    @pytest.mark.asyncio
    async def test_searches_the_index_it_is_given(self):
        es = FakeEs(responses={
            POSTS_QUALITY_KNN_INDEX: {
                "hits": {"hits": [{"_score": 0.9, "_source": {"at_uri": "at://post/1"}}]}
            }
        })

        candidates = await knn_search_posts(
            es,
            [0.1, 0.2],
            num_candidates=10,
            search_field=GE_POST_EMBEDDING_FIELD,
            index=POSTS_QUALITY_KNN_INDEX,
        )

        assert [c.at_uri for c in candidates] == ["at://post/1"]
        assert es.calls[0]["index"] == POSTS_QUALITY_KNN_INDEX

    @pytest.mark.asyncio
    async def test_defaults_to_posts_recent_so_other_generators_are_unaffected(self):
        # popularity, random_posts, followed_users and the rest must keep
        # reading the full corpus.
        es = FakeEs()
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=10, search_field=GE_POST_EMBEDDING_FIELD
        )
        assert es.calls[0]["index"] == POSTS_KNN_INDEX

    @pytest.mark.asyncio
    async def test_min_like_count_filter_is_emitted_when_a_caller_passes_one(self):
        # knn_search_posts itself stays a plain query builder: it emits the
        # filter whenever a caller supplies min_like_count, regardless of which
        # index that caller chose. Whether to pass one is two_tower's decision
        # (see TestTwoTowerUsesQualityIndex below), not this function's.
        es = FakeEs()
        await knn_search_posts(
            es,
            [0.1, 0.2],
            num_candidates=10,
            search_field=GE_POST_EMBEDDING_FIELD,
            index=POSTS_KNN_INDEX,
            min_like_count=20,
        )
        filters = es.calls[0]["knn"]["filter"]["bool"]["filter"]
        assert {"range": {"like_count": {"gte": 20}}} in filters

    @pytest.mark.asyncio
    async def test_min_like_count_filter_is_omitted_when_a_caller_passes_none(self):
        es = FakeEs()
        await knn_search_posts(
            es,
            [0.1, 0.2],
            num_candidates=10,
            search_field=GE_POST_EMBEDDING_FIELD,
            index=POSTS_QUALITY_KNN_INDEX,
            min_like_count=None,
        )
        filters = es.calls[0]["knn"]["filter"]["bool"]["filter"]
        assert not any("like_count" in str(f) for f in filters)


class TestTwoTowerUsesQualityIndex:
    @pytest.mark.asyncio
    async def test_generate_searches_the_quality_corpus(self, monkeypatch):
        monkeypatch.delenv("GE_TWO_TOWER_KNN_INDEX", raising=False)
        generator = TwoTowerCandidateGenerator(name="two_tower", history_mode="actual")

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(GET_CACHED_POST_TOWER_UUID, new_callable=AsyncMock, return_value="uuid-1"),
            patch(COMPUTE_USER_EMBEDDING, new_callable=AsyncMock, return_value=[0.1, 0.2]),
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn_search,
        ):
            await generator.generate(object(), "did:plc:user1", num_candidates=10)

        assert knn_search.await_args is not None
        kwargs = knn_search.await_args.kwargs
        assert kwargs["index"] == POSTS_QUALITY_KNN_INDEX
        # No traction filter against the quality corpus: membership already
        # guarantees it, so reapplying MIN_LIKE_COUNT here would only add a
        # cross-repo constant that has to track ingex's own promotion
        # threshold for no behavioral benefit.
        assert kwargs["min_like_count"] is None
        assert kwargs["max_age_hours"] == 168

    @pytest.mark.asyncio
    async def test_generate_honours_the_index_override(self, monkeypatch):
        monkeypatch.setenv("GE_TWO_TOWER_KNN_INDEX", POSTS_KNN_INDEX)
        generator = TwoTowerCandidateGenerator(name="two_tower", history_mode="actual")

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(GET_CACHED_POST_TOWER_UUID, new_callable=AsyncMock, return_value="uuid-1"),
            patch(COMPUTE_USER_EMBEDDING, new_callable=AsyncMock, return_value=[0.1, 0.2]),
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn_search,
        ):
            await generator.generate(object(), "did:plc:user1", num_candidates=10)

        assert knn_search.await_args is not None
        kwargs = knn_search.await_args.kwargs
        assert kwargs["index"] == POSTS_KNN_INDEX
        # Pinned back to the full corpus, MIN_LIKE_COUNT is the only thing
        # enforcing any traction preference at all — it must still be applied.
        assert kwargs["min_like_count"] == MIN_LIKE_COUNT

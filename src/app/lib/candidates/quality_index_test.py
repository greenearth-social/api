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
from .two_tower import MIN_LIKE_COUNT, TWO_TOWER_MAX_AGE_CAP_HOURS, TwoTowerCandidateGenerator

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
    async def test_like_count_filter_is_still_applied(self):
        # Kept deliberately. Against the quality corpus it is non-selective, so
        # it costs nothing and does not trip Lucene's fallbacks — but it keeps
        # the query's semantics identical to today's (a pre-filter, not a
        # post-filter) and screens out any member whose count later drifts.
        es = FakeEs()
        await knn_search_posts(
            es,
            [0.1, 0.2],
            num_candidates=10,
            search_field=GE_POST_EMBEDDING_FIELD,
            index=POSTS_QUALITY_KNN_INDEX,
            min_like_count=20,
        )
        filters = es.calls[0]["knn"]["filter"]["bool"]["filter"]
        assert {"range": {"like_count": {"gte": 20}}} in filters


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

        assert knn_search.await_args.kwargs["index"] == POSTS_QUALITY_KNN_INDEX
        # The pre-filter and the window cap are unchanged by this switch.
        assert knn_search.await_args.kwargs["min_like_count"] == MIN_LIKE_COUNT
        assert knn_search.await_args.kwargs["max_age_hours"] == TWO_TOWER_MAX_AGE_CAP_HOURS

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

        assert knn_search.await_args.kwargs["index"] == POSTS_KNN_INDEX

"""Tests for the two-tower candidate generator."""

from unittest.mock import AsyncMock, patch

import pytest

from ...models import CandidatePost
from ..candidates import get_generator, list_generators
from ..candidates.two_tower import (
    TWO_TOWER_GENERATOR_NAME,
    TwoTowerCandidateGenerator,
)
from ..embeddings import GE_POST_EMBEDDING_FIELD

GET_INFERENCE_SETTINGS = "app.lib.candidates.two_tower.get_inference_settings"
COMPUTE_USER_EMBEDDING = "app.lib.candidates.two_tower.compute_user_embedding"
GET_CACHED_POST_TOWER_UUID = "app.lib.candidates.two_tower.get_cached_post_tower_uuid"
KNN_SEARCH_POSTS = "app.lib.candidates.two_tower.knn_search_posts"
INFERENCE_SETTINGS = ("https://inference", "api-key")


@pytest.fixture
def generator():
    return TwoTowerCandidateGenerator()


class TestTwoTowerCandidateGenerator:
    def test_name(self, generator):
        assert generator.name == TWO_TOWER_GENERATOR_NAME

    def test_registered_as_builtin_generator(self):
        registered = get_generator(TWO_TOWER_GENERATOR_NAME)
        assert isinstance(registered, TwoTowerCandidateGenerator)
        assert TWO_TOWER_GENERATOR_NAME in list_generators()

    @pytest.mark.asyncio
    async def test_generate_runs_user_tower_then_ge_post_knn(self, generator):
        es = object()
        user_embedding = [0.1, 0.2, 0.3]
        candidates = [
            CandidatePost(
                at_uri="at://post/1",
                content="one",
                score=0.9,
                generator_name=TWO_TOWER_GENERATOR_NAME,
            ),
            CandidatePost(
                at_uri="at://post/2",
                content="two",
                score=0.8,
                generator_name=TWO_TOWER_GENERATOR_NAME,
            ),
        ]

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS) as settings,
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value="post-tower-uuid",
            ) as get_post_tower_uuid,
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
                return_value=user_embedding,
            ) as compute_user_embedding,
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
                return_value=candidates,
            ) as knn_search,
        ):
            result = await generator.generate(
                es,
                "did:plc:user1",
                num_candidates=12,
                video_only=True,
                exclude_uris=["at://old/1", "at://old/2"],
            )

        settings.assert_called_once_with()
        get_post_tower_uuid.assert_awaited_once_with("https://inference", "api-key")
        compute_user_embedding.assert_awaited_once_with(
            "did:plc:user1",
            es,
            "https://inference",
            "api-key",
            TWO_TOWER_GENERATOR_NAME,
        )
        knn_search.assert_awaited_once_with(
            es,
            user_embedding,
            12,
            search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=TWO_TOWER_GENERATOR_NAME,
            video_only=True,
            exclude_uris=["at://old/1", "at://old/2"],
            ge_post_embedding_model_uuid="post-tower-uuid",
        )
        assert result.generator_name == TWO_TOWER_GENERATOR_NAME
        assert result.candidates == candidates

    @pytest.mark.asyncio
    async def test_generate_uses_default_options(self, generator):
        es = object()
        user_embedding = [0.5, 0.6]

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value="post-tower-uuid",
            ),
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
                return_value=user_embedding,
            ),
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
                return_value=[],
            ) as knn_search,
        ):
            result = await generator.generate(es, "did:plc:user1")

        knn_search.assert_awaited_once_with(
            es,
            user_embedding,
            100,
            search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=TWO_TOWER_GENERATOR_NAME,
            video_only=False,
            exclude_uris=None,
            ge_post_embedding_model_uuid="post-tower-uuid",
        )
        assert result.generator_name == TWO_TOWER_GENERATOR_NAME
        assert result.candidates == []

    @pytest.mark.asyncio
    async def test_generate_allows_zero_candidates_passthrough(self, generator):
        es = object()
        user_embedding = [0.5, 0.6]

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value="post-tower-uuid",
            ),
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
                return_value=user_embedding,
            ),
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
                return_value=[],
            ) as knn_search,
        ):
            result = await generator.generate(es, "did:plc:user1", num_candidates=0)

        knn_search.assert_awaited_once_with(
            es,
            user_embedding,
            0,
            search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=TWO_TOWER_GENERATOR_NAME,
            video_only=False,
            exclude_uris=None,
            ge_post_embedding_model_uuid="post-tower-uuid",
        )
        assert result.candidates == []

    @pytest.mark.asyncio
    async def test_generate_returns_empty_when_post_tower_uuid_missing(self, generator):
        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value=None,
            ) as get_post_tower_uuid,
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
            ) as compute_user_embedding,
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
            ) as knn_search,
        ):
            result = await generator.generate(object(), "did:plc:user1")

        get_post_tower_uuid.assert_awaited_once_with("https://inference", "api-key")
        compute_user_embedding.assert_not_awaited()
        knn_search.assert_not_awaited()
        assert result.generator_name == TWO_TOWER_GENERATOR_NAME
        assert result.candidates == []

    @pytest.mark.asyncio
    async def test_generate_propagates_post_tower_uuid_errors(self, generator):
        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                side_effect=RuntimeError("ready response malformed"),
            ) as get_post_tower_uuid,
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
            ) as compute_user_embedding,
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
            ) as knn_search,
        ):
            with pytest.raises(RuntimeError, match="ready response malformed"):
                await generator.generate(object(), "did:plc:user1")

        get_post_tower_uuid.assert_awaited_once_with("https://inference", "api-key")
        compute_user_embedding.assert_not_awaited()
        knn_search.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_generate_propagates_settings_errors(self, generator):
        with (
            patch(
                GET_INFERENCE_SETTINGS,
                side_effect=RuntimeError("missing inference settings"),
            ),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
            ) as get_post_tower_uuid,
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
            ) as compute_user_embedding,
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
            ) as knn_search,
        ):
            with pytest.raises(RuntimeError, match="missing inference settings"):
                await generator.generate(object(), "did:plc:user1")

        get_post_tower_uuid.assert_not_awaited()
        compute_user_embedding.assert_not_awaited()
        knn_search.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_generate_propagates_user_embedding_errors(self, generator):
        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value="post-tower-uuid",
            ),
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
                side_effect=RuntimeError("user tower down"),
            ),
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
            ) as knn_search,
        ):
            with pytest.raises(RuntimeError, match="user tower down"):
                await generator.generate(object(), "did:plc:user1")

        knn_search.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_generate_propagates_knn_errors(self, generator):
        user_embedding = [0.5, 0.6]

        with (
            patch(GET_INFERENCE_SETTINGS, return_value=INFERENCE_SETTINGS),
            patch(
                GET_CACHED_POST_TOWER_UUID,
                new_callable=AsyncMock,
                return_value="post-tower-uuid",
            ),
            patch(
                COMPUTE_USER_EMBEDDING,
                new_callable=AsyncMock,
                return_value=user_embedding,
            ) as compute_user_embedding,
            patch(
                KNN_SEARCH_POSTS,
                new_callable=AsyncMock,
                side_effect=RuntimeError("es down"),
            ),
        ):
            with pytest.raises(RuntimeError, match="es down"):
                await generator.generate(object(), "did:plc:user1")

        compute_user_embedding.assert_awaited_once()

"""Tests for the LLM query vector candidate generator."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ...models import CandidatePost
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from .llm_query_vector import LlmQueryVectorCandidateGenerator
from .llm_query_vector_cache import set_llm_query_vector_cache

GENERATOR_NAME = "llm_query_vector"
KNN_SEARCH_POSTS = "app.lib.candidates.llm_query_vector.knn_search_posts"


def _make_vector_doc(prompt_key: str, vector: list[float], updated_at: datetime):
    from ...documents import LlmQueryVectorDocument
    return LlmQueryVectorDocument(
        prompt_key=prompt_key,
        user_did="did:plc:user1",
        query_vector=vector,
        prompt="test prompt",
        updated_at=updated_at,
    )


def _mock_cache(latest_return=None) -> MagicMock:
    cache = MagicMock()
    cache.get_latest = AsyncMock(return_value=latest_return)
    return cache


@pytest.fixture(autouse=True)
def reset_cache():
    set_llm_query_vector_cache(None)
    yield
    set_llm_query_vector_cache(None)


@pytest.fixture
def generator():
    return LlmQueryVectorCandidateGenerator()


class TestLlmQueryVectorCandidateGenerator:
    def test_name(self, generator):
        assert generator.name == GENERATOR_NAME

    @pytest.mark.asyncio
    async def test_returns_empty_when_cache_not_set(self, generator):
        result = await generator.generate(object(), "did:plc:user1")

        assert result.generator_name == GENERATOR_NAME
        assert result.candidates == []
        assert result.status == "not_run"
        assert result.reason == "cache_not_configured"

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_vectors_in_firestore(self, generator):
        set_llm_query_vector_cache(_mock_cache(latest_return=None))

        result = await generator.generate(object(), "did:plc:user1")

        assert result.generator_name == GENERATOR_NAME
        assert result.candidates == []
        assert result.status == "not_run"
        assert result.reason == "no_query_vector"

    @pytest.mark.asyncio
    async def test_queries_es_with_vector_using_minilm_field(self, generator):
        vector = [0.1, 0.2, 0.3]
        doc = _make_vector_doc("key1", vector, updated_at=datetime.now(UTC))
        candidates = [
            CandidatePost(
                at_uri="at://post/1",
                content="green post",
                score=0.9,
                generator_name=GENERATOR_NAME,
            )
        ]
        set_llm_query_vector_cache(_mock_cache(latest_return=doc))
        es = object()

        with patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=candidates) as knn:
            result = await generator.generate(
                es,
                "did:plc:user1",
                num_candidates=50,
                video_only=True,
                exclude_uris=["at://old/1"],
                max_age_hours=48,
            )

        knn.assert_awaited_once_with(
            es,
            vector,
            50,
            search_field=MINILM_L12_EMBEDDING_FIELD,
            generator_name=GENERATOR_NAME,
            video_only=True,
            exclude_uris=["at://old/1"],
            max_age_hours=48,
        )
        assert result.generator_name == GENERATOR_NAME
        assert result.candidates == candidates

    @pytest.mark.asyncio
    async def test_uses_vector_returned_by_cache(self, generator):
        vector = [0.9, 0.8]
        doc = _make_vector_doc("key1", vector, updated_at=datetime(2026, 8, 1, tzinfo=UTC))
        set_llm_query_vector_cache(_mock_cache(latest_return=doc))

        with patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn:
            await generator.generate(object(), "did:plc:user1")

        assert knn.await_args is not None
        assert knn.await_args.args[1] == vector

    @pytest.mark.asyncio
    async def test_passes_user_did_to_cache(self, generator):
        mock_cache = _mock_cache(latest_return=None)
        set_llm_query_vector_cache(mock_cache)

        await generator.generate(object(), "did:plc:specificuser")

        mock_cache.get_latest.assert_awaited_once_with("did:plc:specificuser")

    @pytest.mark.asyncio
    async def test_uses_default_options(self, generator):
        vector = [0.5, 0.6]
        doc = _make_vector_doc("key1", vector, updated_at=datetime.now(UTC))
        set_llm_query_vector_cache(_mock_cache(latest_return=doc))

        with patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn:
            await generator.generate(object(), "did:plc:user1")

        knn.assert_awaited_once()
        call_args = knn.await_args
        assert call_args is not None
        assert call_args.args[1] == vector
        assert call_args.args[2] == 100
        assert call_args.kwargs["search_field"] == MINILM_L12_EMBEDDING_FIELD
        assert call_args.kwargs["generator_name"] == GENERATOR_NAME
        assert call_args.kwargs["video_only"] is False
        assert call_args.kwargs["exclude_uris"] is None
        assert call_args.kwargs["max_age_hours"] == 168

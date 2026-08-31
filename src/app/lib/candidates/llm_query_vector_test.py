"""Tests for the LLM query vector candidate generator."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from ...models import CandidatePost
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from .llm_query_vector import (
    LlmQueryVectorCandidateGenerator,
    get_llm_query_vector_db,
    set_llm_query_vector_db,
)

GENERATOR_NAME = "llm_query_vector"
GET_ALL_VECTORS = "app.lib.candidates.llm_query_vector.get_all_llm_query_vectors"
KNN_SEARCH_POSTS = "app.lib.candidates.llm_query_vector.knn_search_posts"


def _make_vector_doc(prompt_key: str, vector: list[float], updated_at: datetime):
    """Helper: build a minimal LlmQueryVectorDocument-like object."""
    from ...documents import LlmQueryVectorDocument
    return LlmQueryVectorDocument(
        prompt_key=prompt_key,
        user_did="did:plc:user1",
        query_vector=vector,
        prompt="test prompt",
        updated_at=updated_at,
    )


@pytest.fixture(autouse=True)
def reset_db():
    """Reset the module-level db singleton between tests."""
    set_llm_query_vector_db(None)
    yield
    set_llm_query_vector_db(None)


@pytest.fixture
def generator():
    return LlmQueryVectorCandidateGenerator()


class TestDbSingleton:
    def test_default_is_none(self):
        assert get_llm_query_vector_db() is None

    def test_set_and_get(self):
        fake_db = object()
        set_llm_query_vector_db(fake_db)
        assert get_llm_query_vector_db() is fake_db

    def test_set_none_clears(self):
        set_llm_query_vector_db(object())
        set_llm_query_vector_db(None)
        assert get_llm_query_vector_db() is None


class TestLlmQueryVectorCandidateGenerator:
    def test_name(self, generator):
        assert generator.name == GENERATOR_NAME

    @pytest.mark.asyncio
    async def test_returns_empty_when_db_not_set(self, generator):
        result = await generator.generate(object(), "did:plc:user1")

        assert result.generator_name == GENERATOR_NAME
        assert result.candidates == []
        assert result.status == "not_run"
        assert result.reason == "db_not_configured"

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_vectors_in_firestore(self, generator):
        set_llm_query_vector_db(object())

        with patch(GET_ALL_VECTORS, new_callable=AsyncMock, return_value=[]):
            result = await generator.generate(object(), "did:plc:user1")

        assert result.generator_name == GENERATOR_NAME
        assert result.candidates == []
        assert result.status == "not_run"
        assert result.reason == "no_query_vector"

    @pytest.mark.asyncio
    async def test_queries_es_with_vector_using_minilm_field(self, generator):
        fake_db = object()
        set_llm_query_vector_db(fake_db)
        es = object()
        vector = [0.1, 0.2, 0.3]
        now = datetime.now(UTC)
        doc = _make_vector_doc("key1", vector, updated_at=now)
        candidates = [
            CandidatePost(
                at_uri="at://post/1",
                content="green post",
                score=0.9,
                generator_name=GENERATOR_NAME,
            )
        ]

        with (
            patch(GET_ALL_VECTORS, new_callable=AsyncMock, return_value=[doc]),
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=candidates) as knn,
        ):
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
    async def test_picks_most_recently_updated_vector_when_multiple_exist(self, generator):
        set_llm_query_vector_db(object())
        old_vector = [0.1, 0.2]
        new_vector = [0.9, 0.8]
        older = _make_vector_doc(
            "old", old_vector,
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        newer = _make_vector_doc(
            "new", new_vector,
            updated_at=datetime(2026, 8, 1, tzinfo=UTC),
        )

        with (
            patch(GET_ALL_VECTORS, new_callable=AsyncMock, return_value=[older, newer]),
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn,
        ):
            await generator.generate(object(), "did:plc:user1")

        assert knn.await_args is not None
        assert knn.await_args.args[1] == new_vector

    @pytest.mark.asyncio
    async def test_passes_user_did_to_firestore(self, generator):
        fake_db = object()
        set_llm_query_vector_db(fake_db)

        with (
            patch(GET_ALL_VECTORS, new_callable=AsyncMock, return_value=[]) as get_vectors,
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]),
        ):
            await generator.generate(object(), "did:plc:specificuser")

        get_vectors.assert_awaited_once_with(fake_db, "did:plc:specificuser")

    @pytest.mark.asyncio
    async def test_uses_default_options(self, generator):
        set_llm_query_vector_db(object())
        vector = [0.5, 0.6]
        doc = _make_vector_doc("key1", vector, updated_at=datetime.now(UTC))

        with (
            patch(GET_ALL_VECTORS, new_callable=AsyncMock, return_value=[doc]),
            patch(KNN_SEARCH_POSTS, new_callable=AsyncMock, return_value=[]) as knn,
        ):
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

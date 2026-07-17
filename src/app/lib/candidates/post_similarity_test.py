"""Tests for the post_similarity candidate generator."""

import pytest

from ..candidates.post_similarity import (
    PostSimilarityCandidateGenerator,
    average_vectors,
)
from ..embeddings import MINILM_L12_EMBEDDING_KEY

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def generator():
    return PostSimilarityCandidateGenerator()


class FakeEs:
    """Configurable fake Elasticsearch client for unit tests."""

    def __init__(self, responses: dict | None = None):
        # Map of (index, context_key) -> response dict
        self._responses = responses or {}
        self._default = {"hits": {"hits": []}}
        self.calls: list[dict] = []

    async def search(
        self, *, index=None, query=None, knn=None, size=None, sort=None, _source=None, **kwargs
    ):
        self.calls.append({
            "index": index,
            "query": query,
            "knn": knn,
            "size": size,
            "sort": sort,
            "_source": _source,
        })
        return self._responses.get(index, self._default)


class TestAverageVectors:
    def test_single_vector(self):
        assert average_vectors([[1.0, 2.0, 3.0]]) == [1.0, 2.0, 3.0]

    def test_multiple_vectors(self):
        result = average_vectors([[1.0, 0.0], [3.0, 4.0]])
        assert result == [2.0, 2.0]

    def test_raises_on_empty(self):
        with pytest.raises(ValueError, match="No vectors"):
            average_vectors([])

# ---------------------------------------------------------------------------
# Integration-style tests – full generator
# ---------------------------------------------------------------------------

class TestPostSimilarityGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "post_similarity"

    @pytest.mark.asyncio
    async def test_generate_full_pipeline(self, generator):
        """Happy path: user has likes → embeddings found → kNN results."""

        class FullFakeEs:
            async def search(
                self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs
            ):
                if index == "likes":
                    return {
                        "hits": {
                            "hits": [
                                {"_source": {"subject_uri": "at://post/1"}},
                                {"_source": {"subject_uri": "at://post/2"}},
                            ]
                        }
                    }
                if index == "posts":
                    # Check if this is the embedding lookup or the knn search
                    if isinstance(query, dict) and "terms" in query:
                        return {
                            "hits": {
                                "hits": [
                                    {
                                        "_source": {
                                            "at_uri": "at://post/2",
                                            "content": "liked post two",
                                            "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.0, 1.0]},
                                        }
                                    },
                                    {
                                        "_source": {
                                            "at_uri": "at://post/1",
                                            "content": "liked post one",
                                            "embeddings": {MINILM_L12_EMBEDDING_KEY: [1.0, 0.0]},
                                        }
                                    },
                                ]
                            }
                        }
                if index == "posts_recent":
                    # kNN search
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_score": 0.9,
                                    "_source": {
                                        "at_uri": "at://result/1",
                                        "content": "recommended post",
                                    },
                                }
                            ]
                        }
                    }
                return {"hits": {"hits": []}}

        result = await generator.generate(FullFakeEs(), "did:plc:user1", num_candidates=10)

        assert result.generator_name == "post_similarity"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://result/1"
        assert result.candidates[0].score == 0.9
        assert result.candidates[0].minilm_l12_embedding is None
        assert result.candidates[0].generator_name == "post_similarity"

    @pytest.mark.asyncio
    async def test_generate_no_likes(self, generator):
        """User has no likes → empty result."""
        es = FakeEs()
        result = await generator.generate(es, "did:plc:nobody", num_candidates=10)
        assert result.generator_name == "post_similarity"
        assert result.candidates == []

    @pytest.mark.asyncio
    async def test_generate_likes_but_no_embeddings(self, generator):
        """User has likes but the posts have no embeddings → empty result."""

        class LikesOnlyFakeEs:
            async def search(
                self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs
            ):
                if index == "likes":
                    return {
                        "hits": {
                            "hits": [
                                {"_source": {"subject_uri": "at://post/1"}},
                            ]
                        }
                    }
                # posts index returns hits without embeddings
                return {"hits": {"hits": [{"_source": {"embeddings": {}}}]}}

        result = await generator.generate(LikesOnlyFakeEs(), "did:plc:user1", num_candidates=10)
        assert result.candidates == []

"""Tests for the post_similarity candidate generator."""

import pytest

from ..candidates.post_similarity import (
    PostSimilarityCandidateGenerator,
    average_vectors,
    fetch_post_embeddings,
    fetch_recent_liked_post_uris,
    knn_search_posts,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_EMBEDDING = [0.1, 0.2, 0.3]


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

    async def search(self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs):
        self.calls.append({
            "index": index,
            "query": query,
            "size": size,
            "sort": sort,
            "_source": _source,
        })
        return self._responses.get(index, self._default)


# ---------------------------------------------------------------------------
# Unit tests – helper functions
# ---------------------------------------------------------------------------

class TestFetchRecentLikedPostUris:
    @pytest.mark.asyncio
    async def test_returns_subject_uris(self):
        es = FakeEs(responses={
            "likes": {
                "hits": {
                    "hits": [
                        {"_source": {"subject_uri": "at://post/1"}},
                        {"_source": {"subject_uri": "at://post/2"}},
                    ]
                }
            }
        })
        uris = await fetch_recent_liked_post_uris(es, "did:plc:user1", limit=10)
        assert uris == ["at://post/1", "at://post/2"]

        # Verify query structure
        call = es.calls[0]
        assert call["index"] == "likes"
        assert call["sort"] == [{"created_at": "desc"}]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_likes(self):
        es = FakeEs()
        uris = await fetch_recent_liked_post_uris(es, "did:plc:nobody")
        assert uris == []

    @pytest.mark.asyncio
    async def test_skips_hits_without_subject_uri(self):
        es = FakeEs(responses={
            "likes": {
                "hits": {
                    "hits": [
                        {"_source": {"subject_uri": "at://post/1"}},
                        {"_source": {}},
                        {"_source": {"subject_uri": "at://post/3"}},
                    ]
                }
            }
        })
        uris = await fetch_recent_liked_post_uris(es, "did:plc:user1")
        assert uris == ["at://post/1", "at://post/3"]


class TestFetchPostEmbeddings:
    @pytest.mark.asyncio
    async def test_returns_embeddings(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]}}},
                        {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.3, 0.4]}}},
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings(es, ["at://1", "at://2"])
        assert vecs == [[0.1, 0.2], [0.3, 0.4]]

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_input(self):
        es = FakeEs()
        vecs = await fetch_post_embeddings(es, [])
        assert vecs == []
        assert len(es.calls) == 0

    @pytest.mark.asyncio
    async def test_skips_posts_without_embeddings(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]}}},
                        {"_source": {"embeddings": {}}},
                        {"_source": {}},
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings(es, ["at://1", "at://2", "at://3"])
        assert vecs == [[0.1, 0.2]]


class TestAverageVectors:
    def test_single_vector(self):
        assert average_vectors([[1.0, 2.0, 3.0]]) == [1.0, 2.0, 3.0]

    def test_multiple_vectors(self):
        result = average_vectors([[1.0, 0.0], [3.0, 4.0]])
        assert result == [2.0, 2.0]

    def test_raises_on_empty(self):
        with pytest.raises(ValueError, match="No vectors"):
            average_vectors([])


class TestKnnSearchPosts:
    @pytest.mark.asyncio
    async def test_returns_candidates_with_scores(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.95,
                            "_source": {
                                "at_uri": "at://post/1",
                                "content": "hello",
                                "embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]},
                            },
                        },
                    ]
                }
            }
        })
        candidates = await knn_search_posts(es, [0.1, 0.2], num_candidates=10)
        assert len(candidates) == 1
        assert candidates[0].at_uri == "at://post/1"
        assert candidates[0].content == "hello"
        assert candidates[0].score == 0.95
        assert candidates[0].minilm_l12_embedding is not None
        assert candidates[0].generator_name is None

    @pytest.mark.asyncio
    async def test_passes_generator_name(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.8,
                            "_source": {
                                "at_uri": "at://post/1",
                                "content": "hi",
                                "embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]},
                            },
                        },
                    ]
                }
            }
        })
        candidates = await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, generator_name="post_similarity"
        )
        assert candidates[0].generator_name == "post_similarity"

    @pytest.mark.asyncio
    async def test_video_only_true_includes_filter(self):
        es = FakeEs(responses={
            "posts": {"hits": {"hits": []}}
        })
        await knn_search_posts(es, [0.1, 0.2], num_candidates=5, video_only=True)
        query = es.calls[0]["query"]
        assert {"term": {"contains_video": True}} in query["bool"]["filter"]

    @pytest.mark.asyncio
    async def test_video_only_false_omits_filter(self):
        es = FakeEs(responses={
            "posts": {"hits": {"hits": []}}
        })
        await knn_search_posts(es, [0.1, 0.2], num_candidates=5, video_only=False)
        query = es.calls[0]["query"]
        assert query["bool"]["filter"] == []


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
            async def search(self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs):
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
                                    {"_source": {"embeddings": {"all_MiniLM_L12_v2": [1.0, 0.0]}}},
                                    {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.0, 1.0]}}},
                                ]
                            }
                        }
                    # kNN search
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_score": 0.9,
                                    "_source": {
                                        "at_uri": "at://result/1",
                                        "content": "recommended post",
                                        "embeddings": {"all_MiniLM_L12_v2": [0.5, 0.5]},
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
            async def search(self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs):
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

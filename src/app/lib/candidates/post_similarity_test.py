"""Tests for the post_similarity candidate generator."""

import pytest

from ..candidates.post_similarity import (
    PostSimilarityCandidateGenerator,
    average_vectors,
    fetch_post_embeddings,
    fetch_recent_liked_post_uris,
    knn_search_posts,
)
from ..embeddings import MINILM_L12_EMBEDDING_KEY


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

    async def search(self, *, index=None, query=None, knn=None, size=None, sort=None, _source=None, **kwargs):
        self.calls.append({
            "index": index,
            "query": query,
            "knn": knn,
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
    async def test_returns_embeddings_in_requested_uri_order(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings(es, ["at://1", "at://2"])
        assert vecs == [
            ("at://1", [0.1, 0.2]),
            ("at://2", [0.3, 0.4]),
        ]

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
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "embeddings": {},
                            }
                        },
                        {"_source": {"at_uri": "at://3"}},
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings(es, ["at://1", "at://2", "at://3"])
        assert vecs == [("at://1", [0.1, 0.2])]


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
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.95,
                            "_source": {
                                "at_uri": "at://post/1",
                                "content": "hello",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
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
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.8,
                            "_source": {
                                "at_uri": "at://post/1",
                                "content": "hi",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
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
    async def test_no_reply_filter_in_es_query(self):
        """Reply exclusion is intentionally NOT sent to ES — it would force
        a brute-force fallback (~10x slower). We filter replies in Python."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(es, [0.1, 0.2], num_candidates=5)
        knn = es.calls[0]["knn"]
        assert es.calls[0]["query"] is None
        # No filter at all when there are no exclude_uris.
        assert "filter" not in knn

    @pytest.mark.asyncio
    async def test_exclude_uris_is_an_es_filter(self):
        """exclude_uris is bitmap-friendly and small, so it stays in ES."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, exclude_uris=["at://a", "at://b"]
        )
        knn = es.calls[0]["knn"]
        assert {"terms": {"at_uri": ["at://a", "at://b"]}} in knn["filter"]["bool"]["must_not"]

    @pytest.mark.asyncio
    async def test_replies_are_filtered_out_in_python(self):
        """Hits with thread_parent_post set are dropped from the returned set."""
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.95,
                            "_source": {
                                "at_uri": "at://post/1",
                                "content": "original",
                            },
                        },
                        {
                            "_score": 0.94,
                            "_source": {
                                "at_uri": "at://post/2",
                                "content": "reply",
                                "thread_parent_post": "at://parent/x",
                            },
                        },
                        {
                            "_score": 0.93,
                            "_source": {
                                "at_uri": "at://post/3",
                                "content": "another original",
                            },
                        },
                    ]
                }
            }
        })
        candidates = await knn_search_posts(es, [0.1, 0.2], num_candidates=10)
        assert [c.at_uri for c in candidates] == ["at://post/1", "at://post/3"]

    @pytest.mark.asyncio
    async def test_video_only_filters_in_python(self):
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {
                    "hits": [
                        {"_score": 0.9, "_source": {"at_uri": "at://a", "contains_video": True}},
                        {"_score": 0.8, "_source": {"at_uri": "at://b"}},
                        {"_score": 0.7, "_source": {"at_uri": "at://c", "contains_video": True}},
                    ]
                }
            }
        })
        candidates = await knn_search_posts(es, [0.1, 0.2], num_candidates=10, video_only=True)
        assert [c.at_uri for c in candidates] == ["at://a", "at://c"]

    @pytest.mark.asyncio
    async def test_overfetches_to_compensate_for_python_filter(self):
        """We ask ES for several times num_candidates so Python-side
        reply/video filtering still leaves enough results."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(es, [0.1, 0.2], num_candidates=10)
        call = es.calls[0]
        # OVERFETCH_MULTIPLIER * num_candidates with a floor of MIN_OVERFETCH.
        assert call["size"] >= 50
        assert call["knn"]["k"] == call["size"]

    @pytest.mark.asyncio
    async def test_caps_returned_candidates_at_num_candidates(self):
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {
                    "hits": [
                        {"_score": 1.0 - i / 100, "_source": {"at_uri": f"at://p/{i}"}}
                        for i in range(20)
                    ]
                }
            }
        })
        candidates = await knn_search_posts(es, [0.1, 0.2], num_candidates=5)
        assert len(candidates) == 5
        assert [c.at_uri for c in candidates] == [f"at://p/{i}" for i in range(5)]


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
                                    {
                                        "_source": {
                                            "at_uri": "at://post/2",
                                            "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.0, 1.0]},
                                        }
                                    },
                                    {
                                        "_source": {
                                            "at_uri": "at://post/1",
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
                                        "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.5, 0.5]},
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

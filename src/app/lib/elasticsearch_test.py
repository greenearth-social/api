"""Tests for shared Elasticsearch helpers."""

import pytest

from .elasticsearch import (
    fetch_post_embeddings_and_metadata,
    fetch_recent_liked_post_uris,
    fetch_recent_liked_post_uris_and_times,
)
from .embeddings import MINILM_L12_EMBEDDING_FIELD, MINILM_L12_EMBEDDING_KEY


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


class TestFetchRecentLikedPostUrisAndTimes:
    @pytest.mark.asyncio
    async def test_returns_subject_uris_and_times(self):
        es = FakeEs(responses={
            "likes": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "subject_uri": "at://post/1",
                                "created_at": "2026-01-01T00:00:00+00:00",
                            }
                        },
                        {
                            "_source": {
                                "subject_uri": "at://post/2",
                                "created_at": "2026-01-02T00:00:00+00:00",
                            }
                        },
                    ]
                }
            }
        })
        uris, times = await fetch_recent_liked_post_uris_and_times(es, "did:plc:user1", limit=10)

        assert uris == ["at://post/1", "at://post/2"]
        assert times == ["2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00"]

        call = es.calls[0]
        assert call["index"] == "likes"
        assert call["query"] == {
            "bool": {
                "filter": [
                    {"terms": {"author_did": ["did:plc:user1"]}},
                    {"exists": {"field": "created_at"}},
                ],
            }
        }
        assert call["sort"] == [{"created_at": "desc"}]
        assert call["_source"] == ["subject_uri", "created_at"]

    @pytest.mark.asyncio
    async def test_skips_hits_missing_subject_uri_or_time_to_keep_lists_aligned(self):
        es = FakeEs(responses={
            "likes": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "subject_uri": "at://post/1",
                                "created_at": "2026-01-01T00:00:00+00:00",
                            }
                        },
                        {"_source": {"created_at": "2026-01-02T00:00:00+00:00"}},
                        {"_source": {"subject_uri": "at://post/3"}},
                        {
                            "_source": {
                                "subject_uri": "at://post/4",
                                "created_at": "2026-01-04T00:00:00+00:00",
                            }
                        },
                    ]
                }
            }
        })

        uris, times = await fetch_recent_liked_post_uris_and_times(
            es, ["did:plc:user1", "did:plc:user2"]
        )

        assert uris == ["at://post/1", "at://post/4"]
        assert times == ["2026-01-01T00:00:00+00:00", "2026-01-04T00:00:00+00:00"]
        assert len(uris) == len(times)

    @pytest.mark.asyncio
    async def test_returns_empty_without_search_for_empty_users(self):
        es = FakeEs()

        uris, times = await fetch_recent_liked_post_uris_and_times(es, [])

        assert uris == []
        assert times == []
        assert es.calls == []


class TestFetchPostEmbeddingsAndMetadata:
    @pytest.mark.asyncio
    async def test_returns_embeddings_in_requested_uri_order(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "content": "two",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "content": "one",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2"])
        assert vecs == [
            ("at://1", [0.1, 0.2], "", 0),
            ("at://2", [0.3, 0.4], "", 0),
        ]
        assert es.calls[0]["_source"] == [
            "at_uri",
            MINILM_L12_EMBEDDING_FIELD,
            "author_did",
            "like_count",
            "content",
        ]

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_input_with_metadata_shape(self):
        es = FakeEs()
        vecs = await fetch_post_embeddings_and_metadata(es, [])
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
                                "content": "one",
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
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2", "at://3"])
        assert vecs == [("at://1", [0.1, 0.2], "", 0)]

    @pytest.mark.asyncio
    async def test_skips_embeddings_without_source_text(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "content": "one",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "content": "   ",
                                "media": [{"alt_text": ""}],
                                "video_transcript": None,
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2", "at://3"])
        assert vecs == [
            ("at://1", [0.1, 0.2], "", 0),
        ]


    @pytest.mark.asyncio
    async def test_returns_embeddings_authors_and_like_counts_in_requested_uri_order(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "author_did": "did:plc:two",
                                "like_count": 22,
                                "content": "two",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "author_did": "did:plc:one",
                                "like_count": 11,
                                "content": "one",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2"])
        assert vecs == [
            ("at://1", [0.1, 0.2], "did:plc:one", 11),
            ("at://2", [0.3, 0.4], "did:plc:two", 22),
        ]
        assert es.calls[0]["_source"] == [
            "at_uri",
            MINILM_L12_EMBEDDING_FIELD,
            "author_did",
            "like_count",
            "content",
        ]

    @pytest.mark.asyncio
    async def test_keeps_posts_with_missing_author_dids_and_like_counts(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "content": "one",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "author_did": 123,
                                "like_count": "2",
                                "content": "two",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://3",
                                "author_did": "did:plc:three",
                                "embeddings": {},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2", "at://3"])
        assert vecs == [
            ("at://1", [0.1, 0.2], "", 0),
            ("at://2", [0.3, 0.4], "", 0),
        ]

    @pytest.mark.asyncio
    async def test_skips_embeddings_without_source_text_even_with_author_did(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "at_uri": "at://1",
                                "author_did": "did:plc:one",
                                "content": "some content",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            }
                        },
                        {
                            "_source": {
                                "at_uri": "at://2",
                                "author_did": "did:plc:two",
                                "content": "",
                                "media": [{"alt_text": "   "}],
                                "video_transcript": "",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.3, 0.4]},
                            }
                        },
                    ]
                }
            }
        })
        vecs = await fetch_post_embeddings_and_metadata(es, ["at://1", "at://2"])
        assert vecs == [("at://1", [0.1, 0.2], "did:plc:one", 0)]

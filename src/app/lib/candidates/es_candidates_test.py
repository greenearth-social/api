"""Tests for shared Elasticsearch candidate helpers."""

import pytest

from ..embeddings import MINILM_L12_EMBEDDING_FIELD, MINILM_L12_EMBEDDING_KEY
from .es_candidates import knn_search_posts


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
        candidates = await knn_search_posts(
            es, [0.1, 0.2], num_candidates=10, search_field=MINILM_L12_EMBEDDING_FIELD
        )
        assert len(candidates) == 1
        assert candidates[0].at_uri == "at://post/1"
        assert candidates[0].content == "hello"
        assert candidates[0].score == 0.95
        assert candidates[0].minilm_l12_embedding is not None
        assert candidates[0].generator_name is None

    @pytest.mark.asyncio
    async def test_keeps_candidates_without_embeddings_for_later_hydration(self):
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
                        {
                            "_score": 0.8,
                            "_source": {
                                "at_uri": "at://post/2",
                                "content": "missing embedding",
                            },
                        },
                    ]
                }
            }
        })
        candidates = await knn_search_posts(
            es, [0.1, 0.2], num_candidates=10, search_field=MINILM_L12_EMBEDDING_FIELD
        )
        assert len(candidates) == 2
        assert candidates[0].at_uri == "at://post/1"
        assert candidates[1].at_uri == "at://post/2"
        assert candidates[1].minilm_l12_embedding is None

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
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            generator_name="post_similarity"
        )
        assert candidates[0].generator_name == "post_similarity"

    @pytest.mark.asyncio
    async def test_no_filters_when_no_args(self):
        """No filter clause sent to ES when there is nothing to filter on."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD
        )
        knn = es.calls[0]["knn"]
        assert es.calls[0]["query"] is None
        assert "filter" not in knn

    @pytest.mark.asyncio
    async def test_video_only_true_sends_es_filter(self):
        """video_only is applied on the ES side inside knn.filter."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            video_only=True
        )
        knn = es.calls[0]["knn"]
        assert {"term": {"contains_video": True}} in knn["filter"]["bool"]["filter"]

    @pytest.mark.asyncio
    async def test_video_only_false_omits_filter(self):
        """When video_only is False and no exclude_uris, no filter is sent."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            video_only=False
        )
        knn = es.calls[0]["knn"]
        assert "filter" not in knn

    @pytest.mark.asyncio
    async def test_exclude_uris_is_an_es_filter(self):
        """exclude_uris is bitmap-friendly and stays in ES knn.filter."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            exclude_uris=["at://a", "at://b"]
        )
        knn = es.calls[0]["knn"]
        assert {"terms": {"at_uri": ["at://a", "at://b"]}} in knn["filter"]["bool"]["must_not"]

    @pytest.mark.asyncio
    async def test_ge_post_embedding_model_uuid_is_an_es_filter(self):
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            ge_post_embedding_model_uuid="model-uuid-123"
        )
        knn = es.calls[0]["knn"]
        assert (
            {"term": {"ge_post_embedding_model_uuid": "model-uuid-123"}}
            in knn["filter"]["bool"]["filter"]
        )

    @pytest.mark.asyncio
    async def test_min_like_count_is_an_es_filter(self):
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            min_like_count=20
        )
        knn = es.calls[0]["knn"]
        assert {"range": {"like_count": {"gte": 20}}} in knn["filter"]["bool"]["filter"]

    @pytest.mark.asyncio
    async def test_ge_post_embedding_model_uuid_filter_combines_with_exclude_uris(self):
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=5, search_field=MINILM_L12_EMBEDDING_FIELD,
            exclude_uris=["at://a", "at://b"],
            ge_post_embedding_model_uuid="model-uuid-123",
        )
        knn = es.calls[0]["knn"]
        assert (
            {"term": {"ge_post_embedding_model_uuid": "model-uuid-123"}}
            in knn["filter"]["bool"]["filter"]
        )
        assert {"terms": {"at_uri": ["at://a", "at://b"]}} in knn["filter"]["bool"]["must_not"]

    @pytest.mark.asyncio
    async def test_uses_num_candidates_directly_for_k(self):
        """No overfetch: k == num_candidates since replies are gone from the index."""
        es = FakeEs(responses={"posts_recent": {"hits": {"hits": []}}})
        await knn_search_posts(
            es, [0.1, 0.2], num_candidates=10,
            search_field=MINILM_L12_EMBEDDING_FIELD
        )
        call = es.calls[0]
        assert call["size"] == 10
        assert call["knn"]["k"] == 10

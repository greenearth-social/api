"""Tests for the popularity candidate generator."""

import pytest

from ..candidates.popularity import (
    PopularityCandidateGenerator,
    popularity_search,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def generator():
    return PopularityCandidateGenerator()


class FakeEs:
    """Configurable fake Elasticsearch client for unit tests."""

    def __init__(self, responses: dict | None = None):
        self._responses = responses or {}
        self._default = {"hits": {"hits": []}}
        self.calls: list[dict] = []

    async def search(self, *, index=None, query=None, size=None, **kwargs):
        self.calls.append({"index": index, "query": query, "size": size})
        return self._responses.get(index, self._default)


# ---------------------------------------------------------------------------
# Unit tests – popularity_search
# ---------------------------------------------------------------------------

class TestPopularitySearch:
    @pytest.mark.asyncio
    async def test_returns_candidates_scored(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 12.5,
                            "_source": {
                                "at_uri": "at://popular/1",
                                "content": "trending post",
                                "embeddings": {"all_MiniLM_L12_v2": [0.5, 0.6]},
                            },
                        },
                        {
                            "_score": 10.0,
                            "_source": {
                                "at_uri": "at://popular/2",
                                "content": "another popular one",
                                "embeddings": {},
                            },
                        },
                    ]
                }
            }
        })

        candidates = await popularity_search(es, num_candidates=5, generator_name="popularity")

        assert len(candidates) == 2
        assert candidates[0].at_uri == "at://popular/1"
        assert candidates[0].score == 12.5
        assert candidates[0].generator_name == "popularity"
        assert candidates[0].minilm_l12_embedding is not None

        assert candidates[1].at_uri == "at://popular/2"
        assert candidates[1].score == 10.0
        assert candidates[1].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_sends_function_score_query(self):
        es = FakeEs()
        await popularity_search(es, num_candidates=20)

        assert len(es.calls) == 1
        call = es.calls[0]
        assert call["index"] == "posts"
        assert call["size"] == 20

        query = call["query"]
        assert "function_score" in query
        funcs = query["function_score"]["functions"]
        func_types = [list(f.keys())[0] for f in funcs]
        assert "gauss" in func_types
        assert "field_value_factor" in func_types

    @pytest.mark.asyncio
    async def test_video_only_true_includes_filter(self):
        es = FakeEs()
        await popularity_search(es, num_candidates=10, video_only=True)
        filters = es.calls[0]["query"]["function_score"]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} in filters

    @pytest.mark.asyncio
    async def test_video_only_false_omits_video_filter(self):
        es = FakeEs()
        await popularity_search(es, num_candidates=10, video_only=False)
        filters = es.calls[0]["query"]["function_score"]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} not in filters
        # Should still have the recency range filter
        assert any("range" in f for f in filters)

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_results(self):
        es = FakeEs()
        candidates = await popularity_search(es, num_candidates=10)
        assert candidates == []

    @pytest.mark.asyncio
    async def test_handles_missing_embeddings(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 5.0,
                            "_source": {
                                "at_uri": "at://popular/3",
                                "content": "no embeddings post",
                            },
                        },
                    ]
                }
            }
        })
        candidates = await popularity_search(es, num_candidates=5)
        assert len(candidates) == 1
        assert candidates[0].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_generator_name_defaults_to_none(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 1.0,
                            "_source": {
                                "at_uri": "at://popular/4",
                                "content": "post",
                                "embeddings": {},
                            },
                        },
                    ]
                }
            }
        })
        candidates = await popularity_search(es, num_candidates=1)
        assert candidates[0].generator_name is None


# ---------------------------------------------------------------------------
# Integration-style tests – full generator
# ---------------------------------------------------------------------------

class TestPopularityCandidateGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "popularity"

    @pytest.mark.asyncio
    async def test_generate(self, generator):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 8.0,
                            "_source": {
                                "at_uri": "at://popular/1",
                                "content": "popular post",
                                "embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]},
                            },
                        },
                    ]
                }
            }
        })

        result = await generator.generate(es, "did:plc:user1", num_candidates=10)

        assert result.generator_name == "popularity"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://popular/1"
        assert result.candidates[0].score == 8.0
        assert result.candidates[0].generator_name == "popularity"

    @pytest.mark.asyncio
    async def test_generate_empty(self, generator):
        es = FakeEs()
        result = await generator.generate(es, "did:plc:nobody", num_candidates=10)
        assert result.generator_name == "popularity"
        assert result.candidates == []

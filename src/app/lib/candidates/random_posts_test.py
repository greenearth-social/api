"""Tests for the random_posts candidate generator."""

import pytest

from ..candidates.random_posts import (
    RandomPostsCandidateGenerator,
    random_posts_search,
)


@pytest.fixture
def generator():
    return RandomPostsCandidateGenerator()


class FakeEs:
    """Configurable fake Elasticsearch client for unit tests."""

    def __init__(self, responses: dict | None = None):
        self._responses = responses or {}
        self._default = {"hits": {"hits": []}}
        self.calls: list[dict] = []

    async def search(self, *, index=None, query=None, size=None, **kwargs):
        self.calls.append({"index": index, "query": query, "size": size})
        return self._responses.get(index, self._default)


class TestRandomPostsSearch:
    @pytest.mark.asyncio
    async def test_returns_candidates_scored(self):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.91,
                            "_source": {
                                "at_uri": "at://random/1",
                                "content": "random post",
                                "embeddings": {"all_MiniLM_L12_v2": [0.5, 0.6]},
                            },
                        },
                        {
                            "_score": 0.72,
                            "_source": {
                                "at_uri": "at://random/2",
                                "content": "another random post",
                                "embeddings": {},
                            },
                        },
                    ]
                }
            }
        })

        candidates = await random_posts_search(es, num_candidates=5, generator_name="random_posts")

        assert len(candidates) == 2
        assert candidates[0].at_uri == "at://random/1"
        assert candidates[0].score == 0.91
        assert candidates[0].generator_name == "random_posts"
        assert candidates[0].minilm_l12_embedding is not None

        assert candidates[1].at_uri == "at://random/2"
        assert candidates[1].score == 0.72
        assert candidates[1].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_sends_random_score_query(self):
        es = FakeEs()

        await random_posts_search(es, num_candidates=20)

        assert len(es.calls) == 1
        call = es.calls[0]
        assert call["index"] == "posts"
        assert call["size"] == 20

        query = call["query"]
        function_score = query["function_score"]
        assert "random_score" in function_score
        assert function_score["boost_mode"] == "replace"

    @pytest.mark.asyncio
    async def test_video_only_true_includes_filter(self):
        es = FakeEs()
        await random_posts_search(es, num_candidates=10, video_only=True)
        filters = es.calls[0]["query"]["function_score"]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} in filters

    @pytest.mark.asyncio
    async def test_video_only_false_omits_filter(self):
        es = FakeEs()
        await random_posts_search(es, num_candidates=10, video_only=False)
        filters = es.calls[0]["query"]["function_score"]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} not in filters

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_results(self):
        es = FakeEs()
        candidates = await random_posts_search(es, num_candidates=10)
        assert candidates == []


class TestRandomPostsCandidateGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "random_posts"

    @pytest.mark.asyncio
    async def test_generate(self, generator):
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.8,
                            "_source": {
                                "at_uri": "at://random/1",
                                "content": "random post",
                                "embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]},
                            },
                        },
                    ]
                }
            }
        })

        result = await generator.generate(es, "did:plc:user1", num_candidates=10)

        assert result.generator_name == "random_posts"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://random/1"
        assert result.candidates[0].score == 0.8
        assert result.candidates[0].generator_name == "random_posts"

    @pytest.mark.asyncio
    async def test_generate_empty(self, generator):
        es = FakeEs()
        result = await generator.generate(es, "did:plc:nobody", num_candidates=10)
        assert result.generator_name == "random_posts"
        assert result.candidates == []

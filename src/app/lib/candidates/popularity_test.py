"""Tests for the popularity candidate generator."""

from datetime import datetime, timezone

import pytest

from ...models import CandidatePost
from ..candidates.popularity import (
    PopularityCandidateGenerator,
    popularity_search,
    recency_decay_scale,
    take_from_pool,
)
from ..candidates.popularity_cache import PopularityPool, set_popularity_cache
from ..embeddings import MINILM_L12_EMBEDDING_KEY


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
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 12.5,
                            "_source": {
                                "at_uri": "at://popular/1",
                                "content": "trending post",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.5, 0.6]},
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
        assert call["index"] == "posts_recent"
        assert call["size"] == 20

        query = call["query"]
        assert "function_score" in query
        funcs = query["function_score"]["functions"]
        func_types = [list(f.keys())[0] for f in funcs]
        assert "gauss" in func_types
        assert "script_score" in func_types

        script_func = next(f["script_score"] for f in funcs if "script_score" in f)
        script_source = script_func["script"]["source"]
        assert "Math.max(likes, 0.0)" in script_source
        assert "Math.log1p(likes)" in script_source
        assert query["function_score"]["score_mode"] == "multiply"
        assert query["function_score"]["boost_mode"] == "replace"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("hours", "scale"),
        [(6, "90m"), (24, "6h"), (168, "42h")],
    )
    async def test_selected_window_sets_cutoff_and_decay(self, hours, scale):
        es = FakeEs()
        await popularity_search(es, num_candidates=10, max_age_hours=hours)
        function_score = es.calls[0]["query"]["function_score"]
        filters = function_score["query"]["bool"]["filter"]
        assert {"range": {"created_at": {"gte": f"now-{hours}h"}}} in filters
        gauss = next(f["gauss"] for f in function_score["functions"] if "gauss" in f)
        assert gauss["created_at"]["scale"] == scale
        assert recency_decay_scale(hours) == scale

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
    async def test_exclude_uris_pushed_into_query(self):
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 10.0,
                            "_source": {"at_uri": "at://popular/1", "content": "x", "embeddings": {}},
                        },
                        {
                            "_score": 8.0,
                            "_source": {"at_uri": "at://popular/2", "content": "x", "embeddings": {}},
                        },
                    ]
                }
            }
        })

        candidates = await popularity_search(
            es,
            num_candidates=2,
            exclude_uris=["at://popular/excluded"],
        )

        inner_bool = es.calls[0]["query"]["function_score"]["query"]["bool"]
        assert inner_bool["must_not"] == [
            {"terms": {"at_uri": ["at://popular/excluded"]}}
        ]
        assert es.calls[0]["size"] == 2  # no overfetch; ES handles exclusions
        assert [c.at_uri for c in candidates] == ["at://popular/1", "at://popular/2"]

    @pytest.mark.asyncio
    async def test_no_exclude_uris_no_overfetch(self):
        es = FakeEs()
        await popularity_search(es, num_candidates=10)
        assert es.calls[0]["size"] == 10

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_results(self):
        es = FakeEs()
        candidates = await popularity_search(es, num_candidates=10)
        assert candidates == []

    @pytest.mark.asyncio
    async def test_handles_missing_embeddings(self):
        es = FakeEs(responses={
            "posts_recent": {
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
            "posts_recent": {
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
            "posts_recent": {
                "hits": {
                    "hits": [
                        {
                            "_score": 8.0,
                            "_source": {
                                "at_uri": "at://popular/1",
                                "content": "popular post",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
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


# ---------------------------------------------------------------------------
# take_from_pool
# ---------------------------------------------------------------------------

def _pool(n: int) -> list[CandidatePost]:
    return [CandidatePost(at_uri=f"at://pool/{i}", score=float(n - i)) for i in range(n)]


class TestTakeFromPool:
    def test_takes_the_top_n_in_pool_order(self):
        taken = take_from_pool(_pool(10), None, 3)
        assert [c.at_uri for c in taken] == ["at://pool/0", "at://pool/1", "at://pool/2"]

    def test_skips_excluded_uris(self):
        taken = take_from_pool(_pool(10), ["at://pool/0", "at://pool/2"], 3)
        assert [c.at_uri for c in taken] == ["at://pool/1", "at://pool/3", "at://pool/4"]

    def test_returns_what_it_can_when_the_pool_runs_out(self):
        taken = take_from_pool(_pool(3), ["at://pool/0", "at://pool/1"], 5)
        assert [c.at_uri for c in taken] == ["at://pool/2"]


# ---------------------------------------------------------------------------
# Generator + shared pool cache
# ---------------------------------------------------------------------------

class StubCache:
    """Stands in for the process-level PopularityCache."""

    def __init__(self, pool: list[CandidatePost] | None):
        self._pool = pool
        self.calls: list[dict] = []

    async def get_pool(self, *, video_only, max_age_hours, fetch):
        self.calls.append({"video_only": video_only, "max_age_hours": max_age_hours})
        self.fetch = fetch
        if self._pool is None:
            return None
        return PopularityPool(candidates=self._pool, generated_at=datetime.now(timezone.utc))


@pytest.fixture
def install_cache():
    installed: list[StubCache] = []

    def _install(pool: list[CandidatePost] | None) -> StubCache:
        cache = StubCache(pool)
        installed.append(cache)
        set_popularity_cache(cache)
        return cache

    yield _install
    set_popularity_cache(None)


class TestPopularityGeneratorWithCache:
    @pytest.mark.asyncio
    async def test_serves_from_the_pool_without_querying(self, generator, install_cache):
        cache = install_cache(_pool(50))
        es = FakeEs()

        result = await generator.generate(es, "did:plc:user1", num_candidates=5)

        assert [c.at_uri for c in result.candidates] == [f"at://pool/{i}" for i in range(5)]
        assert es.calls == []
        assert cache.calls == [{"video_only": False, "max_age_hours": 168}]

    @pytest.mark.asyncio
    async def test_applies_per_user_exclusions_to_the_shared_pool(self, generator, install_cache):
        install_cache(_pool(50))

        result = await generator.generate(
            FakeEs(), "did:plc:user1", num_candidates=3, exclude_uris=["at://pool/1"]
        )

        assert [c.at_uri for c in result.candidates] == [
            "at://pool/0",
            "at://pool/2",
            "at://pool/3",
        ]

    @pytest.mark.asyncio
    async def test_falls_back_to_a_query_when_nothing_is_cached(self, generator, install_cache):
        install_cache(None)
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {"hits": [{"_score": 3.0, "_source": {"at_uri": "at://live/1"}}]}
            }
        })

        result = await generator.generate(es, "did:plc:user1", num_candidates=5)

        assert [c.at_uri for c in result.candidates] == ["at://live/1"]
        assert len(es.calls) == 1

    @pytest.mark.asyncio
    async def test_falls_back_when_exclusions_exhaust_the_pool(
        self, generator, install_cache, monkeypatch
    ):
        """A user who has seen everything popular still gets a full slate."""
        monkeypatch.setenv("GE_POPULARITY_CACHE_POOL_SIZE", "4")  # a full pool
        install_cache(_pool(4))
        es = FakeEs(responses={
            "posts_recent": {
                "hits": {"hits": [{"_score": 3.0, "_source": {"at_uri": "at://live/1"}}]}
            }
        })

        result = await generator.generate(
            es,
            "did:plc:user1",
            num_candidates=4,
            exclude_uris=["at://pool/0", "at://pool/1"],
        )

        assert [c.at_uri for c in result.candidates] == ["at://live/1"]
        assert len(es.calls) == 1

    @pytest.mark.asyncio
    async def test_short_pool_is_served_without_a_fallback_query(
        self, generator, install_cache, monkeypatch
    ):
        """A pool short of its target holds every eligible post; don't re-query."""
        monkeypatch.setenv("GE_POPULARITY_CACHE_POOL_SIZE", "500")
        install_cache(_pool(2))
        es = FakeEs()

        result = await generator.generate(es, "did:plc:user1", num_candidates=10)

        assert [c.at_uri for c in result.candidates] == ["at://pool/0", "at://pool/1"]
        assert es.calls == []

    @pytest.mark.asyncio
    async def test_pool_refresh_query_carries_no_user_exclusions(self, generator, install_cache):
        """The pool is shared, so one user's seen posts must not shape it."""
        cache = install_cache(_pool(50))
        es = FakeEs()

        await generator.generate(
            es, "did:plc:user1", num_candidates=5, exclude_uris=["at://pool/0"],
            max_age_hours=24, video_only=True,
        )
        await cache.fetch(500)

        assert len(es.calls) == 1
        assert es.calls[0]["size"] == 500
        bool_query = es.calls[0]["query"]["function_score"]["query"]["bool"]
        assert "must_not" not in bool_query
        assert {"term": {"contains_video": True}} in bool_query["filter"]

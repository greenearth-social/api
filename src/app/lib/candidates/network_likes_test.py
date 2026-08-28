"""Tests for the network_likes candidate generator."""

import pytest

from .. import bsky as bsky_module
from ..candidates import network_likes as network_likes_module
from ..candidates.network_likes import (
    NetworkLikesCandidateGenerator,
    fetch_posts_by_uris,
    fetch_recent_liked_post_uris,
    hydrated_uri_limit,
    liked_post_scan_size,
    network_likes_search,
)
from ..embeddings import MINILM_L12_EMBEDDING_KEY


@pytest.fixture
def generator():
    return NetworkLikesCandidateGenerator()


def like_hit(uri: str | None, sort_value: int):
    source = {}
    if uri is not None:
        source["subject_uri"] = uri
    return {"_source": source, "sort": [sort_value]}


def post_hit(uri: str, content: str | None = None):
    return {
        "_score": 99.0,
        "_source": {
            "at_uri": uri,
            "content": content or uri,
            "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
        },
    }


def likes_response(hits: list[dict]):
    return {"hits": {"hits": hits}}


def post_terms_from_query(query: dict) -> list[str]:
    for filter_clause in query["bool"]["filter"]:
        terms = filter_clause.get("terms", {})
        if "at_uri" in terms:
            return terms["at_uri"]
    return []


class FakeEs:
    """Configurable fake Elasticsearch client for unit tests."""

    def __init__(
        self,
        *,
        likes: dict | None = None,
        posts_by_uri: dict[str, dict] | None = None,
        posts_return_order: list[str] | None = None,
    ):
        self.likes = likes
        self.posts_by_uri = posts_by_uri or {}
        self.posts_return_order = posts_return_order
        self.calls: list[dict] = []

    async def search(
        self,
        *,
        index=None,
        query=None,
        size=None,
        sort=None,
        _source=None,
        search_after=None,
        **kwargs,
    ):
        self.calls.append(
            {
                "index": index,
                "query": query,
                "size": size,
                "sort": sort,
                "_source": _source,
                "search_after": search_after,
                "kwargs": kwargs,
            }
        )

        if index == "likes":
            if self.likes is None:
                return likes_response([])
            query_body = query if isinstance(query, dict) else {}
            excluded_uris = {
                uri
                for clause in query_body.get("bool", {}).get("must_not", [])
                for uri in clause.get("terms", {}).get("subject_uri", [])
            }
            hits = self.likes.get("hits", {}).get("hits", [])
            if excluded_uris:
                hits = [
                    hit
                    for hit in hits
                    if (hit.get("_source") or {}).get("subject_uri") not in excluded_uris
                ]
            return likes_response(hits[: size or len(hits)])

        if index == "posts_recent":
            assert query is not None
            requested_uris = post_terms_from_query(query)
            return_order = self.posts_return_order or requested_uris
            hits = [
                self.posts_by_uri[uri]
                for uri in return_order
                if uri in requested_uris and uri in self.posts_by_uri
            ]
            return {"hits": {"hits": hits}}

        return {"hits": {"hits": []}}


def stub_followed_dids(monkeypatch, dids: list[str]):
    async def fake_get_followed_dids(user_did: str):
        assert user_did == "did:plc:user1"
        return dids

    monkeypatch.setattr(
        network_likes_module,
        "get_followed_dids_cached",
        fake_get_followed_dids,
    )


class TestLikedPostScanSize:
    def test_scales_with_num_candidates_between_floor_and_cap(self):
        assert liked_post_scan_size(1) == network_likes_module.MIN_LIKES_SCANNED
        assert liked_post_scan_size(1_000_000) == network_likes_module.MAX_LIKES_SCANNED

        num_candidates = network_likes_module.MIN_LIKES_SCANNED
        assert liked_post_scan_size(num_candidates) == min(
            num_candidates * network_likes_module.LIKES_OVERFETCH_FACTOR,
            network_likes_module.MAX_LIKES_SCANNED,
        )


class TestSizingAtTheRealAllocation:
    """Prod feeds split num_candidates=30 across generators by weight, so
    network_likes is asked for 3-6 — never the 30 of its debug feed. Sizing the
    floors for 30 made every real request scan and hydrate ~10x what it needed
    (see #416); these pin the numbers the real path actually gets."""

    def test_scan_and_hydrate_are_sized_for_three_to_six_candidates(self):
        assert [liked_post_scan_size(c) for c in (3, 6)] == [200, 200]
        assert [hydrated_uri_limit(c) for c in (3, 6)] == [50, 60]

    def test_hydrate_covers_the_measured_yield_with_headroom(self):
        # Measured in prod: candidates.network_likes.hydrate_hit_share ~= 0.40.
        measured_hit_share = 0.40
        for num_candidates in (3, 6):
            expected = hydrated_uri_limit(num_candidates) * measured_hit_share
            assert expected >= num_candidates * 3


class TestHydratedUriLimit:
    def test_scales_with_num_candidates_between_floor_and_cap(self):
        assert hydrated_uri_limit(1) == network_likes_module.MIN_HYDRATED_URIS
        assert hydrated_uri_limit(1_000_000) == network_likes_module.MAX_HYDRATED_URIS
        assert hydrated_uri_limit(50) == max(
            network_likes_module.MIN_HYDRATED_URIS,
            50 * network_likes_module.HYDRATE_OVERFETCH_FACTOR,
        )

    def test_never_exceeds_the_likes_scan(self):
        for num_candidates in (1, 30, 100, 10_000):
            assert hydrated_uri_limit(num_candidates) <= liked_post_scan_size(num_candidates)


class TestFetchRecentLikedPostUris:
    @pytest.mark.asyncio
    async def test_returns_uris_in_like_recency_order(self):
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/1", 3),
                    like_hit(None, 2),
                    like_hit("at://post/2", 1),
                ]
            )
        )

        uris = await fetch_recent_liked_post_uris(
            es,
            ["did:plc:follow1"],
            size=50,
            max_age_hours=48,
            exclude_uris=["at://post/seen"],
        )

        assert uris == ["at://post/1", "at://post/2"]

        call = es.calls[0]
        assert call["index"] == "likes"
        assert call["query"] == {
            "bool": {
                "filter": [
                    {"terms": {"author_did": ["did:plc:follow1"]}},
                    {"range": {"created_at": {"gte": "now-48h"}}},
                ],
                "must_not": [
                    {"terms": {"subject_uri": ["at://post/seen"]}},
                ],
            }
        }
        assert call["size"] == 50
        assert call["sort"] == [{"created_at": "desc"}]
        assert call["_source"] == ["subject_uri"]
        assert call["search_after"] is None

    @pytest.mark.asyncio
    async def test_skips_es_when_input_is_empty(self):
        es = FakeEs()

        assert await fetch_recent_liked_post_uris(es, [], size=50) == []
        assert es.calls == []


class TestFetchPostsByUris:
    @pytest.mark.asyncio
    async def test_preserves_requested_uri_order(self):
        es = FakeEs(
            posts_by_uri={
                "at://post/a": post_hit("at://post/a"),
                "at://post/b": post_hit("at://post/b"),
            },
            posts_return_order=["at://post/b", "at://post/a"],
        )

        candidates = await fetch_posts_by_uris(
            es,
            ["at://post/a", "at://post/b"],
            generator_name="network_likes",
        )

        assert [candidate.at_uri for candidate in candidates] == [
            "at://post/a",
            "at://post/b",
        ]
        assert candidates[0].generator_name == "network_likes"
        assert candidates[0].score == 99.0
        assert candidates[0].minilm_l12_embedding is not None
        assert es.calls[0]["index"] == "posts_recent"

    @pytest.mark.asyncio
    async def test_applies_video_filter_in_es_and_exclude_filter_in_python(self):
        es = FakeEs(
            posts_by_uri={
                "at://post/a": post_hit("at://post/a"),
                "at://post/seen": post_hit("at://post/seen"),
            },
        )

        candidates = await fetch_posts_by_uris(
            es,
            ["at://post/a", "at://post/seen"],
            video_only=True,
            exclude_uris=["at://post/seen"],
        )

        query = es.calls[0]["query"]
        assert {"term": {"contains_video": True}} in query["bool"]["filter"]
        assert {"range": {"created_at": {"gte": "now-168h"}}} in query["bool"]["filter"]
        assert {"terms": {"at_uri": ["at://post/a", "at://post/seen"]}} in query["bool"]["filter"]
        assert "must_not" not in query["bool"]
        assert [c.at_uri for c in candidates] == ["at://post/a"]

    @pytest.mark.asyncio
    async def test_empty_uri_list_skips_es(self):
        es = FakeEs()

        candidates = await fetch_posts_by_uris(es, [])

        assert candidates == []
        assert es.calls == []


class TestNetworkLikesSearch:
    @pytest.mark.asyncio
    async def test_uses_one_likes_and_one_hydrate_call_and_scores_by_like_count(
        self,
        monkeypatch,
    ):
        stub_followed_dids(monkeypatch, ["did:plc:follow1", "did:plc:follow2"])
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 60),
                    like_hit("at://missing/1", 50),
                    like_hit("at://missing/2", 40),
                    like_hit("at://post/a", 30),
                    like_hit("at://missing/3", 20),
                    like_hit("at://post/b", 10),
                ]
            ),
            posts_by_uri={
                "at://post/a": post_hit("at://post/a"),
                "at://post/b": post_hit("at://post/b"),
            },
        )

        candidates = await network_likes_search(
            es,
            "did:plc:user1",
            num_candidates=2,
            generator_name="network_likes",
        )

        assert [(candidate.at_uri, candidate.score) for candidate in candidates] == [
            ("at://post/a", 2.0),
            ("at://post/b", 1.0),
        ]
        assert [candidate.generator_name for candidate in candidates] == [
            "network_likes",
            "network_likes",
        ]

        likes_calls = [call for call in es.calls if call["index"] == "likes"]
        posts_calls = [call for call in es.calls if call["index"] == "posts_recent"]
        assert len(likes_calls) == 1
        assert len(posts_calls) == 1
        assert likes_calls[0]["size"] == liked_post_scan_size(2)
        assert post_terms_from_query(posts_calls[0]["query"]) == [
            "at://post/a",
            "at://missing/1",
            "at://missing/2",
            "at://missing/3",
            "at://post/b",
        ]

    @pytest.mark.asyncio
    async def test_hydrates_only_the_most_liked_uris_when_over_the_cap(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        monkeypatch.setattr(network_likes_module, "MAX_HYDRATED_URIS", 2)
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/cold", 5),
                    like_hit("at://post/hot", 4),
                    like_hit("at://post/hot", 3),
                    like_hit("at://post/warm", 2),
                    like_hit("at://post/warm", 1),
                ]
            ),
            posts_by_uri={
                "at://post/hot": post_hit("at://post/hot"),
                "at://post/warm": post_hit("at://post/warm"),
                "at://post/cold": post_hit("at://post/cold"),
            },
        )

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=3)

        posts_call = next(call for call in es.calls if call["index"] == "posts_recent")
        assert post_terms_from_query(posts_call["query"]) == [
            "at://post/hot",
            "at://post/warm",
        ]
        assert [candidate.at_uri for candidate in candidates] == [
            "at://post/hot",
            "at://post/warm",
        ]

    @pytest.mark.asyncio
    async def test_applies_requested_freshness_to_likes_and_posts(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes=likes_response([like_hit("at://post/a", 1)]),
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        candidates = await network_likes_search(
            es,
            "did:plc:user1",
            num_candidates=1,
            max_age_hours=48,
        )

        assert [candidate.at_uri for candidate in candidates] == ["at://post/a"]
        expected_range = {"range": {"created_at": {"gte": "now-48h"}}}
        likes_call = next(call for call in es.calls if call["index"] == "likes")
        posts_call = next(call for call in es.calls if call["index"] == "posts_recent")
        assert expected_range in likes_call["query"]["bool"]["filter"]
        assert expected_range in posts_call["query"]["bool"]["filter"]

    @pytest.mark.asyncio
    async def test_excludes_seen_uris_from_likes_query(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/seen", 2),
                    like_hit("at://post/a", 1),
                ]
            ),
            posts_by_uri={
                "at://post/seen": post_hit("at://post/seen"),
                "at://post/a": post_hit("at://post/a"),
            },
        )

        candidates = await network_likes_search(
            es,
            "did:plc:user1",
            num_candidates=1,
            exclude_uris=["at://post/seen"],
        )

        assert [candidate.at_uri for candidate in candidates] == ["at://post/a"]
        likes_call = next(call for call in es.calls if call["index"] == "likes")
        assert likes_call["query"]["bool"]["must_not"] == [
            {"terms": {"subject_uri": ["at://post/seen"]}},
        ]
        posts_call = next(call for call in es.calls if call["index"] == "posts_recent")
        assert post_terms_from_query(posts_call["query"]) == ["at://post/a"]

    @pytest.mark.asyncio
    async def test_respects_hard_likes_scan_cap(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        monkeypatch.setattr(network_likes_module, "MAX_LIKES_SCANNED", 4)
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://missing/1", 4),
                    like_hit("at://missing/2", 3),
                    like_hit("at://missing/3", 2),
                    like_hit("at://missing/4", 1),
                ]
            )
        )

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=2)

        assert candidates == []
        likes_calls = [call for call in es.calls if call["index"] == "likes"]
        assert len(likes_calls) == 1
        assert likes_calls[0]["size"] == 4

    @pytest.mark.asyncio
    async def test_equal_like_counts_tie_break_by_last_seen_recency(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 4),
                    like_hit("at://post/b", 3),
                    like_hit("at://post/b", 2),
                    like_hit("at://post/a", 1),
                ]
            ),
            posts_by_uri={
                "at://post/a": post_hit("at://post/a"),
                "at://post/b": post_hit("at://post/b"),
            },
        )

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=2)

        assert [(candidate.at_uri, candidate.score) for candidate in candidates] == [
            ("at://post/b", 2.0),
            ("at://post/a", 2.0),
        ]

    @pytest.mark.asyncio
    async def test_returns_empty_and_skips_es_when_no_followed_users(self, monkeypatch):
        stub_followed_dids(monkeypatch, [])
        es = FakeEs()

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert candidates == []
        assert es.calls == []

    @pytest.mark.asyncio
    async def test_lookup_error_returns_empty_and_skips_es(self, monkeypatch):
        async def fake_get_followed_dids(user_did: str):
            raise bsky_module.FollowedUsersLookupError("lookup exploded")

        monkeypatch.setattr(
            network_likes_module,
            "get_followed_dids_cached",
            fake_get_followed_dids,
        )
        es = FakeEs()

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert candidates == []
        assert es.calls == []


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))

    def value(self, name):
        return next(value for recorded, value, _ in self.records if recorded == name)

    def names(self):
        return [name for name, _, _ in self.records]


class TestNetworkLikesTelemetry:
    """Under-fill is the risk of scanning once instead of paging: these metrics
    say whether it happened and which limit caused it."""

    @pytest.fixture
    def collector(self, monkeypatch):
        collector = _RecordingCollector()
        monkeypatch.setattr(network_likes_module, "get_metric_collector", lambda: collector)
        return collector

    @pytest.mark.asyncio
    async def test_records_hydrate_yield_and_no_saturation_when_under_the_limits(
        self,
        collector,
        monkeypatch,
    ):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 3),
                    like_hit("at://missing/1", 2),
                    like_hit("at://missing/2", 1),
                ]
            ),
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert collector.value("candidates.network_likes.hydrate_hit_share") == 1 / 3
        assert "candidates.network_likes.likes_scan_saturated_count" not in collector.names()
        assert "candidates.network_likes.hydrate_truncated_count" not in collector.names()

    @pytest.mark.asyncio
    async def test_counts_a_saturated_likes_scan(self, collector, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        monkeypatch.setattr(network_likes_module, "MIN_LIKES_SCANNED", 2)
        monkeypatch.setattr(network_likes_module, "MAX_LIKES_SCANNED", 2)
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 2),
                    like_hit("at://post/b", 1),
                ]
            ),
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert collector.value("candidates.network_likes.likes_scan_saturated_count") == 1

    @pytest.mark.asyncio
    async def test_counts_a_truncated_hydrate(self, collector, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        monkeypatch.setattr(network_likes_module, "MAX_HYDRATED_URIS", 1)
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 2),
                    like_hit("at://post/b", 1),
                ]
            ),
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert collector.value("candidates.network_likes.hydrate_truncated_count") == 1

    @pytest.mark.asyncio
    async def test_records_nothing_when_there_are_no_likes_to_hydrate(
        self,
        collector,
        monkeypatch,
    ):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(likes=likes_response([]))

        await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert collector.names() == []


class TestNetworkLikesCandidateGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "network_likes"

    @pytest.mark.asyncio
    async def test_generate(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes=likes_response(
                [
                    like_hit("at://post/a", 2),
                    like_hit("at://post/a", 1),
                ]
            ),
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        result = await generator.generate(es, "did:plc:user1", num_candidates=1)

        assert result.generator_name == "network_likes"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://post/a"
        assert result.candidates[0].score == 2.0
        assert result.candidates[0].generator_name == "network_likes"

    @pytest.mark.asyncio
    async def test_generate_explains_no_recent_likes(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])

        result = await generator.generate(FakeEs(), "did:plc:user1", num_candidates=10)

        assert result.candidates == []
        assert result.reason == "no_recent_network_likes"

    @pytest.mark.asyncio
    async def test_generate_explains_liked_posts_missing_from_corpus(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(likes=likes_response([like_hit("at://missing", 1)]))

        result = await generator.generate(es, "did:plc:user1", num_candidates=10)

        assert result.candidates == []
        assert result.reason == "liked_posts_unavailable"

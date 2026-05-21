"""Tests for the network_likes candidate generator."""

import pytest

from .. import bsky as bsky_module
from ..candidates import network_likes as network_likes_module
from ..candidates.network_likes import (
    MAX_FOLLOWED_USERS,
    NetworkLikesCandidateGenerator,
    fetch_posts_by_uris,
    fetch_recent_liked_post_uri_page,
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
        likes_pages: list[dict] | None = None,
        posts_by_uri: dict[str, dict] | None = None,
        posts_return_order: list[str] | None = None,
    ):
        self.likes_pages = list(likes_pages or [])
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
        self.calls.append({
            "index": index,
            "query": query,
            "size": size,
            "sort": sort,
            "_source": _source,
            "search_after": search_after,
            "kwargs": kwargs,
        })

        if index == "likes":
            if self.likes_pages:
                return self.likes_pages.pop(0)
            return likes_response([])

        if index == "posts":
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
    async def fake_get_followed_user_dids(user_did: str, limit: int):
        assert user_did == "did:plc:user1"
        assert limit == MAX_FOLLOWED_USERS
        return dids

    monkeypatch.setattr(
        network_likes_module,
        "get_followed_user_dids",
        fake_get_followed_user_dids,
    )


class TestFetchRecentLikedPostUriPage:
    @pytest.mark.asyncio
    async def test_returns_uris_and_next_search_after(self):
        es = FakeEs(likes_pages=[
            likes_response([
                like_hit("at://post/1", 3),
                like_hit(None, 2),
                like_hit("at://post/2", 1),
            ])
        ])

        page = await fetch_recent_liked_post_uri_page(
            es,
            ["did:plc:follow1"],
            size=50,
            search_after=["cursor"],
        )

        assert page.uris == ["at://post/1", "at://post/2"]
        assert page.next_search_after == [1]
        assert page.hit_count == 3

        call = es.calls[0]
        assert call["index"] == "likes"
        assert call["query"] == {
            "bool": {
                "filter": [{"terms": {"author_did": ["did:plc:follow1"]}}],
            }
        }
        assert call["size"] == 50
        assert call["sort"] == [{"created_at": "desc"}]
        assert call["_source"] == ["subject_uri"]
        assert call["search_after"] == ["cursor"]

    @pytest.mark.asyncio
    async def test_skips_es_when_input_is_empty(self):
        es = FakeEs()

        page = await fetch_recent_liked_post_uri_page(es, [], size=50)

        assert page.uris == []
        assert page.next_search_after is None
        assert page.hit_count == 0
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

    @pytest.mark.asyncio
    async def test_applies_video_and_exclude_filters(self):
        es = FakeEs()

        await fetch_posts_by_uris(
            es,
            ["at://post/a"],
            video_only=True,
            exclude_uris=["at://post/seen"],
        )

        query = es.calls[0]["query"]
        assert {"term": {"contains_video": True}} in query["bool"]["filter"]
        assert {"terms": {"at_uri": ["at://post/a"]}} in query["bool"]["filter"]
        assert query["bool"]["must_not"] == [
            {"exists": {"field": "thread_parent_post"}},
            {"terms": {"at_uri": ["at://post/seen"]}},
        ]

    @pytest.mark.asyncio
    async def test_empty_uri_list_skips_es(self):
        es = FakeEs()

        candidates = await fetch_posts_by_uris(es, [])

        assert candidates == []
        assert es.calls == []


class TestNetworkLikesSearch:
    @pytest.mark.asyncio
    async def test_paginates_until_enough_post_hits_and_scores_by_like_count(
        self,
        monkeypatch,
    ):
        stub_followed_dids(monkeypatch, ["did:plc:follow1", "did:plc:follow2"])
        monkeypatch.setattr(network_likes_module, "LIKED_POSTS_PAGE_SIZE", 1)
        monkeypatch.setattr(network_likes_module, "MAX_LIKES_SCANNED", 10)
        es = FakeEs(
            likes_pages=[
                likes_response([
                    like_hit("at://post/a", 60),
                    like_hit("at://missing/1", 50),
                    like_hit("at://missing/2", 40),
                    like_hit("at://post/a", 30),
                    like_hit("at://missing/3", 20),
                    like_hit("at://missing/4", 10),
                ]),
                likes_response([
                    like_hit("at://post/b", 1),
                ]),
            ],
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
        assert [call["search_after"] for call in likes_calls] == [None, [10]]

        posts_calls = [call for call in es.calls if call["index"] == "posts"]
        assert post_terms_from_query(posts_calls[0]["query"]) == [
            "at://post/a",
            "at://missing/1",
            "at://missing/2",
            "at://missing/3",
            "at://missing/4",
        ]
        assert post_terms_from_query(posts_calls[1]["query"]) == ["at://post/b"]

    @pytest.mark.asyncio
    async def test_respects_hard_likes_scan_cap(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        monkeypatch.setattr(network_likes_module, "LIKED_POSTS_PAGE_SIZE", 1)
        monkeypatch.setattr(network_likes_module, "MAX_LIKES_SCANNED", 4)
        es = FakeEs(likes_pages=[
            likes_response([
                like_hit("at://missing/1", 4),
                like_hit("at://missing/2", 3),
                like_hit("at://missing/3", 2),
                like_hit("at://missing/4", 1),
            ])
        ])

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=2)

        assert candidates == []
        likes_calls = [call for call in es.calls if call["index"] == "likes"]
        assert len(likes_calls) == 1
        assert likes_calls[0]["size"] == 4

    @pytest.mark.asyncio
    async def test_equal_like_counts_tie_break_by_last_seen_recency(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes_pages=[
                likes_response([
                    like_hit("at://post/a", 4),
                    like_hit("at://post/b", 3),
                    like_hit("at://post/b", 2),
                    like_hit("at://post/a", 1),
                ])
            ],
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
        async def fake_get_followed_user_dids(user_did: str, limit: int):
            raise bsky_module.FollowedUsersLookupError("lookup exploded")

        monkeypatch.setattr(
            network_likes_module,
            "get_followed_user_dids",
            fake_get_followed_user_dids,
        )
        es = FakeEs()

        candidates = await network_likes_search(es, "did:plc:user1", num_candidates=10)

        assert candidates == []
        assert es.calls == []


class TestNetworkLikesCandidateGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "network_likes"

    @pytest.mark.asyncio
    async def test_generate(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(
            likes_pages=[
                likes_response([
                    like_hit("at://post/a", 2),
                    like_hit("at://post/a", 1),
                ])
            ],
            posts_by_uri={"at://post/a": post_hit("at://post/a")},
        )

        result = await generator.generate(es, "did:plc:user1", num_candidates=1)

        assert result.generator_name == "network_likes"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://post/a"
        assert result.candidates[0].score == 2.0
        assert result.candidates[0].generator_name == "network_likes"

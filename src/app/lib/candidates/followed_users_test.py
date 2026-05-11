"""Tests for the followed_users candidate generator."""

import httpx
import pytest

from ..candidates import followed_users as followed_users_module
from ..candidates.followed_users import (
    FollowedUsersCandidateGenerator,
    FollowedUsersLookupError,
    followed_users_search,
    get_followed_user_dids,
)
from ..embeddings import MINILM_L12_EMBEDDING_KEY


@pytest.fixture
def generator():
    return FollowedUsersCandidateGenerator()


class FakeEs:
    """Configurable fake Elasticsearch client for unit tests."""

    def __init__(self, responses: dict | None = None):
        self._responses = responses or {}
        self._default = {"hits": {"hits": []}}
        self.calls: list[dict] = []

    async def search(self, *, index=None, query=None, size=None, sort=None, **kwargs):
        self.calls.append({
            "index": index,
            "query": query,
            "size": size,
            "sort": sort,
        })
        return self._responses.get(index, self._default)


class FakeResponse:
    def __init__(self, json_data=None, *, json_exc=None, status_exc=None):
        self._json_data = json_data
        self._json_exc = json_exc
        self._status_exc = status_exc

    def raise_for_status(self):
        if self._status_exc is not None:
            raise self._status_exc

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_data


class FakeAsyncClient:
    instances: list["FakeAsyncClient"] = []
    response = FakeResponse({"follows": []})
    responses: list[FakeResponse | Exception] = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.get_calls: list[dict] = []
        self.closed = False
        FakeAsyncClient.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True

    async def get(self, url, *, params=None, **kwargs):
        self.get_calls.append({"url": url, "params": params, "kwargs": kwargs})
        if FakeAsyncClient.responses:
            response = FakeAsyncClient.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return FakeAsyncClient.response


@pytest.fixture
def fake_http_client(monkeypatch):
    FakeAsyncClient.instances = []
    FakeAsyncClient.response = FakeResponse({"follows": []})
    FakeAsyncClient.responses = []
    monkeypatch.setattr(followed_users_module.httpx, "AsyncClient", FakeAsyncClient)
    return FakeAsyncClient


def stub_followed_dids(monkeypatch, dids: list[str]):
    async def fake_get_followed_user_dids(user_did: str, limit: int):
        return dids

    monkeypatch.setattr(
        followed_users_module,
        "get_followed_user_dids",
        fake_get_followed_user_dids,
    )


class TestGetFollowedUserDids:
    @pytest.mark.asyncio
    async def test_returns_followed_dids_and_uses_query_params(self, fake_http_client):
        fake_http_client.response = FakeResponse({
            "follows": [
                {"did": "did:plc:follow1"},
                {"did": "did:plc:follow2"},
            ]
        })

        dids = await get_followed_user_dids("did:plc:user1", limit=50)

        assert dids == ["did:plc:follow1", "did:plc:follow2"]
        client = fake_http_client.instances[0]
        assert client.kwargs == {"timeout": followed_users_module.FOLLOWS_HTTP_TIMEOUT}
        assert client.closed is True
        assert client.get_calls == [{
            "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
            "params": {"actor": "did:plc:user1", "limit": 50},
            "kwargs": {},
        }]

    @pytest.mark.asyncio
    async def test_paginates_follows_with_api_page_limit(self, fake_http_client):
        first_page = [{"did": f"did:plc:follow{i}"} for i in range(100)]
        second_page = [
            {"did": "did:plc:follow100"},
            {"did": "did:plc:follow101"},
            {"did": "did:plc:follow102"},
        ]
        fake_http_client.responses = [
            FakeResponse({"follows": first_page, "cursor": "next-page"}),
            FakeResponse({"follows": second_page}),
        ]

        dids = await get_followed_user_dids("did:plc:user1", limit=103)

        assert dids == [f"did:plc:follow{i}" for i in range(103)]
        client = fake_http_client.instances[0]
        assert client.get_calls == [
            {
                "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
                "params": {"actor": "did:plc:user1", "limit": 100},
                "kwargs": {},
            },
            {
                "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
                "params": {
                    "actor": "did:plc:user1",
                    "limit": 3,
                    "cursor": "next-page",
                },
                "kwargs": {},
            },
        ]

    @pytest.mark.asyncio
    async def test_caps_returned_dids_at_requested_total_limit(self, fake_http_client):
        fake_http_client.responses = [
            FakeResponse({
                "follows": [
                    {"did": "did:plc:follow1"},
                    {"did": "did:plc:follow2"},
                ],
                "cursor": "next-page",
            }),
        ]

        dids = await get_followed_user_dids("did:plc:user1", limit=1)

        assert dids == ["did:plc:follow1"]
        client = fake_http_client.instances[0]
        assert client.get_calls[0]["params"] == {"actor": "did:plc:user1", "limit": 1}

    @pytest.mark.asyncio
    async def test_non_positive_limit_skips_http_request(self, fake_http_client):
        dids = await get_followed_user_dids("did:plc:user1", limit=0)

        assert dids == []
        assert fake_http_client.instances == []

    @pytest.mark.asyncio
    async def test_skips_malformed_follow_entries(self, fake_http_client):
        fake_http_client.response = FakeResponse({
            "follows": [
                {"did": "did:plc:follow1"},
                {"handle": "missing.did"},
                {"did": 123},
                None,
                "not-a-dict",
                {"did": "did:plc:follow2"},
            ]
        })

        dids = await get_followed_user_dids("did:plc:user1", limit=100)

        assert dids == ["did:plc:follow1", "did:plc:follow2"]

    @pytest.mark.asyncio
    async def test_missing_follows_defaults_to_empty_list(self, fake_http_client):
        fake_http_client.response = FakeResponse({})

        dids = await get_followed_user_dids("did:plc:user1", limit=100)

        assert dids == []

    @pytest.mark.asyncio
    async def test_raises_lookup_error_for_http_error(self, fake_http_client):
        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(503, request=request)
        fake_http_client.response = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable",
                request=request,
                response=response,
            )
        )

        with pytest.raises(FollowedUsersLookupError, match="Failed to fetch"):
            await get_followed_user_dids("did:plc:user1", limit=100)

        client = fake_http_client.instances[0]
        assert len(client.get_calls) == 2

    @pytest.mark.asyncio
    async def test_retries_transient_http_error_once(
        self,
        fake_http_client,
        monkeypatch,
    ):
        sleeps = []

        async def fake_sleep(delay):
            sleeps.append(delay)

        monkeypatch.setattr(followed_users_module.asyncio, "sleep", fake_sleep)
        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(429, request=request)
        fake_http_client.responses = [
            FakeResponse(
                status_exc=httpx.HTTPStatusError(
                    "rate limited",
                    request=request,
                    response=response,
                )
            ),
            FakeResponse({"follows": [{"did": "did:plc:follow1"}]}),
        ]

        dids = await get_followed_user_dids("did:plc:user1", limit=100)

        assert dids == ["did:plc:follow1"]
        assert sleeps == [followed_users_module.FOLLOWS_RETRY_BACKOFF_SECONDS]
        client = fake_http_client.instances[0]
        assert len(client.get_calls) == 2

    @pytest.mark.asyncio
    async def test_returns_partial_dids_when_later_page_fails(
        self,
        fake_http_client,
        monkeypatch,
        caplog,
    ):
        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(followed_users_module.asyncio, "sleep", fake_sleep)
        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(503, request=request)
        first_page = [{"did": f"did:plc:follow{i}"} for i in range(100)]
        error_page = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable",
                request=request,
                response=response,
            )
        )
        fake_http_client.responses = [
            FakeResponse({"follows": first_page, "cursor": "next-page"}),
            error_page,
            error_page,
        ]

        dids = await get_followed_user_dids("did:plc:user1", limit=200)

        assert dids == [f"did:plc:follow{i}" for i in range(100)]
        assert "partial followed users" in caplog.text
        client = fake_http_client.instances[0]
        assert len(client.get_calls) == 3

    @pytest.mark.asyncio
    async def test_returns_partial_dids_when_total_lookup_budget_expires(
        self,
        fake_http_client,
        monkeypatch,
        caplog,
    ):
        class ExpiringTimeout:
            def __init__(self, delay):
                self.delay = delay

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                raise TimeoutError

        monkeypatch.setattr(followed_users_module.asyncio, "timeout", ExpiringTimeout)
        fake_http_client.response = FakeResponse({
            "follows": [{"did": "did:plc:follow1"}],
        })

        dids = await get_followed_user_dids("did:plc:user1", limit=100)

        assert dids == ["did:plc:follow1"]
        assert "exceeded" in caplog.text

    @pytest.mark.asyncio
    async def test_raises_lookup_error_for_invalid_json(self, fake_http_client):
        fake_http_client.response = FakeResponse(json_exc=ValueError("bad json"))

        with pytest.raises(FollowedUsersLookupError, match="Failed to fetch"):
            await get_followed_user_dids("did:plc:user1", limit=100)

    @pytest.mark.asyncio
    async def test_raises_lookup_error_when_follows_is_not_list(self, fake_http_client):
        fake_http_client.response = FakeResponse({"follows": {"did": "did:plc:follow1"}})

        with pytest.raises(FollowedUsersLookupError, match="Unexpected follows response"):
            await get_followed_user_dids("did:plc:user1", limit=100)


class TestFollowedUsersSearch:
    @pytest.mark.asyncio
    async def test_returns_candidates_scored(self, monkeypatch):
        async def fake_get_followed_user_dids(user_did: str, limit: int):
            assert user_did == "did:plc:user1"
            assert limit == followed_users_module.MAX_FOLLOWED_USERS
            return ["did:plc:follow1", "did:plc:follow2"]

        monkeypatch.setattr(
            followed_users_module,
            "get_followed_user_dids",
            fake_get_followed_user_dids,
        )
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.91,
                            "_source": {
                                "at_uri": "at://followed/1",
                                "content": "followed post",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.5, 0.6]},
                            },
                        },
                        {
                            "_score": 0.72,
                            "_source": {
                                "at_uri": "at://followed/2",
                                "content": "another followed post",
                                "embeddings": {},
                            },
                        },
                    ]
                }
            }
        })

        candidates = await followed_users_search(
            es,
            "did:plc:user1",
            num_candidates=5,
            generator_name="followed_users",
        )

        assert len(candidates) == 2
        assert candidates[0].at_uri == "at://followed/1"
        assert candidates[0].content == "followed post"
        assert candidates[0].score == 0.91
        assert candidates[0].generator_name == "followed_users"
        assert candidates[0].minilm_l12_embedding is not None

        assert candidates[1].at_uri == "at://followed/2"
        assert candidates[1].score == 0.72
        assert candidates[1].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_sends_followed_author_query_sorted_by_created_at(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1", "did:plc:follow2"])
        es = FakeEs()

        await followed_users_search(es, "did:plc:user1", num_candidates=20)

        assert len(es.calls) == 1
        call = es.calls[0]
        assert call["index"] == "posts"
        assert call["size"] == 20
        assert call["sort"] == [{"created_at": "desc"}]

        query = call["query"]
        assert query == {
            "bool": {
                "filter": [
                    {"terms": {"author_did": ["did:plc:follow1", "did:plc:follow2"]}},
                ],
            }
        }

    @pytest.mark.asyncio
    async def test_video_only_true_includes_filter(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs()

        await followed_users_search(es, "did:plc:user1", num_candidates=10, video_only=True)

        filters = es.calls[0]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} in filters
        assert {"terms": {"author_did": ["did:plc:follow1"]}} in filters

    @pytest.mark.asyncio
    async def test_video_only_false_omits_video_filter(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs()

        await followed_users_search(es, "did:plc:user1", num_candidates=10, video_only=False)

        filters = es.calls[0]["query"]["bool"]["filter"]
        assert {"term": {"contains_video": True}} not in filters

    @pytest.mark.asyncio
    async def test_exclude_uris_adds_must_not_terms(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs()

        await followed_users_search(
            es,
            "did:plc:user1",
            num_candidates=10,
            exclude_uris=["at://post/1", "at://post/2"],
        )

        query = es.calls[0]["query"]
        assert query["bool"]["must_not"] == [
            {"terms": {"at_uri": ["at://post/1", "at://post/2"]}},
        ]

    @pytest.mark.asyncio
    async def test_no_exclude_uris_omits_must_not(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs()

        await followed_users_search(es, "did:plc:user1", num_candidates=10)

        assert "must_not" not in es.calls[0]["query"]["bool"]

    @pytest.mark.asyncio
    async def test_returns_empty_and_skips_es_when_no_followed_users(self, monkeypatch):
        stub_followed_dids(monkeypatch, [])
        es = FakeEs()

        candidates = await followed_users_search(es, "did:plc:nobody", num_candidates=10)

        assert candidates == []
        assert es.calls == []

    @pytest.mark.asyncio
    async def test_lookup_errors_return_empty_and_skip_es(self, monkeypatch):
        async def fake_get_followed_user_dids(user_did: str, limit: int):
            raise FollowedUsersLookupError("lookup exploded")

        monkeypatch.setattr(
            followed_users_module,
            "get_followed_user_dids",
            fake_get_followed_user_dids,
        )
        es = FakeEs()

        candidates = await followed_users_search(
            es,
            "did:plc:user1",
            num_candidates=10,
        )

        assert candidates == []
        assert es.calls == []

    @pytest.mark.asyncio
    async def test_generator_name_defaults_to_none(self, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 1.0,
                            "_source": {
                                "at_uri": "at://followed/1",
                                "content": "post",
                                "embeddings": {},
                            },
                        },
                    ]
                }
            }
        })

        candidates = await followed_users_search(es, "did:plc:user1", num_candidates=1)

        assert candidates[0].generator_name is None


class TestFollowedUsersCandidateGenerator:
    @pytest.mark.asyncio
    async def test_name(self, generator):
        assert generator.name == "followed_users"

    @pytest.mark.asyncio
    async def test_generate(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, ["did:plc:follow1"])
        es = FakeEs(responses={
            "posts": {
                "hits": {
                    "hits": [
                        {
                            "_score": 0.8,
                            "_source": {
                                "at_uri": "at://followed/1",
                                "content": "followed post",
                                "embeddings": {MINILM_L12_EMBEDDING_KEY: [0.1, 0.2]},
                            },
                        },
                    ]
                }
            }
        })

        result = await generator.generate(es, "did:plc:user1", num_candidates=10)

        assert result.generator_name == "followed_users"
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://followed/1"
        assert result.candidates[0].score == 0.8
        assert result.candidates[0].generator_name == "followed_users"

    @pytest.mark.asyncio
    async def test_generate_empty_when_no_followed_users(self, generator, monkeypatch):
        stub_followed_dids(monkeypatch, [])
        es = FakeEs()

        result = await generator.generate(es, "did:plc:nobody", num_candidates=10)

        assert result.generator_name == "followed_users"
        assert result.candidates == []
        assert es.calls == []

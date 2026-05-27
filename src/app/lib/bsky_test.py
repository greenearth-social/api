"""Tests for Bluesky API helpers."""

import httpx
import pytest

from . import bsky as bsky_module
from .bsky import FollowedUsersLookupError, get_followed_user_dids


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
    def __init__(self):
        self.get_calls: list[dict] = []
        self.response: FakeResponse = FakeResponse({"follows": []})
        self.responses: list[FakeResponse | Exception] = []

    async def get(self, url, *, params=None, **kwargs):
        self.get_calls.append({"url": url, "params": params, "kwargs": kwargs})
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return self.response


@pytest.fixture
def fake_http_client(monkeypatch):
    client = FakeAsyncClient()
    monkeypatch.setattr(bsky_module, "get_http_client", lambda: client)
    return client


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
        assert fake_http_client.get_calls == [{
            "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
            "params": {"actor": "did:plc:user1", "limit": 50},
            "kwargs": {"timeout": bsky_module.FOLLOWS_HTTP_TIMEOUT},
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
        assert fake_http_client.get_calls == [
            {
                "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
                "params": {"actor": "did:plc:user1", "limit": 100},
                "kwargs": {"timeout": bsky_module.FOLLOWS_HTTP_TIMEOUT},
            },
            {
                "url": "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
                "params": {
                    "actor": "did:plc:user1",
                    "limit": 3,
                    "cursor": "next-page",
                },
                "kwargs": {"timeout": bsky_module.FOLLOWS_HTTP_TIMEOUT},
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
        assert fake_http_client.get_calls[0]["params"] == {"actor": "did:plc:user1", "limit": 1}

    @pytest.mark.asyncio
    async def test_non_positive_limit_skips_http_request(self, fake_http_client):
        dids = await get_followed_user_dids("did:plc:user1", limit=0)

        assert dids == []
        assert fake_http_client.get_calls == []

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

        assert len(fake_http_client.get_calls) == 2

    @pytest.mark.asyncio
    async def test_retries_transient_http_error_once(
        self,
        fake_http_client,
        monkeypatch,
    ):
        sleeps = []

        async def fake_sleep(delay):
            sleeps.append(delay)

        monkeypatch.setattr(bsky_module.asyncio, "sleep", fake_sleep)
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
        assert sleeps == [bsky_module.FOLLOWS_RETRY_BACKOFF_SECONDS]
        assert len(fake_http_client.get_calls) == 2

    @pytest.mark.asyncio
    async def test_returns_partial_dids_when_later_page_fails(
        self,
        fake_http_client,
        monkeypatch,
        caplog,
    ):
        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(bsky_module.asyncio, "sleep", fake_sleep)
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
        assert len(fake_http_client.get_calls) == 3

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

        monkeypatch.setattr(bsky_module.asyncio, "timeout", ExpiringTimeout)
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

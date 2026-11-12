"""Tests for Bluesky API helpers."""

import httpx
import pytest

from . import bsky as bsky_module
from .bsky import (
    AccountGoneError,
    FollowedUsersLookupError,
    FollowsFetch,
    fetch_follow_records,
    fetch_followed_user_dids,
    fetch_visible_follow_records,
    get_followed_user_dids,
)


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


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


class TestGetFollowedUserDidsFailureCounters:
    """bsky.follows.failure_count is recorded with a status_code label once
    per failed lookup, whether it raises or falls back to partial results."""

    @pytest.mark.asyncio
    async def test_counts_http_status_failures(self, fake_http_client, monkeypatch):
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(503, request=request)
        fake_http_client.response = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable", request=request, response=response
            )
        )

        with pytest.raises(FollowedUsersLookupError):
            await get_followed_user_dids("did:plc:x", limit=10)

        assert ("bsky.follows.failure_count", 1, {"status_code": "503"}) in collector.records

    @pytest.mark.asyncio
    async def test_counts_timeouts(self, fake_http_client, monkeypatch):
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        request = httpx.Request("GET", "https://example.test")
        fake_http_client.response = FakeResponse(
            status_exc=httpx.ConnectTimeout("connect timed out", request=request)
        )

        with pytest.raises(FollowedUsersLookupError):
            await get_followed_user_dids("did:plc:x", limit=10)

        assert ("bsky.follows.failure_count", 1, {"status_code": "timeout"}) in collector.records

    @pytest.mark.asyncio
    async def test_counts_connection_failures(self, fake_http_client, monkeypatch):
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        request = httpx.Request("GET", "https://example.test")
        fake_http_client.response = FakeResponse(
            status_exc=httpx.ConnectError("connection refused", request=request)
        )

        with pytest.raises(FollowedUsersLookupError):
            await get_followed_user_dids("did:plc:x", limit=10)

        assert (
            "bsky.follows.failure_count",
            1,
            {"status_code": "connection"},
        ) in collector.records

    @pytest.mark.asyncio
    async def test_counts_exactly_once_when_first_page_fails_with_no_partials(
        self, fake_http_client, monkeypatch
    ):
        """A lookup that fails on its first page (no partial data accumulated
        yet) re-raises out of the inner per-page handler and is counted once
        by the outer handler that catches it -- not twice."""
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(503, request=request)
        fake_http_client.response = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable", request=request, response=response
            )
        )

        with pytest.raises(FollowedUsersLookupError):
            await get_followed_user_dids("did:plc:x", limit=10)

        failures = [r for r in collector.records if r[0] == "bsky.follows.failure_count"]
        assert failures == [("bsky.follows.failure_count", 1, {"status_code": "503"})]

    @pytest.mark.asyncio
    async def test_counts_malformed_first_page_with_no_partials(
        self, fake_http_client, monkeypatch
    ):
        """A malformed first-page response (not a list of follows) raises
        FollowedUsersLookupError through the inner handler's bare re-raise,
        then through the outer `except FollowedUsersLookupError: raise`
        clause -- it must still be counted exactly once, matching the
        malformed-response-with-partials case."""
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        fake_http_client.response = FakeResponse({"follows": {"did": "not-a-list"}})

        with pytest.raises(FollowedUsersLookupError, match="Unexpected follows response"):
            await get_followed_user_dids("did:plc:x", limit=10)

        failures = [r for r in collector.records if r[0] == "bsky.follows.failure_count"]
        assert failures == [("bsky.follows.failure_count", 1, {"status_code": "other"})]

    @pytest.mark.asyncio
    async def test_counts_once_for_partial_result_fallback(
        self, fake_http_client, monkeypatch, caplog
    ):
        """A later-page failure that falls back to partial results still
        counts as one external failure."""
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)

        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(bsky_module.asyncio, "sleep", fake_sleep)
        request = httpx.Request("GET", "https://example.test")
        response = httpx.Response(503, request=request)
        first_page = [{"did": f"did:plc:follow{i}"} for i in range(100)]
        error_page = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable", request=request, response=response
            )
        )
        fake_http_client.responses = [
            FakeResponse({"follows": first_page, "cursor": "next-page"}),
            error_page,
            error_page,
        ]

        dids = await get_followed_user_dids("did:plc:user1", limit=200)

        assert dids == [f"did:plc:follow{i}" for i in range(100)]
        failures = [r for r in collector.records if r[0] == "bsky.follows.failure_count"]
        assert failures == [("bsky.follows.failure_count", 1, {"status_code": "503"})]


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


class TestFetchFollowedUserDidsCompleteness:
    """``complete`` distinguishes an exhaustive walk from a truncated one.

    The followed-users cache persists this: an entry built from an incomplete
    fetch must never be treated as authoritative, or a single Bluesky blip
    would pin a user to a truncated follow set for a whole TTL.
    """

    @pytest.mark.asyncio
    async def test_complete_when_cursor_is_exhausted(self, fake_http_client):
        fake_http_client.response = FakeResponse({
            "follows": [{"did": "did:plc:follow1"}],
        })

        result = await fetch_followed_user_dids("did:plc:user1", limit=100)

        assert result == FollowsFetch(dids=["did:plc:follow1"], complete=True)

    @pytest.mark.asyncio
    async def test_complete_when_limit_cap_is_reached_with_cursor_remaining(
        self,
        fake_http_client,
    ):
        # Stopping at MAX_FOLLOWED_USERS is a deliberate bound, not a failure:
        # the walk did everything it was asked to do.
        page = [{"did": f"did:plc:follow{i}"} for i in range(100)]
        fake_http_client.response = FakeResponse({"follows": page, "cursor": "more"})

        result = await fetch_followed_user_dids("did:plc:user1", limit=100)

        assert result.complete is True
        assert len(result.dids) == 100

    @pytest.mark.asyncio
    async def test_incomplete_when_a_later_page_fails(self, fake_http_client, monkeypatch):
        async def fake_sleep(delay):
            return None

        monkeypatch.setattr(bsky_module.asyncio, "sleep", fake_sleep)
        request = httpx.Request("GET", "https://example.test")
        error_page = FakeResponse(
            status_exc=httpx.HTTPStatusError(
                "service unavailable",
                request=request,
                response=httpx.Response(503, request=request),
            )
        )
        first_page = [{"did": f"did:plc:follow{i}"} for i in range(100)]
        fake_http_client.responses = [
            FakeResponse({"follows": first_page, "cursor": "next-page"}),
            error_page,
            error_page,
        ]

        result = await fetch_followed_user_dids("did:plc:user1", limit=200)

        assert result.complete is False
        assert len(result.dids) == 100

    @pytest.mark.asyncio
    async def test_incomplete_when_budget_expires(self, fake_http_client, monkeypatch):
        class ExpiringTimeout:
            def __init__(self, delay):
                self.delay = delay

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                raise TimeoutError

        monkeypatch.setattr(bsky_module.asyncio, "timeout", ExpiringTimeout)
        fake_http_client.response = FakeResponse({"follows": [{"did": "did:plc:follow1"}]})

        result = await fetch_followed_user_dids("did:plc:user1", limit=100)

        assert result == FollowsFetch(dids=["did:plc:follow1"], complete=False)

    @pytest.mark.asyncio
    async def test_empty_result_is_complete(self, fake_http_client):
        # A user who follows nobody is a legitimate complete answer, not a
        # cold entry to be refreshed on every request forever.
        fake_http_client.response = FakeResponse({"follows": []})

        result = await fetch_followed_user_dids("did:plc:user1", limit=100)

        assert result == FollowsFetch(dids=[], complete=True)

    @pytest.mark.asyncio
    async def test_raises_when_nothing_could_be_fetched(self, fake_http_client):
        fake_http_client.response = FakeResponse(json_exc=ValueError("bad json"))

        with pytest.raises(FollowedUsersLookupError):
            await fetch_followed_user_dids("did:plc:user1", limit=100)

    @pytest.mark.asyncio
    async def test_caller_supplied_budget_overrides_the_request_path_default(
        self,
        fake_http_client,
        monkeypatch,
    ):
        # The background refresh is not on the request path, so it buys a
        # bigger budget than the 1s clamp a feed request can afford.
        budgets: list[float] = []

        class RecordingTimeout:
            def __init__(self, delay):
                budgets.append(delay)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        monkeypatch.setattr(bsky_module.asyncio, "timeout", RecordingTimeout)
        fake_http_client.response = FakeResponse({"follows": []})

        await fetch_followed_user_dids("did:plc:user1", limit=100, timeout_seconds=15.0)
        await fetch_followed_user_dids("did:plc:user1", limit=100)

        assert budgets == [15.0, bsky_module.FOLLOWS_LOOKUP_TIMEOUT_SECONDS]

    @pytest.mark.asyncio
    async def test_get_followed_user_dids_still_returns_a_plain_list(self, fake_http_client):
        fake_http_client.response = FakeResponse({"follows": [{"did": "did:plc:follow1"}]})

        assert await get_followed_user_dids("did:plc:user1", limit=100) == ["did:plc:follow1"]


def _record(rkey: str, subject: str) -> dict:
    return {
        "uri": f"at://did:plc:me/app.bsky.graph.follow/{rkey}",
        "value": {"subject": subject, "createdAt": "2026-01-01T00:00:00Z"},
    }


class TestFetchFollowRecords:
    @pytest.fixture(autouse=True)
    def _pds(self, monkeypatch):
        self.resolved: list[str] = []

        async def resolve(user_did):
            self.resolved.append(user_did)
            return "https://pds.example"

        monkeypatch.setattr(bsky_module, "_resolve_pds", resolve)

    @pytest.mark.asyncio
    async def test_walks_the_users_follow_records_on_their_pds(self, fake_http_client):
        fake_http_client.responses = [
            FakeResponse({"records": [_record("r1", "did:plc:a")], "cursor": "c1"}),
            FakeResponse({"records": [_record("r2", "did:plc:b")]}),
        ]

        fetch = await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)

        assert fetch == FollowsFetch(
            dids=["did:plc:a", "did:plc:b"], complete=True, rkeys=["r1", "r2"]
        )
        # The PDS is resolved once per walk, not per page.
        assert self.resolved == ["did:plc:me"]
        first, second = fake_http_client.get_calls
        assert first["url"] == "https://pds.example/xrpc/com.atproto.repo.listRecords"
        assert first["params"] == {
            "repo": "did:plc:me",
            "collection": "app.bsky.graph.follow",
            "limit": 10,
        }
        assert second["params"]["cursor"] == "c1"

    @pytest.mark.asyncio
    async def test_walk_ending_on_its_own_is_not_capped(self, fake_http_client):
        fake_http_client.response = FakeResponse({"records": [_record("r1", "did:plc:a")]})

        fetch = await fetch_follow_records("did:plc:me", limit=1, timeout_seconds=5)

        assert fetch.capped is False

    @pytest.mark.asyncio
    async def test_caps_at_limit_and_keeps_rkeys_aligned(self, fake_http_client):
        fake_http_client.response = FakeResponse({
            "records": [_record("r1", "did:plc:a"), _record("r2", "did:plc:b"),
                        _record("r3", "did:plc:c")],
            "cursor": "more",
        })

        fetch = await fetch_follow_records("did:plc:me", limit=2, timeout_seconds=5)

        assert fetch.dids == ["did:plc:a", "did:plc:b"]
        assert fetch.rkeys == ["r1", "r2"]
        assert fetch.complete is True
        assert fetch.capped is True

    @pytest.mark.asyncio
    async def test_skips_malformed_records(self, fake_http_client):
        fake_http_client.response = FakeResponse({
            "records": [
                _record("r1", "did:plc:a"),
                {"uri": "at://did:plc:me/app.bsky.graph.follow/r2"},
                {"value": {"subject": "did:plc:c"}},
                "nonsense",
            ],
        })

        fetch = await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)

        assert fetch.dids == ["did:plc:a"]
        assert fetch.rkeys == ["r1"]

    @pytest.mark.asyncio
    async def test_partial_walk_is_incomplete_with_aligned_rkeys(self, fake_http_client):
        request = httpx.Request("GET", "https://pds.example")
        fake_http_client.responses = [
            FakeResponse({"records": [_record("r1", "did:plc:a")], "cursor": "c1"}),
            httpx.ConnectError("down", request=request),
        ]

        fetch = await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)

        assert fetch == FollowsFetch(dids=["did:plc:a"], complete=False, rkeys=["r1"])

    @pytest.mark.asyncio
    async def test_unresolvable_pds_raises_lookup_error(self, fake_http_client, monkeypatch):
        async def fail(user_did):
            raise FollowedUsersLookupError("no pds")

        monkeypatch.setattr(bsky_module, "_resolve_pds", fail)

        with pytest.raises(FollowedUsersLookupError):
            await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)
        assert fake_http_client.get_calls == []

    @pytest.mark.asyncio
    async def test_getfollows_walks_carry_no_rkeys(self, fake_http_client):
        fake_http_client.response = FakeResponse({"follows": [{"did": "did:plc:a"}]})

        fetch = await fetch_followed_user_dids("did:plc:me", limit=10)

        assert fetch.rkeys == []


class TestFetchVisibleFollowRecords:
    """Follow records filtered to the follows getFollows shows."""

    @pytest.fixture
    def walks(self, monkeypatch):
        self.records: FollowsFetch | Exception = FollowsFetch(
            dids=["did:plc:a", "did:plc:blocked", "did:plc:b"],
            rkeys=["ra", "rx", "rb"],
            capped=True,
        )
        self.visible: FollowsFetch | Exception = FollowsFetch(dids=["did:plc:b", "did:plc:a"])

        async def records(*args, **kwargs):
            if isinstance(self.records, Exception):
                raise self.records
            return self.records

        async def visible(*args, **kwargs):
            if isinstance(self.visible, Exception):
                raise self.visible
            return self.visible

        monkeypatch.setattr(bsky_module, "fetch_follow_records", records)
        monkeypatch.setattr(bsky_module, "fetch_followed_user_dids", visible)

    @pytest.mark.asyncio
    async def test_drops_records_getfollows_hides(self, walks):
        fetch = await fetch_visible_follow_records("did:plc:me", 10, 5)

        assert fetch == FollowsFetch(
            dids=["did:plc:a", "did:plc:b"], rkeys=["ra", "rb"], complete=True, capped=True
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "visible",
        [FollowedUsersLookupError("appview down"), FollowsFetch(dids=["did:plc:a"],
                                                                complete=False)],
    )
    async def test_unfiltered_but_incomplete_without_a_full_getfollows(self, walks, visible):
        self.visible = visible

        fetch = await fetch_visible_follow_records("did:plc:me", 10, 5)

        assert fetch.dids == ["did:plc:a", "did:plc:blocked", "did:plc:b"]
        assert fetch.rkeys == ["ra", "rx", "rb"]
        assert fetch.complete is False

    @pytest.mark.asyncio
    async def test_records_failure_raises(self, walks):
        self.records = FollowedUsersLookupError("pds down")

        with pytest.raises(FollowedUsersLookupError):
            await fetch_visible_follow_records("did:plc:me", 10, 5)


def _status_400(body: dict) -> FakeResponse:
    request = httpx.Request("GET", "https://example.test")
    response = httpx.Response(400, json=body, request=request)
    return FakeResponse(
        status_exc=httpx.HTTPStatusError("bad request", request=request, response=response)
    )


class TestAccountGone:
    """Only an explicit "not found" from the service walked means gone."""

    @pytest.fixture(autouse=True)
    def _pds(self, monkeypatch):
        async def resolve(user_did):
            return "https://pds.example"

        monkeypatch.setattr(bsky_module, "_resolve_pds", resolve)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body",
        [
            {"error": "InvalidRequest", "message": "Could not find repo: did:plc:me"},
            {"error": "RepoTakendown", "message": "Repo has been takendown"},
            {"error": "RepoDeactivated", "message": "Repo has been deactivated"},
        ],
    )
    async def test_pds_without_the_repo(self, fake_http_client, body):
        fake_http_client.response = _status_400(body)

        with pytest.raises(AccountGoneError):
            await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body",
        [
            {"error": "InvalidRequest", "message": "Actor not found: did:plc:me"},
            {"error": "AccountTakedown", "message": "Account has been suspended"},
        ],
    )
    async def test_appview_without_the_actor(self, fake_http_client, body):
        fake_http_client.response = _status_400(body)

        with pytest.raises(AccountGoneError):
            await fetch_followed_user_dids("did:plc:me", limit=10)

    @pytest.mark.asyncio
    async def test_a_gone_account_is_not_counted_as_a_failure(
        self, fake_http_client, monkeypatch
    ):
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)
        fake_http_client.response = _status_400(
            {"error": "InvalidRequest", "message": "Actor not found: did:plc:me"}
        )

        with pytest.raises(AccountGoneError):
            await fetch_followed_user_dids("did:plc:me", limit=10)
        assert collector.records == []

    @pytest.mark.asyncio
    async def test_any_other_400_is_an_ordinary_failure(self, fake_http_client, monkeypatch):
        collector = _RecordingCollector()
        monkeypatch.setattr(bsky_module, "get_metric_collector", lambda: collector)
        fake_http_client.response = _status_400(
            {"error": "InvalidRequest", "message": "Error: limit must be <= 100"}
        )

        with pytest.raises(FollowedUsersLookupError) as raised:
            await fetch_follow_records("did:plc:me", limit=10, timeout_seconds=5)
        assert not isinstance(raised.value, AccountGoneError)
        assert [r[0] for r in collector.records] == ["bsky.follows.failure_count"]


class TestFetchVisibleFollowRecordsAccountGone:
    @pytest.fixture
    def walks(self, monkeypatch):
        self.records: FollowsFetch | Exception = AccountGoneError("no repo")
        self.visible: FollowsFetch | Exception = AccountGoneError("no actor")

        async def records(*args, **kwargs):
            if isinstance(self.records, Exception):
                raise self.records
            return self.records

        async def visible(*args, **kwargs):
            if isinstance(self.visible, Exception):
                raise self.visible
            return self.visible

        monkeypatch.setattr(bsky_module, "fetch_follow_records", records)
        monkeypatch.setattr(bsky_module, "fetch_followed_user_dids", visible)

    @pytest.mark.asyncio
    async def test_gone_when_pds_and_appview_agree(self, walks):
        with pytest.raises(AccountGoneError):
            await fetch_visible_follow_records("did:plc:me", 10, 5)

    @pytest.mark.asyncio
    async def test_pds_alone_is_an_ordinary_failure(self, walks):
        self.visible = FollowsFetch(dids=["did:plc:a"])

        with pytest.raises(FollowedUsersLookupError) as raised:
            await fetch_visible_follow_records("did:plc:me", 10, 5)
        assert not isinstance(raised.value, AccountGoneError)

    @pytest.mark.asyncio
    async def test_appview_alone_keeps_the_records_unfiltered(self, walks):
        self.records = FollowsFetch(dids=["did:plc:a"], rkeys=["ra"])

        fetch = await fetch_visible_follow_records("did:plc:me", 10, 5)

        assert fetch.dids == ["did:plc:a"]
        assert fetch.complete is False

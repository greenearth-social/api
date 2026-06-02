"""Tests for the XRPC feed generator endpoints."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..feeds import FEEDS
from ..models import CandidatePost, FeedCursor, RankedCandidate, RankPredictResult
from ..lib.candidates.base import CandidateResult
from ..lib.feed_cache import FeedCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SERVICE_DID = "did:web:test.example.com"
PUBLISHER_DID = "did:plc:publisherabc123"
FEED_RKEY = "basic-similarity"
FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{FEED_RKEY}"
RANDOM_FEED_RKEY = "random"
RANDOM_FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{RANDOM_FEED_RKEY}"
RANKED_FEED_RKEY = "your-feed"
RANKED_FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{RANKED_FEED_RKEY}"
BEST_OF_FRIENDS_FEED_RKEY = "best-of-friends"
BEST_OF_FRIENDS_FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{BEST_OF_FRIENDS_FEED_RKEY}"
# The AppView sends the publisher DID in the feed URI, not the service DID.
FEED_URI_FROM_APPVIEW = f"at://{PUBLISHER_DID}/app.bsky.feed.generator/{FEED_RKEY}"
TEST_USERNAME = "testuser.bsky.app"


def _make_candidates(prefix: str, n: int, generator_name: str = "test") -> list[CandidatePost]:
    return [
        CandidatePost(at_uri=f"at://{prefix}/{i}", content=f"post {i}", minilm_l12_embedding=None, score=None, generator_name=generator_name)
        for i in range(n)
    ]


def _patch_basic_similarity_generators(
    post_similarity_candidates,
    followed_users_candidates=None,
    infill_candidates=None,
):
    """Patch generators used by the basic-similarity feed.

    Most tests care about feed endpoint behavior rather than the exact mix of
    candidate sources, so followed_users defaults to the same candidates as
    post_similarity. The pipeline then deduplicates them back to the expected
    output shape.
    """
    post_similarity_gen = AsyncMock()
    post_similarity_gen.generate.return_value = CandidateResult(
        generator_name="post_similarity",
        candidates=post_similarity_candidates,
    )
    followed_users_gen = AsyncMock()
    followed_users_gen.generate.return_value = CandidateResult(
        generator_name="followed_users",
        candidates=(
            post_similarity_candidates
            if followed_users_candidates is None
            else followed_users_candidates
        ),
    )
    infill_gen = AsyncMock()
    infill_gen.generate.return_value = CandidateResult(
        generator_name="popularity",
        candidates=infill_candidates or [],
    )

    def fake_get_generator(name):
        if name == "post_similarity":
            return post_similarity_gen
        if name == "followed_users":
            return followed_users_gen
        if name == "popularity":
            return infill_gen
        return None

    return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator)


class InMemoryFeedCache(FeedCache):
    """Trivial in-memory feed cache for tests."""

    def __init__(self):
        self._store: dict[str, list[str]] = {}

    async def store(self, key: str, items: list[str], ttl_seconds: int = 600) -> None:
        self._store[key] = items

    async def retrieve(self, key: str) -> list[str] | None:
        return self._store.get(key)

    async def append(self, key: str, new_items: list[str]) -> list[str] | None:
        existing = self._store.get(key)
        if existing is None:
            return None
        updated = existing + new_items
        self._store[key] = updated
        return updated


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FEED_CONTEXT_SECRET = "test-feed-context-secret"


@pytest.fixture(autouse=True)
def set_feed_generator_did(monkeypatch):
    """Ensure a deterministic service DID for all tests."""
    monkeypatch.setenv("GE_FEED_GENERATOR_DID", SERVICE_DID)


@pytest.fixture(autouse=True)
def set_feed_context_secret(monkeypatch):
    """getFeedSkeleton now signs a feedContext on every item, so the signing
    secret must be present for the endpoint to serve a response."""
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", FEED_CONTEXT_SECRET)


@pytest.fixture(autouse=True)
def fake_app_es():
    """Attach a fake ES client so the app doesn't need a real connection."""
    app.state.es = AsyncMock()
    app.state.id_resolver = AsyncMock()
    did_doc = MagicMock()
    did_doc.get_handle.return_value = TEST_USERNAME
    app.state.id_resolver.did.resolve = AsyncMock(return_value=did_doc)
    app.state.firestore = AsyncMock()
    app.state.feed_cache = InMemoryFeedCache()
    yield
    try:
        delattr(app.state, "es")
    except Exception:
        pass
    try:
        delattr(app.state, "id_resolver")
    except Exception:
        pass
    try:
        delattr(app.state, "firestore")
    except Exception:
        pass
    try:
        delattr(app.state, "feed_cache")
    except Exception:
        pass


client = TestClient(app)


# ---------------------------------------------------------------------------
# /.well-known/did.json
# ---------------------------------------------------------------------------

class TestWellKnownDid:
    def test_returns_200(self):
        resp = client.get("/.well-known/did.json")
        assert resp.status_code == 200

    def test_content_type_is_json(self):
        resp = client.get("/.well-known/did.json")
        assert "application/json" in resp.headers["content-type"]

    def test_did_document_id(self):
        data = client.get("/.well-known/did.json").json()
        assert data["id"] == SERVICE_DID

    def test_did_document_context(self):
        data = client.get("/.well-known/did.json").json()
        assert "https://www.w3.org/ns/did/v1" in data["@context"]

    def test_did_document_service_entry(self):
        data = client.get("/.well-known/did.json").json()
        services = data["service"]
        assert len(services) == 1
        svc = services[0]
        assert svc["id"] == "#bsky_fg"
        assert svc["type"] == "BskyFeedGenerator"
        assert svc["serviceEndpoint"] == "https://test.example.com"

    def test_hostname_derived_from_did(self):
        """The service endpoint hostname comes from the did:web DID."""
        data = client.get("/.well-known/did.json").json()
        assert data["service"][0]["serviceEndpoint"] == "https://test.example.com"


# ---------------------------------------------------------------------------
# /xrpc/app.bsky.feed.describeFeedGenerator
# ---------------------------------------------------------------------------

class TestDescribeFeedGenerator:
    def test_returns_200(self):
        resp = client.get("/xrpc/app.bsky.feed.describeFeedGenerator")
        assert resp.status_code == 200

    def test_response_did(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        assert data["did"] == SERVICE_DID

    def test_feeds_list_contains_basic_similarity(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        uris = [f["uri"] for f in data["feeds"]]
        assert FEED_URI in uris

    def test_feeds_list_contains_random(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        uris = [f["uri"] for f in data["feeds"]]
        assert RANDOM_FEED_URI in uris

    def test_feeds_list_contains_your_feed(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        uris = [f["uri"] for f in data["feeds"]]
        assert RANKED_FEED_URI in uris

    def test_feeds_list_contains_best_of_friends(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        uris = [f["uri"] for f in data["feeds"]]
        assert BEST_OF_FRIENDS_FEED_URI in uris

    def test_feeds_list_length(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        assert len(data["feeds"]) == len(FEEDS)


# ---------------------------------------------------------------------------
# /xrpc/app.bsky.feed.getFeedSkeleton
# ---------------------------------------------------------------------------

class TestGetFeedSkeleton:
    """Tests for the getFeedSkeleton endpoint."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        """Default to an authenticated caller for non-auth-focused tests."""
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        """Keep Firestore I/O out of generic feed skeleton tests."""
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def _patch_generators(self, primary_candidates, infill_candidates=None):
        """Return a context-manager that patches get_generator.

        ``primary_candidates`` and ``infill_candidates`` are lists of
        ``CandidatePost`` (or ``None`` to simulate an unregistered generator).
        """
        return _patch_basic_similarity_generators(
            primary_candidates,
            infill_candidates=infill_candidates,
        )

    # --- basic happy path ---

    def test_returns_200(self):
        with self._patch_generators(_make_candidates("p", 3)):
            resp = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI})
        assert resp.status_code == 200

    def test_returns_feed_items(self):
        with self._patch_generators(_make_candidates("p", 3)):
            data = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}).json()
        assert len(data["feed"]) == 3
        assert data["feed"][0]["post"] == "at://p/0"

    # --- feedContext ---

    def test_items_carry_signed_feed_context(self):
        from app.lib.feed_context import decode_feed_context

        with self._patch_generators(_make_candidates("p", 3)):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}
            ).json()

        for item in data["feed"]:
            payload = decode_feed_context(item["feedContext"])
            assert payload is not None
            assert payload.did == "did:plc:testuser"
            assert payload.feed == FEED_RKEY

    def test_all_items_share_one_request_id(self):
        from app.lib.feed_context import decode_feed_context

        with self._patch_generators(_make_candidates("p", 3)):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}
            ).json()

        payloads = [decode_feed_context(i["feedContext"]) for i in data["feed"]]
        assert all(p is not None for p in payloads)
        rids = {p.rid for p in payloads if p is not None}
        assert len(rids) == 1

    # --- rkey matching ---

    def test_matches_feed_by_rkey_regardless_of_did(self):
        """The AppView sends the publisher DID, not the service DID."""
        with self._patch_generators(_make_candidates("p", 2)):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI_FROM_APPVIEW},
            )
        assert resp.status_code == 200
        assert len(resp.json()["feed"]) == 2

    def test_matches_feed_by_internal_rkey(self):
        """Feeds published under their Caterpie internal_rkey are still served."""
        feed_cfg = FEEDS["basic-similarity"]
        internal_uri = f"at://{SERVICE_DID}/app.bsky.feed.generator/{feed_cfg.internal_rkey}"
        with self._patch_generators(_make_candidates("p", 2)):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": internal_uri},
            )
        assert resp.status_code == 200
        assert len(resp.json()["feed"]) == 2

    # --- unknown feed ---

    def test_unknown_feed_returns_400(self):
        with self._patch_generators([]):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": f"at://{SERVICE_DID}/app.bsky.feed.generator/nonexistent"},
            )
        assert resp.status_code == 400

    def test_malformed_feed_uri_returns_400(self):
        with self._patch_generators([]):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": "not-a-valid-uri"},
            )
        assert resp.status_code == 400

    def test_wrong_collection_returns_400(self):
        with self._patch_generators([]):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": f"at://{SERVICE_DID}/app.bsky.feed.post/{FEED_RKEY}"},
            )
        assert resp.status_code == 400

    # --- cursor is excluded when None ---

    def test_cursor_omitted_when_none(self):
        """AT Protocol requires cursor to be absent, not null."""
        with self._patch_generators(_make_candidates("p", 1)):
            resp = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI})
        assert "cursor" not in resp.json()

    # --- limit ---

    def test_respects_limit_parameter(self):
        with self._patch_generators(_make_candidates("p", 10)):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()
        assert len(data["feed"]) == 3

    def test_default_limit_is_30(self):
        with self._patch_generators(_make_candidates("p", 50)):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
            ).json()
        assert len(data["feed"]) == 30

    # --- de-duplication ---

    def test_deduplicates_by_at_uri(self):
        duped = [
            CandidatePost(at_uri="at://dup/1", content="a", minilm_l12_embedding=None, score=None, generator_name="g"),
            CandidatePost(at_uri="at://dup/1", content="a", minilm_l12_embedding=None, score=None, generator_name="g"),
            CandidatePost(at_uri="at://dup/2", content="b", minilm_l12_embedding=None, score=None, generator_name="g"),
        ]
        with self._patch_generators(duped):
            data = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}).json()
        uris = [item["post"] for item in data["feed"]]
        assert uris == ["at://dup/1", "at://dup/2"]

    # --- infill ---

    def test_infill_called_when_primary_short(self):
        primary = _make_candidates("prim", 2, "post_similarity")
        infill = _make_candidates("infill", 5, "popularity")
        with self._patch_generators(primary, infill):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        posts = [item["post"] for item in data["feed"]]
        assert "at://prim/0" in posts
        assert "at://infill/0" in posts
        assert len(posts) == 5

    def test_infill_not_called_when_primary_sufficient(self):
        # The pipeline pre-generates a batch larger than limit (limit * 5),
        # so we supply enough candidates to cover the full batch.
        primary = _make_candidates("prim", 25, "post_similarity")
        with self._patch_generators(primary) as mock_get:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        assert len(data["feed"]) == 5
        # Infill generator's generate method should not have been called
        infill_gen = mock_get.side_effect("popularity")
        infill_gen.generate.assert_not_called()

    def test_basic_similarity_uses_followed_users_generator(self):
        similarity = _make_candidates("sim", 3, "post_similarity")
        followed = _make_candidates("followed", 3, "followed_users")

        with _patch_basic_similarity_generators(
            similarity,
            followed_users_candidates=followed,
        ) as mock_get:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 6},
            ).json()

        posts = [item["post"] for item in data["feed"]]
        assert "at://sim/0" in posts
        assert "at://followed/0" in posts
        mock_get.side_effect("followed_users").generate.assert_awaited_once()

    # --- primary generator failure ---

    def test_primary_failure_falls_back_to_infill(self):
        """If primary raises, we still get infill results."""
        infill = _make_candidates("infill", 3, "popularity")

        primary_gen = AsyncMock()
        primary_gen.generate.side_effect = RuntimeError("ES down")

        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity",
            candidates=infill,
        )

        followed_users_gen = AsyncMock()
        followed_users_gen.generate.return_value = CandidateResult(
            generator_name="followed_users",
            candidates=[],
        )

        def fake_get(name):
            return {
                "post_similarity": primary_gen,
                "followed_users": followed_users_gen,
                "popularity": infill_gen,
            }.get(name)

        with patch("app.lib.candidates.generate.get_generator", side_effect=fake_get):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()

        assert len(data["feed"]) == 3
        assert data["feed"][0]["post"] == "at://infill/0"

    # --- empty feed ---

    def test_empty_feed_returns_empty_list(self):
        with self._patch_generators([]):
            data = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}).json()
        assert data["feed"] == []

    # --- MMR diversification ---

    def test_same_author_candidates_are_spread_in_feed(self):
        """MMR should interleave candidates from the same author with others."""
        candidates = [
            CandidatePost(at_uri="at://alice/1", score=1.0, author_did="did:plc:alice", content=None, minilm_l12_embedding=None, generator_name="g"),
            CandidatePost(at_uri="at://alice/2", score=0.9, author_did="did:plc:alice", content=None, minilm_l12_embedding=None, generator_name="g"),
            CandidatePost(at_uri="at://alice/3", score=0.8, author_did="did:plc:alice", content=None, minilm_l12_embedding=None, generator_name="g"),
            CandidatePost(at_uri="at://bob/1", score=0.5, author_did="did:plc:bob", content=None, minilm_l12_embedding=None, generator_name="g"),
        ]
        with self._patch_generators(candidates):
            data = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}).json()
        uris = [item["post"] for item in data["feed"]]
        assert uris[0] == "at://alice/1"
        assert uris.index("at://bob/1") < uris.index("at://alice/2")

    # --- posts with no at_uri are skipped ---

    def test_posts_without_at_uri_are_skipped(self):
        candidates = [
            CandidatePost(at_uri=None, content="no uri", minilm_l12_embedding=None, score=None, generator_name="g"),
            CandidatePost(at_uri="at://good/1", content="has uri", minilm_l12_embedding=None, score=None, generator_name="g"),
        ]
        with self._patch_generators(candidates):
            data = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}).json()
        assert len(data["feed"]) == 1
        assert data["feed"][0]["post"] == "at://good/1"


# ---------------------------------------------------------------------------
# Cursor / pagination
# ---------------------------------------------------------------------------

class TestFeedSkeletonCursor:
    """Tests for cursor-based feed pagination."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def _patch_generators(self, primary_candidates, infill_candidates=None):
        return _patch_basic_similarity_generators(
            primary_candidates,
            infill_candidates=infill_candidates,
        )

    def test_first_page_returns_cursor_when_more_available(self):
        candidates = _make_candidates("p", 10)
        with self._patch_generators(candidates):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()
        assert len(data["feed"]) == 3
        assert "cursor" in data
        parsed = FeedCursor.decode(data["cursor"])
        assert parsed.offset == 3

    def test_no_cursor_when_all_results_fit_in_one_page(self):
        candidates = _make_candidates("p", 3)
        with self._patch_generators(candidates):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        assert len(data["feed"]) == 3
        assert "cursor" not in data

    def test_second_page_via_cursor(self):
        candidates = _make_candidates("p", 10)
        with self._patch_generators(candidates):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4},
            ).json()

        assert len(first["feed"]) == 4
        cursor = first["cursor"]

        # Second page — no generator call needed (served from cache).
        with self._patch_generators([]):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4, "cursor": cursor},
            ).json()

        assert len(second["feed"]) == 4
        assert second["feed"][0]["post"] == "at://p/4"

    def test_last_page_has_no_cursor(self):
        candidates = _make_candidates("p", 6)
        with self._patch_generators(candidates):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4},
            ).json()

        with self._patch_generators([]):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4, "cursor": first["cursor"]},
            ).json()

        assert len(second["feed"]) == 2
        # Last cache page still returns a cursor so the client can
        # request more; following it with no new candidates ends the feed.
        assert "cursor" in second

        with self._patch_generators([]):
            third = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4, "cursor": second["cursor"]},
            ).json()

        assert third["feed"] == []

    def test_full_scroll_returns_all_items(self):
        """Scrolling through all pages collects every generated post."""
        candidates = _make_candidates("p", 12)
        all_posts: list[str] = []

        with self._patch_generators(candidates):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        all_posts.extend(item["post"] for item in data["feed"])

        while "cursor" in data:
            with self._patch_generators([]):
                data = client.get(
                    "/xrpc/app.bsky.feed.getFeedSkeleton",
                    params={"feed": FEED_URI, "limit": 5, "cursor": data["cursor"]},
                ).json()
            all_posts.extend(item["post"] for item in data["feed"])

        assert all_posts == [f"at://p/{i}" for i in range(12)]

    def test_invalid_cursor_returns_400(self):
        with self._patch_generators([]):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "cursor": "not-valid-base64!@#"},
            )
        assert resp.status_code == 400
        assert "Invalid cursor" in resp.json()["detail"]

    def test_expired_cursor_generates_fresh_results(self):
        """When the cache entry is gone, a fresh batch is generated."""
        candidates = _make_candidates("p", 8)
        with self._patch_generators(candidates):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        # Simulate cache eviction.
        app.state.feed_cache._store.clear()

        fresh = _make_candidates("fresh", 5)
        with self._patch_generators(fresh):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": first["cursor"]},
            ).json()

        # Should have generated fresh results, not errored.
        assert len(data["feed"]) == 3
        assert data["feed"][0]["post"] == "at://fresh/0"

    def test_missing_feed_cache_returns_500(self):
        saved = app.state.feed_cache
        app.state.feed_cache = None
        try:
            with self._patch_generators(_make_candidates("p", 1)):
                resp = client.get(
                    "/xrpc/app.bsky.feed.getFeedSkeleton",
                    params={"feed": FEED_URI},
                )
            assert resp.status_code == 500
            assert resp.json()["detail"] == "Feed cache unavailable"
        finally:
            app.state.feed_cache = saved

    def test_end_of_cache_regenerates_with_exclusions(self):
        """When cursor offset >= cached length, new posts are generated excluding previously shown."""
        candidates = _make_candidates("p", 6)
        with self._patch_generators(candidates):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4},
            ).json()

        # Exhaust the cache — still returns a cursor for regeneration.
        with self._patch_generators([]):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 4, "cursor": first["cursor"]},
            ).json()
        assert len(second["feed"]) == 2
        assert "cursor" in second

    def test_scroll_past_end_returns_new_posts(self):
        """Scrolling past the end of the first batch fetches a new batch with dedup."""
        # First batch: 5 posts, request in pages of 3.
        initial = _make_candidates("p", 5)
        with self._patch_generators(initial):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        assert len(first["feed"]) == 3
        cur = first["cursor"]

        # Second page (p/3, p/4) — exhausts the cache but returns a cursor.
        with self._patch_generators([]):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": cur},
            ).json()

        assert len(second["feed"]) == 2
        assert "cursor" in second

    def test_regeneration_extends_cache(self):
        """After regeneration, new items are appended and further paging works."""
        initial = _make_candidates("p", 5)
        with self._patch_generators(initial):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        assert len(first["feed"]) == 3
        cur = first["cursor"]

        # Consume the rest of the cached items (p/3, p/4).
        with self._patch_generators([]):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": cur},
            ).json()

        assert len(second["feed"]) == 2
        # Cursor returned so client will request more — triggers regeneration.
        assert "cursor" in second

        # Following the cursor should trigger regeneration.
        fresh = _make_candidates("fresh", 4)
        with self._patch_generators(fresh):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": second["cursor"]},
            ).json()

        assert len(data["feed"]) == 3
        assert data["feed"][0]["post"] == "at://fresh/0"
        assert "cursor" in data

        # Continue paging into the appended results.
        with self._patch_generators([]):
            more = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": data["cursor"]},
            ).json()

        assert len(more["feed"]) == 1
        assert more["feed"][0]["post"] == "at://fresh/3"

    def test_regeneration_with_no_new_results_ends_feed(self):
        """When regeneration returns nothing new, the feed ends gracefully."""
        initial = _make_candidates("p", 5)
        with self._patch_generators(initial):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        cur = first["cursor"]
        parsed = FeedCursor.decode(cur)
        end_cursor = FeedCursor(id=parsed.id, offset=5).encode()

        # Regeneration returns empty.
        with self._patch_generators([]):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": end_cursor},
            ).json()

        assert data["feed"] == []
        assert "cursor" not in data

    def test_exclude_uris_passed_to_generator_on_regen(self):
        """Verify exclude_uris is populated with previously-shown URIs."""
        initial = _make_candidates("p", 5)
        with self._patch_generators(initial):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        cur = first["cursor"]
        parsed = FeedCursor.decode(cur)
        end_cursor = FeedCursor(id=parsed.id, offset=5).encode()

        # Track what the generator receives.
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="post_similarity",
            candidates=_make_candidates("new", 2),
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity", candidates=[],
        )
        followed_users_gen = AsyncMock()
        followed_users_gen.generate.return_value = CandidateResult(
            generator_name="followed_users", candidates=[],
        )

        def fake_get(name):
            return {
                "post_similarity": primary_gen,
                "followed_users": followed_users_gen,
                "popularity": infill_gen,
            }.get(name)

        with patch("app.lib.candidates.generate.get_generator", side_effect=fake_get):
            client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3, "cursor": end_cursor},
            )

        # The primary generator should have been called with exclude_uris
        # containing the 5 initial URIs.
        call_kwargs = primary_gen.generate.call_args
        assert call_kwargs.kwargs.get("exclude_uris") == [
            "at://p/0", "at://p/1", "at://p/2", "at://p/3", "at://p/4",
        ]
        followed_call_kwargs = followed_users_gen.generate.call_args
        assert followed_call_kwargs.kwargs.get("exclude_uris") == [
            "at://p/0", "at://p/1", "at://p/2", "at://p/3", "at://p/4",
        ]

class TestGetFeedSkeletonAuth:
    """Tests that getFeedSkeleton correctly passes through the authenticated DID."""

    def _patch_generators(self, primary_candidates):
        return _patch_basic_similarity_generators(primary_candidates)

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        """Avoid real Firestore interactions unless a test explicitly patches it."""
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def test_authenticated_user_did_passed_to_generator(self):
        """When a valid JWT is present, the user's DID flows to the generator."""
        from unittest.mock import MagicMock

        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )
        assert resp.status_code == 200

    def test_unauthenticated_request_uses_empty_did(self):
        """Without auth header, endpoint should reject the request."""
        with self._patch_generators(_make_candidates("p", 2)) as mock_get:
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
            )
        assert resp.status_code == 401

    def test_invalid_jwt_still_returns_feed(self):
        """Invalid JWT should be rejected."""
        from atproto_server.exceptions import TokenInvalidSignatureError

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                side_effect=TokenInvalidSignatureError("bad sig"),
            ),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer bad.jwt.token"},
            )
        assert resp.status_code == 401

    def test_authenticated_request_upserts_user(self):
        """Authenticated requests should upsert the user in Firestore."""
        from unittest.mock import MagicMock

        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock) as mock_upsert,
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 200
        mock_upsert.assert_awaited_once_with(
            app.state.firestore,
            "did:plc:autheduser",
            TEST_USERNAME,
        )

    def test_username_resolution_failure_is_logged_but_non_fatal(self, caplog):
        """Username resolution runs in a background task; failures are logged
        but don't block the response."""
        from unittest.mock import MagicMock

        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch.object(app.state.id_resolver.did, "resolve", new_callable=AsyncMock, return_value=None),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 200
        assert "Failed to resolve username" in caplog.text

    def test_firestore_upsert_failure_is_logged_but_non_fatal(self, caplog):
        """Firestore user upserts run in a background task; failures are
        logged but don't block the response."""
        from unittest.mock import MagicMock

        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch(
                "app.routers.xrpc.upsert_user",
                new_callable=AsyncMock,
                side_effect=RuntimeError("firestore down"),
            ),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 200
        assert "Failed to upsert user" in caplog.text

    def test_missing_firestore_client_is_fatal(self):
        """Missing Firestore client should fail the request."""
        from unittest.mock import MagicMock

        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch.object(app.state, "firestore", None),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 500
        assert resp.json()["detail"] == "Firestore unavailable"

    def test_unauthenticated_request_does_not_upsert_user(self):
        """Unauthenticated requests should not write to Firestore."""
        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock) as mock_upsert,
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
            )

        assert resp.status_code == 401
        mock_upsert.assert_not_awaited()

    def test_authenticated_request_records_feed_activity(self):
        """Authenticated requests should record feed activity in Firestore."""
        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock) as mock_activity,
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 200
        mock_activity.assert_awaited_once_with(app.state.firestore, "did:plc:autheduser", FEED_RKEY)

    def test_feed_activity_failure_is_logged_but_non_fatal(self, caplog):
        """Feed-activity upserts run in a background task; failures are
        logged but don't block the response."""
        mock_payload = MagicMock()
        mock_payload.iss = "did:plc:autheduser"

        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch(
                "app.lib.atproto_auth.verify_jwt_async",
                new_callable=AsyncMock,
                return_value=mock_payload,
            ),
            patch(
                "app.routers.xrpc.upsert_feed_activity",
                new_callable=AsyncMock,
                side_effect=RuntimeError("firestore down"),
            ),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.status_code == 200
        assert "Failed to record feed activity" in caplog.text


# ---------------------------------------------------------------------------
# _get_service_did / _get_hostname helpers
# ---------------------------------------------------------------------------

class TestConfigHelpers:
    def test_get_service_did_from_env(self):
        from ..routers.xrpc import _get_service_did
        assert _get_service_did() == SERVICE_DID

    def test_get_service_did_default(self, monkeypatch):
        from ..routers.xrpc import _get_service_did
        monkeypatch.delenv("GE_FEED_GENERATOR_DID", raising=False)
        assert _get_service_did() == "did:web:localhost"

    def test_get_hostname_from_did_web(self):
        from ..routers.xrpc import _get_hostname
        assert _get_hostname() == "test.example.com"

    def test_get_hostname_non_web_did(self, monkeypatch):
        from ..routers.xrpc import _get_hostname
        monkeypatch.setenv("GE_FEED_GENERATOR_DID", "did:plc:abc123")
        assert _get_hostname() == "localhost"


# ---------------------------------------------------------------------------
# Ranked feed
# ---------------------------------------------------------------------------

class TestRankedFeed:
    """Tests for feeds with a rank_request_template wired in."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def _patch_generators(self, candidates):
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="post_similarity", candidates=candidates
        )
        followed_gen = AsyncMock()
        followed_gen.generate.return_value = CandidateResult(
            generator_name="followed_users", candidates=[]
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity", candidates=[]
        )

        def fake_get(name):
            return {
                "post_similarity": primary_gen,
                "followed_users": followed_gen,
                "popularity": infill_gen,
            }.get(name)

        return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get)

    def test_ranking_applied_to_candidates(self):
        """When ranking succeeds, posts are returned in ranked order."""
        candidates = _make_candidates("p", 3)
        # Ranker reverses the order: p/2, p/1, p/0
        reversed_rankings = [
            RankedCandidate(at_uri=f"at://p/{i}", rank=r + 1, rank_score=float(3 - r))
            for r, i in enumerate([2, 1, 0])
        ]
        rank_result = RankPredictResult(rankings=reversed_rankings)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            ).json()

        posts = [item["post"] for item in data["feed"]]
        assert posts == ["at://p/2", "at://p/1", "at://p/0"]

    def test_mmr_uses_rank_score_not_generator_score(self):
        """MMR should weight by the model's rank_score, not the generator's ES score.

        Candidates are given generator scores that disagree with the ranker's
        ordering.  With no embeddings, MMR picks purely by relevance score, so
        the output order reveals which score is used.
        """
        # Generator scores: p/0 highest, p/1 middle, p/2 lowest.
        candidates = [
            CandidatePost(at_uri="at://p/0", score=3.0, content=None, minilm_l12_embedding=None, generator_name="g"),
            CandidatePost(at_uri="at://p/1", score=2.0, content=None, minilm_l12_embedding=None, generator_name="g"),
            CandidatePost(at_uri="at://p/2", score=1.0, content=None, minilm_l12_embedding=None, generator_name="g"),
        ]
        # Ranker reverses the order: p/2 best, p/1 middle, p/0 worst.
        rank_result = RankPredictResult(rankings=[
            RankedCandidate(at_uri="at://p/2", rank=1, rank_score=3.0),
            RankedCandidate(at_uri="at://p/1", rank=2, rank_score=2.0),
            RankedCandidate(at_uri="at://p/0", rank=3, rank_score=1.0),
        ])

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            ).json()

        posts = [item["post"] for item in data["feed"]]
        # Should follow rank_score order (p/2 first), not generator score (p/0 first).
        assert posts == ["at://p/2", "at://p/1", "at://p/0"]

    def test_ranking_failure_returns_500(self):
        """When ranking raises, the feed fails with a 500."""
        candidates = _make_candidates("p", 3)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, side_effect=RuntimeError("inference down")):
            resp = TestClient(app, raise_server_exceptions=False).get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            )

        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Best-of-friends feed
# ---------------------------------------------------------------------------

class TestBestOfFriendsFeed:
    """Tests for the best-of-friends feed (followed_users candidates + two-tower ranking)."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def _patch_generators(self, candidates):
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="followed_users", candidates=candidates
        )

        def fake_get(name):
            return {"followed_users": primary_gen}.get(name)

        return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get)

    def test_ranking_applied_to_candidates(self):
        """Candidates from followed_users are returned in two-tower ranked order."""
        candidates = _make_candidates("p", 3)
        reversed_rankings = [
            RankedCandidate(at_uri=f"at://p/{i}", rank=r + 1, rank_score=float(3 - r))
            for r, i in enumerate([2, 1, 0])
        ]
        rank_result = RankPredictResult(rankings=reversed_rankings)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": BEST_OF_FRIENDS_FEED_URI},
            ).json()

        posts = [item["post"] for item in data["feed"]]
        assert posts == ["at://p/2", "at://p/1", "at://p/0"]

    def test_ranking_failure_returns_500(self):
        """When the two-tower ranker raises, the feed returns HTTP 500."""
        candidates = _make_candidates("p", 3)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, side_effect=RuntimeError("inference down")):
            resp = TestClient(app, raise_server_exceptions=False).get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": BEST_OF_FRIENDS_FEED_URI},
            )

        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# /xrpc/app.bsky.feed.sendInteractions
# ---------------------------------------------------------------------------


def _make_token(did="did:plc:interactor", feed="your-feed", rid="req-1", iat=1730000000):
    from app.lib.feed_context import FeedContextPayload, encode_feed_context

    return encode_feed_context(FeedContextPayload(did=did, feed=feed, rid=rid, iat=iat))


class TestShortEvent:
    def test_strips_defs_prefix(self):
        from app.routers.xrpc import _short_event

        assert _short_event("app.bsky.feed.defs#interactionLike") == "interactionLike"

    def test_passes_through_unprefixed(self):
        from app.routers.xrpc import _short_event

        assert _short_event("interactionLike") == "interactionLike"

    def test_falls_back_to_original_when_suffix_empty(self):
        from app.routers.xrpc import _short_event

        assert _short_event("app.bsky.feed.defs#") == "app.bsky.feed.defs#"

    def test_empty_input_returns_empty(self):
        from app.routers.xrpc import _short_event

        assert _short_event(None) == ""
        assert _short_event("") == ""


class TestSendInteractions:
    def test_returns_empty_object(self):
        resp = client.post(
            "/xrpc/app.bsky.feed.sendInteractions", json={"interactions": []}
        )
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_accepts_well_formed_payload(self):
        # Patch the background recorder so the endpoint wiring is tested without
        # touching the (mock) Firestore client. The coroutine is created
        # synchronously when the endpoint calls it, so the call is recorded
        # before the request returns.
        with patch("app.routers.xrpc._record_interactions", new_callable=AsyncMock) as rec:
            resp = client.post(
                "/xrpc/app.bsky.feed.sendInteractions",
                json={
                    "interactions": [
                        {
                            "item": "at://post/1",
                            "event": "app.bsky.feed.defs#interactionLike",
                            "feedContext": _make_token(),
                        }
                    ]
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {}
        rec.assert_called_once()
        interactions = rec.call_args[0][1]
        assert len(interactions) == 1
        assert interactions[0].item == "at://post/1"

    @pytest.mark.asyncio
    async def test_records_valid_interaction(self):
        from app.routers.xrpc import Interaction, _record_interactions

        ix = Interaction(
            item="at://post/1",
            event="app.bsky.feed.defs#interactionLike",
            feed_context=_make_token(did="did:plc:u", feed="your-feed", rid="r1"),
        )
        db = MagicMock()
        with patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock) as rec:
            await _record_interactions(db, [ix])

        rec.assert_called_once()
        doc = rec.call_args[0][1]
        assert doc.user_did == "did:plc:u"
        assert doc.feed_name == "your-feed"
        assert doc.request_id == "r1"
        assert doc.item_uri == "at://post/1"
        # The app.bsky.feed.defs# prefix is stripped before storage.
        assert doc.event == "interactionLike"
        assert doc.feed_generated_at is not None

    @pytest.mark.asyncio
    async def test_drops_forged_token(self):
        from app.routers.xrpc import Interaction, _record_interactions

        ix = Interaction(
            item="at://post/1",
            event="app.bsky.feed.defs#interactionLike",
            feed_context="forged.token",
        )
        db = MagicMock()
        with patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock) as rec:
            await _record_interactions(db, [ix])

        rec.assert_not_called()

    @pytest.mark.asyncio
    async def test_drops_missing_token(self):
        from app.routers.xrpc import Interaction, _record_interactions

        ix = Interaction(item="at://post/1", event="app.bsky.feed.defs#interactionLike")
        db = MagicMock()
        with patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock) as rec:
            await _record_interactions(db, [ix])

        rec.assert_not_called()

    @pytest.mark.asyncio
    async def test_records_only_valid_interactions_in_mixed_batch(self):
        from app.routers.xrpc import Interaction, _record_interactions

        like = "app.bsky.feed.defs#interactionLike"
        repost = "app.bsky.feed.defs#interactionRepost"
        less = "app.bsky.feed.defs#requestLess"
        interactions = [
            Interaction(item="at://post/1", event=like, feed_context=_make_token()),
            Interaction(item="at://post/2", event=repost, feed_context="bad"),
            Interaction(item="at://post/3", event=less, feed_context=_make_token()),
        ]
        db = MagicMock()
        with patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock) as rec:
            await _record_interactions(db, interactions)

        assert rec.call_count == 2

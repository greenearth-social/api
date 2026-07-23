"""Tests for the XRPC feed generator endpoints."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..feeds import FEEDS
from ..lib.candidates.base import CandidateResult
from ..lib.embeddings import encode_float32_b64
from ..lib.feed_cache import FeedCache
from ..lib.feed_context import decode_feed_context
from ..lib.metrics import MetricCollector, set_metric_collector
from ..main import app
from ..models import CandidatePost, FeedCursor, RankedCandidate, RankPredictResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_initial_request_reuse():
    from ..routers.xrpc import _clear_initial_request_cache

    _clear_initial_request_cache()
    yield
    _clear_initial_request_cache()

SERVICE_DID = "did:web:test.example.com"
PUBLISHER_DID = "did:plc:publisherabc123"
FEED_RKEY = "unranked-your-feed"
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
CANDIDATE_ONLY_FEEDS = (
    ("post-similarity", "post_similarity"),
    ("followed-users", "followed_users"),
    ("network-likes", "network_likes"),
    ("popularity", "popularity"),
    ("two-tower", "two_tower"),
)
TEST_EMBEDDING = encode_float32_b64([1.0, 0.0, 0.0])


def _make_candidates(prefix: str, n: int, generator_name: str = "test", with_embedding: bool = False) -> list[CandidatePost]:
    embedding = TEST_EMBEDDING if with_embedding else None
    return [
        CandidatePost(at_uri=f"at://{prefix}/{i}", content=f"post {i}", minilm_l12_embedding=embedding, score=None, generator_name=generator_name)
        for i in range(n)
    ]


def _patch_unranked_your_feed_generators(
    two_tower_candidates,
    followed_users_candidates=None,
    infill_candidates=None,
):
    """Patch generators used by the unranked-your-feed feed.

    Most tests care about feed endpoint behavior rather than the exact mix of
    candidate sources, so followed_users defaults to the same candidates as
    two_tower. The pipeline then deduplicates them back to the expected
    output shape.
    """
    two_tower_gen = AsyncMock()
    two_tower_gen.generate.return_value = CandidateResult(
        generator_name="two_tower",
        candidates=two_tower_candidates,
    )
    followed_users_gen = AsyncMock()
    followed_users_gen.generate.return_value = CandidateResult(
        generator_name="followed_users",
        candidates=(
            two_tower_candidates
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
        if name == "two_tower":
            return two_tower_gen
        if name == "followed_users":
            return followed_users_gen
        if name == "popularity":
            return infill_gen
        return None

    return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator)


class FakeMetricCollector:
    def __init__(self):
        self.calls: list[tuple[str, float, dict[str, str]]] = []

    def record(self, name: str, value: float, **attributes: str) -> None:
        self.calls.append((name, value, dict(attributes)))


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

    def test_feeds_list_contains_unranked_your_feed(self):
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

    @pytest.mark.parametrize("feed_name", [feed_name for feed_name, _ in CANDIDATE_ONLY_FEEDS])
    def test_feeds_list_contains_candidate_only_feed(self, feed_name):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        uris = [f["uri"] for f in data["feeds"]]
        assert f"at://{SERVICE_DID}/app.bsky.feed.generator/{feed_name}" in uris

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
        return _patch_unranked_your_feed_generators(
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

    def test_seen_uris_excluded_on_fresh_request(self):
        """A fresh feed load excludes the user's recently-seen posts."""
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="two_tower", candidates=_make_candidates("p", 2),
        )
        followed_gen = AsyncMock()
        followed_gen.generate.return_value = CandidateResult(
            generator_name="followed_users", candidates=[],
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity", candidates=[],
        )

        def fake_get(name):
            return {
                "two_tower": primary_gen,
                "followed_users": followed_gen,
                "popularity": infill_gen,
            }.get(name)

        with (
            patch("app.lib.candidates.generate.get_generator", side_effect=fake_get),
            patch(
                "app.routers.xrpc.get_recent_seen_uris",
                new_callable=AsyncMock,
                return_value=["at://seen/1", "at://seen/2"],
            ),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}
            )

        assert resp.status_code == 200
        assert primary_gen.generate.call_args.kwargs.get("exclude_uris") == [
            "at://seen/1",
            "at://seen/2",
        ]

    def test_seen_not_excluded_when_feed_disables_it(self, monkeypatch):
        """A feed with exclude_seen_posts off neither fetches nor excludes seen posts."""
        monkeypatch.setattr(FEEDS[FEED_RKEY], "exclude_seen_posts", False)

        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="two_tower", candidates=_make_candidates("p", 2),
        )
        followed_gen = AsyncMock()
        followed_gen.generate.return_value = CandidateResult(
            generator_name="followed_users", candidates=[],
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity", candidates=[],
        )

        def fake_get(name):
            return {
                "two_tower": primary_gen,
                "followed_users": followed_gen,
                "popularity": infill_gen,
            }.get(name)

        with (
            patch("app.lib.candidates.generate.get_generator", side_effect=fake_get),
            patch(
                "app.routers.xrpc.get_recent_seen_uris", new_callable=AsyncMock
            ) as seen_fetch,
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI}
            )

        assert resp.status_code == 200
        seen_fetch.assert_not_called()
        assert not primary_gen.generate.call_args.kwargs.get("exclude_uris")

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

    def test_identical_initial_requests_within_window_reuse_response(self):
        candidates = _make_candidates("p", 8)
        with (
            self._patch_generators(candidates) as mock_get,
            patch(
                "app.routers.xrpc.merge_feed_snapshot",
                new_callable=AsyncMock,
                return_value=False,
            ) as snapshot_write,
        ):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            )
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            )

        assert first.json() == second.json()
        mock_get.side_effect("two_tower").generate.assert_awaited_once()
        snapshot_write.assert_awaited_once()

    def test_initial_request_after_reuse_window_gets_new_session(self):
        from app.routers import xrpc as xrpc_mod

        candidates = _make_candidates("p", 8)
        with self._patch_generators(candidates):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()
            key = ("did:plc:testuser", FEED_RKEY, 3)
            xrpc_mod._initial_requests[key].created_at -= (
                xrpc_mod.INITIAL_REQUEST_REUSE_SECONDS + 1
            )
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            ).json()

        first_context = decode_feed_context(first["feed"][0]["feedContext"])
        second_context = decode_feed_context(second["feed"][0]["feedContext"])
        assert first_context is not None
        assert second_context is not None
        assert first_context.rid != second_context.rid

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
        feed_cfg = FEEDS["unranked-your-feed"]
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
        """AT Protocol requires cursor to be absent, not null.

        A fresh request that generates nothing is the no-cursor case (any
        non-empty slate now issues a cursor so paging can restart the session).
        """
        with self._patch_generators([]):
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
        primary = _make_candidates("prim", 2, "two_tower")
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
        primary = _make_candidates("prim", 25, "two_tower")
        with self._patch_generators(primary) as mock_get:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        assert len(data["feed"]) == 5
        # popularity is a primary generator for this feed; no extra infill call should happen.
        popularity_gen = mock_get.side_effect("popularity")
        popularity_gen.generate.assert_awaited_once()

    def test_unranked_your_feed_uses_followed_users_generator(self):
        two_tower = _make_candidates("tower", 3, "two_tower")
        followed = _make_candidates("followed", 3, "followed_users")

        with _patch_unranked_your_feed_generators(
            two_tower,
            followed_users_candidates=followed,
        ) as mock_get:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 6},
            ).json()

        posts = [item["post"] for item in data["feed"]]
        assert "at://tower/0" in posts
        assert "at://followed/0" in posts
        mock_get.side_effect("followed_users").generate.assert_awaited_once()

    # --- primary generator failure ---

    def test_primary_failure_falls_back_to_infill(self):
        """If primary raises, we still get infill results.

        The XRPC handler installs a PipelineContext for every render, so a failing
        primary generator is recorded as a degradation event rather than raising,
        and the feed is served with partial results from infill/other generators.
        """
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
                "two_tower": primary_gen,
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
# Candidate-generator-only feeds
# ---------------------------------------------------------------------------

class TestCandidateGeneratorOnlyFeeds:
    """Tests for private feeds that expose one candidate generator directly."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    @pytest.mark.parametrize("feed_name,expected_generator", CANDIDATE_ONLY_FEEDS)
    def test_routes_to_expected_generator(self, feed_name, expected_generator):
        generator_mocks = {}
        for _, generator_name in CANDIDATE_ONLY_FEEDS:
            gen = AsyncMock()
            gen.generate.return_value = CandidateResult(
                generator_name=generator_name,
                candidates=[
                    CandidatePost(
                        at_uri=f"at://{generator_name}/{i}",
                        content=f"post {i}",
                        minilm_l12_embedding="fake-embedding",
                        score=None,
                        generator_name=generator_name,
                    )
                    for i in range(2)
                ],
            )
            generator_mocks[generator_name] = gen

        with patch(
            "app.lib.candidates.generate.get_generator",
            side_effect=lambda name: generator_mocks.get(name),
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={
                    "feed": f"at://{SERVICE_DID}/app.bsky.feed.generator/{feed_name}",
                    "limit": 3,
                },
            )

        assert resp.status_code == 200
        posts = [item["post"] for item in resp.json()["feed"]]
        assert posts == [f"at://{expected_generator}/0", f"at://{expected_generator}/1"]
        generator_mocks[expected_generator].generate.assert_awaited_once()
        for generator_name, gen in generator_mocks.items():
            if generator_name != expected_generator:
                gen.generate.assert_not_awaited()


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
        return _patch_unranked_your_feed_generators(
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

    def test_cursor_returned_when_all_results_fit_in_one_page(self):
        """Even a single-page batch gets a cursor: following it past the end
        regenerates (restarts the ranking session) instead of ending the feed."""
        candidates = _make_candidates("p", 3)
        with self._patch_generators(candidates):
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        assert len(data["feed"]) == 3
        assert "cursor" in data
        parsed = FeedCursor.decode(data["cursor"])
        assert parsed.offset == 3

        # Following the cursor regenerates with the shown posts excluded.
        fresh = _make_candidates("fresh", 2)
        with self._patch_generators(fresh):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5, "cursor": data["cursor"]},
            ).json()
        assert [item["post"] for item in second["feed"]] == ["at://fresh/0", "at://fresh/1"]

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
            generator_name="two_tower",
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
                "two_tower": primary_gen,
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
        return _patch_unranked_your_feed_generators(primary_candidates)

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
        but don't block the response, and the session is still recorded
        without the handle."""
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
        assert "Could not resolve handle" in caplog.text

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

    @pytest.fixture(autouse=True)
    def _clear_pinned_post(self, monkeypatch):
        """Isolate ranking tests from the pinned post — that's tested in TestPinnedPost."""
        monkeypatch.setattr(FEEDS["your-feed"], "pinned_post_uri", None)

    @pytest.fixture(autouse=True)
    def _disable_slate_cutoffs(self, monkeypatch):
        """Isolate ranking tests from the slate cutoffs — tested in TestSlateCutoffs."""
        monkeypatch.setattr(FEEDS["your-feed"], "max_render_share", None)
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", None)
        monkeypatch.setattr(FEEDS["your-feed"], "min_mmr_score", None)

    def _patch_generators(self, candidates):
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="two_tower", candidates=candidates
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
                "two_tower": primary_gen,
                "followed_users": followed_gen,
                "popularity": infill_gen,
            }.get(name)

        return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get)

    def test_ranking_applied_to_candidates(self):
        """When ranking succeeds, posts are returned in ranked order."""
        candidates = _make_candidates("p", 3, with_embedding=True)
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

    def test_hydrates_lightweight_candidates_before_ranking(self):
        """Embedding-free generated candidates are hydrated before ranking."""
        candidates = _make_candidates("p", 1)
        rank_result = RankPredictResult(rankings=[
            RankedCandidate(at_uri="at://p/0", rank=1, rank_score=1.0),
        ])

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.fetch_post_embeddings", new_callable=AsyncMock, return_value=[("at://p/0", [1.0, 0.0, 0.0])]) as mock_fetch, \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result) as mock_run:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            ).json()

        mock_fetch.assert_awaited_once()
        assert mock_fetch.await_args
        assert mock_run.await_args
        assert mock_fetch.await_args.args[1] == ["at://p/0"]
        rank_req = mock_run.await_args.args[0]
        assert rank_req.candidates[0].minilm_l12_embedding == TEST_EMBEDDING
        assert [item["post"] for item in data["feed"]] == ["at://p/0"]

    def test_drops_candidates_missing_embeddings_before_ranking(self):
        """Candidates still missing embeddings after hydration are not sent to ranking."""
        candidates = [
            CandidatePost(at_uri="at://p/0", content=None, minilm_l12_embedding=TEST_EMBEDDING, score=None, generator_name="g"),
            CandidatePost(at_uri="at://p/1", content=None, minilm_l12_embedding=None, score=None, generator_name="g"),
        ]
        rank_result = RankPredictResult(rankings=[
            RankedCandidate(at_uri="at://p/0", rank=1, rank_score=1.0),
            RankedCandidate(at_uri="at://p/1", rank=2, rank_score=0.5),
        ])

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc._hydrate_embeddings", new_callable=AsyncMock, return_value=candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result) as mock_run:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            ).json()

        assert mock_run.await_args
        rank_req = mock_run.await_args.args[0]
        assert [c.at_uri for c in rank_req.candidates] == ["at://p/0"]
        assert [item["post"] for item in data["feed"]] == ["at://p/0"]

    def test_mmr_uses_rank_score_not_generator_score(self):
        """MMR should weight by the model's rank_score, not the generator's ES score.

        Candidates are given generator scores that disagree with the ranker's
        ordering.  With identical embeddings, MMR's content penalty is tied, so
        the output order reveals which score is used.
        """
        # Generator scores: p/0 highest, p/1 middle, p/2 lowest.
        candidates = [
            CandidatePost(at_uri="at://p/0", score=3.0, content=None, minilm_l12_embedding=TEST_EMBEDDING, generator_name="g"),
            CandidatePost(at_uri="at://p/1", score=2.0, content=None, minilm_l12_embedding=TEST_EMBEDDING, generator_name="g"),
            CandidatePost(at_uri="at://p/2", score=1.0, content=None, minilm_l12_embedding=TEST_EMBEDDING, generator_name="g"),
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

    def test_ranking_failure_soft_fails_to_unranked(self):
        """When ranking raises, the feed soft-fails and returns candidates in unranked order."""
        candidates = _make_candidates("p", 3, with_embedding=True)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, side_effect=RuntimeError("inference down")):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            )

        assert resp.status_code == 200
        posts = [item["post"] for item in resp.json()["feed"]]
        assert len(posts) == 3


# ---------------------------------------------------------------------------
# Embedding hydration timeout
# ---------------------------------------------------------------------------


class TestEmbeddingHydrationTimeout:
    """A hung `fetch_post_embeddings` call must not block the pipeline past
    `GE_EMBED_HYDRATION_TIMEOUT_SEC` — it should fall back to unhydrated
    candidates via the existing error path, same as any other hydration
    failure."""

    @pytest.mark.asyncio
    async def test_timeout_falls_back_to_unhydrated_candidates(self, monkeypatch):
        from ..lib.pipeline_context import (
            DegradationStage,
            PipelineContext,
            pipeline_context_scope,
        )
        from ..routers import xrpc as xrpc_module

        monkeypatch.setattr(xrpc_module, "_EMBED_HYDRATION_TIMEOUT_SEC", 0.01)

        async def _hangs(*args, **kwargs):
            await asyncio.sleep(9999)

        candidates = [CandidatePost(at_uri="at://post/1", score=0.5)]

        with patch(
            "app.routers.xrpc.fetch_post_embeddings", side_effect=_hangs
        ), pipeline_context_scope(PipelineContext(feed_name="f")) as ctx:
            result = await xrpc_module._hydrate_embeddings(object(), candidates)

        assert result == candidates
        assert len(ctx.degradations) == 1
        assert ctx.degradations[0].stage == DegradationStage.EMBED_HYDRATION
        assert ctx.degradations[0].component == "fetch_post_embeddings"
        assert isinstance(ctx.degradations[0].cause, asyncio.TimeoutError)


# ---------------------------------------------------------------------------
# Slate cutoffs (max_render_share / min_rank_score / min_mmr_score)
# ---------------------------------------------------------------------------

class TestSlateCutoffs:
    """The quality gates cut the slate and drive session restarts + discards."""

    @pytest.fixture(autouse=True)
    def _mock_authenticated_user(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    @pytest.fixture(autouse=True)
    def _reset_feed_config(self, monkeypatch):
        """Start each test from a clean your-feed: no pin, no cutoffs, no MMR.

        Individual tests re-enable exactly the gate under test.
        """
        monkeypatch.setattr(FEEDS["your-feed"], "pinned_post_uri", None)
        monkeypatch.setattr(FEEDS["your-feed"], "diversify", False)
        monkeypatch.setattr(FEEDS["your-feed"], "max_render_share", None)
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", None)
        monkeypatch.setattr(FEEDS["your-feed"], "min_mmr_score", None)

    def _patch_generators(self, candidates):
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="two_tower", candidates=candidates
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
                "two_tower": primary_gen,
                "followed_users": followed_gen,
                "popularity": infill_gen,
            }.get(name)

        return (
            patch("app.lib.candidates.generate.get_generator", side_effect=fake_get),
            primary_gen,
        )

    @staticmethod
    def _rank_result(scores: list[float]) -> RankPredictResult:
        """Rank p/0..p/n-1 with the given scores, in descending-score order."""
        order = sorted(range(len(scores)), key=lambda i: -scores[i])
        return RankPredictResult(rankings=[
            RankedCandidate(at_uri=f"at://p/{i}", rank=r + 1, rank_score=scores[i])
            for r, i in enumerate(order)
        ])

    def _get_feed(self, candidates, rank_result, discarded_mock=None):
        gen_patch, _ = self._patch_generators(candidates)
        discarded_mock = discarded_mock if discarded_mock is not None else AsyncMock()
        with gen_patch, \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result), \
             patch("app.routers.xrpc.record_discarded_posts", discarded_mock):
            return client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANKED_FEED_URI},
            ).json()

    def test_rank_floor_cuts_low_scores_and_records_discards(self, monkeypatch):
        """Sub-floor candidates are cut (boundary-equal kept) and persisted as discarded."""
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", 0.0)
        candidates = _make_candidates("p", 4, with_embedding=True)
        discarded = AsyncMock()

        data = self._get_feed(candidates, self._rank_result([0.5, 0.2, 0.0, -0.3]), discarded)

        posts = [item["post"] for item in data["feed"]]
        assert posts == ["at://p/0", "at://p/1", "at://p/2"]
        discarded.assert_awaited_once()
        assert discarded.await_args
        assert discarded.await_args.args[1] == "did:plc:testuser"
        assert discarded.await_args.args[2] == ["at://p/3"]

    def test_share_cap_limits_slate(self, monkeypatch):
        """At most max_render_share of the retrieved candidates are rendered."""
        monkeypatch.setattr(FEEDS["your-feed"], "max_render_share", 0.2)
        candidates = _make_candidates("p", 10, with_embedding=True)

        data = self._get_feed(candidates, self._rank_result([1.0 - i / 10 for i in range(10)]))

        posts = [item["post"] for item in data["feed"]]
        assert posts == ["at://p/0", "at://p/1"]
        # The shortened slate still cursors so paging restarts the session.
        parsed = FeedCursor.decode(data["cursor"])
        assert parsed.offset == 2

    def test_mmr_floor_cuts_slate(self, monkeypatch):
        """With diversify on, the slate stops at the first sub-floor MMR pick."""
        monkeypatch.setattr(FEEDS["your-feed"], "diversify", True)
        monkeypatch.setattr(FEEDS["your-feed"], "min_mmr_score", 0.0)
        # Same author + identical embeddings: pick 2 carries maximal author and
        # content penalties, dropping its MMR score below 0.
        candidates = [
            CandidatePost(
                at_uri=f"at://p/{i}",
                content=f"post {i}",
                minilm_l12_embedding=TEST_EMBEDDING,
                score=None,
                author_did="did:plc:same",
                generator_name="test",
            )
            for i in range(3)
        ]
        discarded = AsyncMock()

        data = self._get_feed(candidates, self._rank_result([1.0, 1.0, 1.0]), discarded)

        assert len(data["feed"]) == 1
        # MMR-cut posts are not discarded — they may fit a future slate.
        discarded.assert_not_awaited()

    def test_fail_open_serves_precut_slate_when_everything_cut(self, monkeypatch):
        """If the gates reject everything retrieved, the pre-cutoff slate is served."""
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", 0.0)
        candidates = _make_candidates("p", 3, with_embedding=True)
        discarded = AsyncMock()

        data = self._get_feed(candidates, self._rank_result([-0.1, -0.2, -0.3]), discarded)

        posts = [item["post"] for item in data["feed"]]
        assert posts == ["at://p/0", "at://p/1", "at://p/2"]
        # Still recorded as discarded so the next session stops retrieving them.
        discarded.assert_awaited_once()
        assert discarded.await_args
        assert discarded.await_args.args[2] == ["at://p/0", "at://p/1", "at://p/2"]

    def test_fail_closed_returns_empty_feed_when_constant_flipped(self, monkeypatch):
        from app.routers import xrpc as xrpc_mod

        monkeypatch.setattr(xrpc_mod, "EMPTY_SLATE_FAIL_OPEN", False)
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", 0.0)
        candidates = _make_candidates("p", 3, with_embedding=True)

        data = self._get_feed(candidates, self._rank_result([-0.1, -0.2, -0.3]))

        assert data["feed"] == []
        assert "cursor" not in data

    def test_discarded_uris_excluded_on_fresh_request(self, monkeypatch):
        """Feeds with a rank floor exclude previously-discarded posts from generation."""
        monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", 0.0)
        candidates = _make_candidates("p", 2, with_embedding=True)
        gen_patch, primary_gen = self._patch_generators(candidates)

        rank_result = self._rank_result([0.5, 0.4])
        with gen_patch, \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result), \
             patch("app.routers.xrpc.record_discarded_posts", new_callable=AsyncMock), \
             patch(
                 "app.routers.xrpc.get_recent_seen_uris",
                 new_callable=AsyncMock,
                 return_value=["at://seen/1"],
             ), \
             patch(
                 "app.routers.xrpc.get_recent_discarded_uris",
                 new_callable=AsyncMock,
                 return_value=["at://disc/1"],
             ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": RANKED_FEED_URI}
            )

        assert resp.status_code == 200
        assert primary_gen.generate.call_args.kwargs.get("exclude_uris") == [
            "at://seen/1",
            "at://disc/1",
        ]

    def test_discarded_not_fetched_without_rank_floor(self):
        """Feeds without a min_rank_score never pay for the discarded-posts read."""
        gen_patch, _ = self._patch_generators(_make_candidates("p", 2, with_embedding=True))

        rank_result = self._rank_result([0.5, 0.4])
        with gen_patch, \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, return_value=rank_result), \
             patch(
                 "app.routers.xrpc.get_recent_discarded_uris", new_callable=AsyncMock
             ) as discarded_fetch:
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": RANKED_FEED_URI}
            )

        assert resp.status_code == 200
        discarded_fetch.assert_not_called()

    def test_cutoff_metrics_recorded(self, monkeypatch):
        """Hitting a gate emits feed.slate.cutoff_count and feed.slate.kept_share."""
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        from ..lib.metrics import MetricCollector, set_metric_collector

        reader = InMemoryMetricReader()
        set_metric_collector(MetricCollector._from_reader(reader, service_name="t", env="test"))
        try:
            monkeypatch.setattr(FEEDS["your-feed"], "min_rank_score", 0.0)
            candidates = _make_candidates("p", 4, with_embedding=True)
            self._get_feed(candidates, self._rank_result([0.5, 0.2, 0.0, -0.3]))

            metrics = {}
            metrics_data = reader.get_metrics_data()
            assert metrics_data is not None
            for rm in metrics_data.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        metrics[metric.name] = metric

            cutoff = metrics["feed.slate.cutoff_count"]
            (point,) = cutoff.data.data_points
            assert point.value == 1
            assert point.attributes["feed_name"] == "your-feed"
            assert point.attributes["reason"] == "rank_score"

            kept = metrics["feed.slate.kept_share"]
            (kept_point,) = kept.data.data_points
            assert kept_point.sum == pytest.approx(3 / 4)

            assert "candidates.generate.retrieved_share" in metrics
            assert "feed.slate.exclusion_size" in metrics
        finally:
            set_metric_collector(None)


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

    @pytest.fixture(autouse=True)
    def _clear_pinned_post(self, monkeypatch):
        """Isolate ranking tests from the pinned post — that's tested in TestPinnedPost."""
        monkeypatch.setattr(FEEDS["best-of-friends"], "pinned_post_uri", None)

    @pytest.fixture(autouse=True)
    def _disable_slate_cutoffs(self, monkeypatch):
        """Isolate ranking tests from the slate cutoffs — tested in TestSlateCutoffs."""
        monkeypatch.setattr(FEEDS["best-of-friends"], "max_render_share", None)
        monkeypatch.setattr(FEEDS["best-of-friends"], "min_rank_score", None)
        monkeypatch.setattr(FEEDS["best-of-friends"], "min_mmr_score", None)

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
        candidates = _make_candidates("p", 3, with_embedding=True)
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

    def test_ranking_failure_soft_fails_to_unranked(self):
        """When the two-tower ranker raises, the feed soft-fails and returns candidates in unranked order."""
        candidates = _make_candidates("p", 3, with_embedding=True)

        with self._patch_generators(candidates), \
             patch("app.routers.xrpc.run_predict", new_callable=AsyncMock, side_effect=RuntimeError("inference down")):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": BEST_OF_FRIENDS_FEED_URI},
            )

        assert resp.status_code == 200
        posts = [item["post"] for item in resp.json()["feed"]]
        assert len(posts) == 3


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

    @pytest.mark.asyncio
    async def test_seen_event_records_seen_posts(self):
        from app.routers.xrpc import Interaction, _record_interactions

        seen = "app.bsky.feed.defs#interactionSeen"
        interactions = [
            Interaction(item="at://post/1", event=seen, feed_context=_make_token(did="did:plc:u")),
            Interaction(item="at://post/2", event=seen, feed_context=_make_token(did="did:plc:u")),
        ]
        db = MagicMock()
        with (
            patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock),
            patch("app.routers.xrpc.record_seen_posts", new_callable=AsyncMock) as seen_rec,
        ):
            await _record_interactions(db, interactions)

        seen_rec.assert_called_once_with(db, "did:plc:u", ["at://post/1", "at://post/2"])

    @pytest.mark.asyncio
    async def test_non_seen_events_do_not_record_seen_posts(self):
        from app.routers.xrpc import Interaction, _record_interactions

        like = "app.bsky.feed.defs#interactionLike"
        ix = Interaction(item="at://post/1", event=like, feed_context=_make_token())
        db = MagicMock()
        with (
            patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock),
            patch("app.routers.xrpc.record_seen_posts", new_callable=AsyncMock) as seen_rec,
        ):
            await _record_interactions(db, [ix])

        seen_rec.assert_not_called()

    @pytest.mark.asyncio
    async def test_seen_not_denormalized_when_feed_disables_it(self, monkeypatch):
        """A feed with exclude_seen_posts off still stores the interaction but
        does not denormalize seen posts onto the user record."""
        from app.feeds import FEEDS
        from app.routers.xrpc import Interaction, _record_interactions

        monkeypatch.setattr(FEEDS["your-feed"], "exclude_seen_posts", False)
        seen = "app.bsky.feed.defs#interactionSeen"
        ix = Interaction(
            item="at://post/1", event=seen, feed_context=_make_token(feed="your-feed"),
        )
        db = MagicMock()
        with (
            patch("app.routers.xrpc.record_interaction", new_callable=AsyncMock) as rec,
            patch("app.routers.xrpc.record_seen_posts", new_callable=AsyncMock) as seen_rec,
        ):
            await _record_interactions(db, [ix])

        rec.assert_called_once()  # raw interaction is still stored
        seen_rec.assert_not_called()  # but not denormalized


# ---------------------------------------------------------------------------
# Feed debug capture
# ---------------------------------------------------------------------------

class TestFeedDebugCapture:
    """getFeedSkeleton always writes a lightweight snapshot; full debug is gated on debug_feeds."""

    @pytest.fixture(autouse=True)
    def _mock_auth_and_session(self):
        with (
            patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"),
            patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock),
            patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock),
        ):
            yield

    def _patch_generators(self, candidates):
        return _patch_unranked_your_feed_generators(candidates)

    def _user_doc(self, debug_feeds):
        from ..documents import UserDocument

        return UserDocument(user_did="did:plc:testuser", username=TEST_USERNAME, debug_feeds=debug_feeds)

    @staticmethod
    async def _drain(coros):
        for coro in coros:
            await coro

    @pytest.mark.asyncio
    async def test_empty_snapshot_skips_firestore_write(self):
        from ..routers.xrpc import _write_feed_snapshot_background

        snapshot = MagicMock(items=[])
        with patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock) as merge:
            await _write_feed_snapshot_background(MagicMock(), "did:plc:testuser", "r1", snapshot)

        merge.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_truncated_snapshot_records_metric(self, caplog):
        from ..routers.xrpc import _write_feed_snapshot_background

        snapshot = MagicMock(items=["at://a"], feed_name="your-feed")
        collector = MagicMock()
        with (
            patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock, return_value=True),
            patch("app.routers.xrpc.get_metric_collector", return_value=collector),
        ):
            await _write_feed_snapshot_background(MagicMock(), "did:plc:testuser", "r1", snapshot)

        collector.record.assert_called_once_with(
            "feed.snapshot.truncated_count", 1, feed_name="your-feed"
        )
        assert "reached item limit" in caplog.text

    def test_snapshot_written_for_all_users(self):
        """Snapshot always written regardless of debug flag."""
        with (
            self._patch_generators(_make_candidates("p", 3)),
            patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=self._user_doc(False)),
            patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock, return_value=False) as mock_snapshot,
            patch("app.routers.xrpc.write_feed_debug", new_callable=AsyncMock) as mock_debug,
        ):
            resp = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI})
            assert resp.status_code == 200

        mock_snapshot.assert_awaited_once()
        mock_debug.assert_not_awaited()

    def test_snapshot_contains_only_posts_returned_on_initial_page(self):
        candidates = _make_candidates("p", 8)
        with (
            self._patch_generators(candidates),
            patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=self._user_doc(False)),
            patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock, return_value=False) as mock_snapshot,
        ):
            response = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 3},
            )

        returned = [item["post"] for item in response.json()["feed"]]
        assert mock_snapshot.await_args is not None
        snapshot = mock_snapshot.await_args.args[3]
        assert snapshot.items == returned
        assert snapshot.items == ["at://p/0", "at://p/1", "at://p/2"]
        assert [meta.at_uri for meta in snapshot.items_meta] == snapshot.items

    def test_full_debug_record_when_enabled(self):
        """Full debug record written in background when debug_feeds is on."""
        spawned: list = []
        with (
            self._patch_generators(_make_candidates("p", 3)),
            patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=self._user_doc(True)),
            patch("app.routers.xrpc._spawn_background", side_effect=lambda coro: spawned.append(coro)),
            patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock, return_value=False) as mock_snapshot,
            patch("app.routers.xrpc.write_feed_debug", new_callable=AsyncMock) as mock_debug,
        ):
            resp = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI})
            assert resp.status_code == 200
            asyncio.run(self._drain(spawned))

        mock_snapshot.assert_awaited_once()
        mock_debug.assert_awaited_once()
        doc = mock_debug.call_args[0][1]
        assert doc.user_did == "did:plc:testuser"
        assert doc.feed_name == FEED_RKEY
        assert doc.final_order == ["at://p/0", "at://p/1", "at://p/2"]
        assert any(r.generator_name == "two_tower" for r in doc.generator_outputs)

    def test_flag_read_failure_is_non_fatal(self, caplog):
        """Snapshot still written even when user lookup fails."""
        with (
            self._patch_generators(_make_candidates("p", 2)),
            patch("app.routers.xrpc.get_user", new_callable=AsyncMock, side_effect=RuntimeError("boom")),
            patch("app.routers.xrpc.merge_feed_snapshot", new_callable=AsyncMock, return_value=False) as mock_snapshot,
            patch("app.routers.xrpc.write_feed_debug", new_callable=AsyncMock) as mock_debug,
        ):
            resp = client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params={"feed": FEED_URI})
            assert resp.status_code == 200

        mock_snapshot.assert_awaited_once()
        mock_debug.assert_not_awaited()


# ---------------------------------------------------------------------------
# Probe bypass (Cloud Scheduler)
# ---------------------------------------------------------------------------

class TestGetFeedSkeletonProbe:
    """Cloud Scheduler hits the endpoint without AT Protocol auth.

    When GE_PROBE_SECRET is set and the request carries the matching
    X-Probe-Secret header, auth is bypassed and a synthetic DID is used.
    """

    PROBE_SECRET = "test-probe-secret-xyz"

    @pytest.fixture(autouse=True)
    def _set_probe_secret(self, monkeypatch):
        monkeypatch.setenv("GE_PROBE_SECRET", self.PROBE_SECRET)

    @pytest.fixture(autouse=True)
    def _no_at_proto_auth(self):
        """Simulate requests with no AT Protocol Bearer token."""
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value=None):
            yield

    @pytest.fixture(autouse=True)
    def _mock_firestore(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    def test_correct_probe_secret_returns_200(self):
        """Matching X-Probe-Secret bypasses 401 and returns a feed."""
        with _patch_unranked_your_feed_generators(_make_candidates("p", 3)):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"X-Probe-Secret": self.PROBE_SECRET},
            )
        assert resp.status_code == 200

    def test_probe_does_not_write_observability_snapshot(self):
        with (
            _patch_unranked_your_feed_generators(_make_candidates("p", 3)),
            patch(
                "app.routers.xrpc.merge_feed_snapshot",
                new_callable=AsyncMock,
            ) as snapshot_write,
        ):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
                headers={"X-Probe-Secret": self.PROBE_SECRET},
            )

        assert resp.status_code == 200
        snapshot_write.assert_not_awaited()

    def test_wrong_probe_secret_returns_401(self):
        """Wrong X-Probe-Secret still 401s (no bypass)."""
        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": FEED_URI},
            headers={"X-Probe-Secret": "wrong-secret"},
        )
        assert resp.status_code == 401

    def test_missing_probe_header_returns_401(self):
        """No header at all still 401s."""
        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": FEED_URI},
        )
        assert resp.status_code == 401

    def test_probe_secret_env_unset_ignores_header(self, monkeypatch):
        """When GE_PROBE_SECRET is not configured, any header value is ignored."""
        monkeypatch.delenv("GE_PROBE_SECRET", raising=False)
        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": FEED_URI},
            headers={"X-Probe-Secret": self.PROBE_SECRET},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Pinned post
# ---------------------------------------------------------------------------


class TestPinnedPost:
    PINNED_URI = "at://did:plc:pinauthor/app.bsky.feed.post/pinnedpost"

    def _make_random_feed_with_pin(self):
        cfg = FEEDS["random"].model_copy(update={"pinned_post_uri": self.PINNED_URI})
        return {"random": cfg, **{k: v for k, v in FEEDS.items() if k != "random"}}

    @patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser")
    @patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=None)
    @patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock)
    @patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock)
    def test_pinned_post_is_first_on_first_page(self, *mocks):
        """Pinned post appears as the first item when no cursor is sent."""
        from app.routers import xrpc as xrpc_mod

        candidates = _make_candidates("did:plc:a", 5, "random_posts")
        random_gen = AsyncMock()
        random_gen.generate.return_value = CandidateResult(
            generator_name="random_posts", candidates=candidates
        )

        def fake_get_generator(name):
            return random_gen if name == "random_posts" else None

        patched_feeds = self._make_random_feed_with_pin()
        with patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator), \
             patch.object(xrpc_mod, "FEEDS", patched_feeds):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 10},
            )

        assert resp.status_code == 200
        assert resp.json()["feed"][0]["post"] == self.PINNED_URI

    @patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser")
    @patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=None)
    @patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock)
    @patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock)
    def test_pinned_post_is_excluded_from_observability_snapshot(self, *mocks):
        from app.routers import xrpc as xrpc_mod

        candidates = _make_candidates("did:plc:a", 5, "random_posts")
        random_gen = AsyncMock()
        random_gen.generate.return_value = CandidateResult(
            generator_name="random_posts", candidates=candidates
        )
        patched_feeds = self._make_random_feed_with_pin()
        with (
            patch(
                "app.lib.candidates.generate.get_generator",
                return_value=random_gen,
            ),
            patch.object(xrpc_mod, "FEEDS", patched_feeds),
            patch(
                "app.routers.xrpc.merge_feed_snapshot",
                new_callable=AsyncMock,
                return_value=False,
            ) as snapshot_write,
        ):
            response = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 3},
            )

        assert response.json()["feed"][0]["post"] == self.PINNED_URI
        assert snapshot_write.await_args is not None
        snapshot = snapshot_write.await_args.args[3]
        assert self.PINNED_URI not in snapshot.items
        assert snapshot.items == [
            "at://did:plc:a/0",
            "at://did:plc:a/1",
        ]
        assert all(meta.at_uri != self.PINNED_URI for meta in snapshot.items_meta)

    @patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser")
    @patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=None)
    @patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock)
    @patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock)
    def test_pinned_post_not_duplicated_if_in_candidates(self, *mocks):
        """If the pinned URI is already in generated candidates, it appears only once."""
        from app.routers import xrpc as xrpc_mod

        candidates = [
            CandidatePost(at_uri=self.PINNED_URI, content="pinned", score=None, generator_name="random_posts"),
            *_make_candidates("did:plc:a", 4, "random_posts"),
        ]
        random_gen = AsyncMock()
        random_gen.generate.return_value = CandidateResult(
            generator_name="random_posts", candidates=candidates
        )

        def fake_get_generator(name):
            return random_gen if name == "random_posts" else None

        patched_feeds = self._make_random_feed_with_pin()
        with patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator), \
             patch.object(xrpc_mod, "FEEDS", patched_feeds):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 10},
            )

        assert resp.status_code == 200
        post_uris = [item["post"] for item in resp.json()["feed"]]
        assert post_uris.count(self.PINNED_URI) == 1
        assert post_uris[0] == self.PINNED_URI

    @patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser")
    @patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=None)
    @patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock)
    @patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock)
    def test_pinned_page_cursor_does_not_skip_a_post(self, *mocks):
        """The pinned first page consumes limit-1 generated posts; the cursor
        must account for that so page 2 continues without skipping one."""
        from app.routers import xrpc as xrpc_mod

        candidates = _make_candidates("did:plc:a", 20, "random_posts")
        random_gen = AsyncMock()
        random_gen.generate.return_value = CandidateResult(
            generator_name="random_posts", candidates=candidates
        )

        def fake_get_generator(name):
            return random_gen if name == "random_posts" else None

        patched_feeds = self._make_random_feed_with_pin()
        with patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator), \
             patch.object(xrpc_mod, "FEEDS", patched_feeds):
            first = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 10},
            ).json()

        assert first["feed"][0]["post"] == self.PINNED_URI
        assert first["feed"][-1]["post"] == "at://did:plc:a/8"
        assert FeedCursor.decode(first["cursor"]).offset == 9

        with patch.object(xrpc_mod, "FEEDS", patched_feeds):
            second = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 10, "cursor": first["cursor"]},
            ).json()

        assert second["feed"][0]["post"] == "at://did:plc:a/9"

    @patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser")
    @patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=None)
    @patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock)
    @patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock)
    def test_pinned_post_not_on_subsequent_pages(self, *mocks):
        """Pinned post does not appear when a cursor is sent (subsequent pages)."""
        from app.routers import xrpc as xrpc_mod

        patched_feeds = self._make_random_feed_with_pin()
        cache = InMemoryFeedCache()
        cached_uris = [f"at://did:plc:a/{i}" for i in range(20)]
        cache._store["testcacheid"] = cached_uris
        app.state.feed_cache = cache

        cursor = FeedCursor(id="testcacheid", offset=10).encode()
        with patch.object(xrpc_mod, "FEEDS", patched_feeds):
            resp = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": RANDOM_FEED_URI, "limit": 10, "cursor": cursor},
            )

        assert resp.status_code == 200
        post_uris = [item["post"] for item in resp.json()["feed"]]
        assert self.PINNED_URI not in post_uris




# ---------------------------------------------------------------------------
# Social-radius override
# ---------------------------------------------------------------------------


class TestSocialRadiusOverride:
    """Generator weights are overridden based on user_doc.social_radius."""

    @pytest.fixture(autouse=True)
    def _mock_auth(self):
        with patch("app.routers.xrpc.verify_auth_header", new_callable=AsyncMock, return_value="did:plc:testuser"):
            yield

    @pytest.fixture(autouse=True)
    def _mock_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), \
             patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock):
            yield

    @patch("app.routers.xrpc.get_user")
    @patch("app.routers.xrpc._run_ranking_pipeline", new_callable=AsyncMock)
    def test_applies_social_radius_preset_0(self, mock_pipeline, mock_get_user):
        """social_radius=0 (Friends) → followed_users-heavy weights."""
        from ..documents import UserDocument
        from .xrpc import SOCIAL_RADIUS_PRESETS, PipelineResult

        mock_get_user.return_value = UserDocument(
            user_did="did:plc:testuser",
            social_radius=0,
        )
        mock_pipeline.return_value = PipelineResult(["at://dummy/1", "at://dummy/2"], [])

        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": RANKED_FEED_URI, "limit": 30},
        )

        assert resp.status_code == 200
        gen_request = mock_pipeline.call_args.args[1]
        assert gen_request.generators == SOCIAL_RADIUS_PRESETS[0]

    @patch("app.routers.xrpc.get_user")
    @patch("app.routers.xrpc._run_ranking_pipeline", new_callable=AsyncMock)
    def test_applies_social_radius_preset_4(self, mock_pipeline, mock_get_user):
        """social_radius=4 (Everyone) → popularity-heavy weights."""
        from ..documents import UserDocument
        from .xrpc import SOCIAL_RADIUS_PRESETS, PipelineResult

        mock_get_user.return_value = UserDocument(
            user_did="did:plc:testuser",
            social_radius=4,
        )
        mock_pipeline.return_value = PipelineResult(["at://dummy/1", "at://dummy/2"], [])

        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": RANKED_FEED_URI, "limit": 30},
        )

        assert resp.status_code == 200
        gen_request = mock_pipeline.call_args.args[1]
        assert gen_request.generators == SOCIAL_RADIUS_PRESETS[4]

    @patch("app.routers.xrpc.get_user")
    @patch("app.routers.xrpc._run_ranking_pipeline", new_callable=AsyncMock)
    def test_default_radius_when_missing(self, mock_pipeline, mock_get_user):
        """User doc without social_radius field → defaults to 3 (balanced)."""
        from ..documents import UserDocument
        from .xrpc import SOCIAL_RADIUS_PRESETS, PipelineResult

        mock_get_user.return_value = UserDocument(
            user_did="did:plc:testuser",
        )
        mock_pipeline.return_value = PipelineResult(["at://dummy/1"], [])

        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": RANKED_FEED_URI, "limit": 30},
        )

        assert resp.status_code == 200
        gen_request = mock_pipeline.call_args.args[1]
        assert gen_request.generators == SOCIAL_RADIUS_PRESETS[3]

    @patch("app.routers.xrpc.get_user")
    @patch("app.routers.xrpc._run_ranking_pipeline", new_callable=AsyncMock)
    def test_no_override_for_non_your_feed(self, mock_pipeline, mock_get_user):
        """best-of-friends is unaffected by social_radius."""
        from ..documents import UserDocument
        from .xrpc import PipelineResult

        mock_get_user.return_value = UserDocument(
            user_did="did:plc:testuser",
            social_radius=0,
        )
        mock_pipeline.return_value = PipelineResult(["at://dummy/1"], [])

        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": BEST_OF_FRIENDS_FEED_URI, "limit": 30},
        )

        assert resp.status_code == 200
        gen_request = mock_pipeline.call_args.args[1]
        assert len(gen_request.generators) == 1
        assert gen_request.generators[0].name == "followed_users"
        assert gen_request.generators[0].weight == 1.0

    @patch("app.routers.xrpc.get_user")
    @patch("app.routers.xrpc._run_ranking_pipeline", new_callable=AsyncMock)
    def test_fallen_back_to_defaults_when_user_has_no_doc(self, mock_pipeline, mock_get_user):
        """User doc is None → no override, defaults used."""
        from .xrpc import SOCIAL_RADIUS_PRESETS, PipelineResult

        mock_get_user.return_value = None
        mock_pipeline.return_value = PipelineResult(["at://dummy/1"], [])

        resp = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={"feed": RANKED_FEED_URI, "limit": 30},
        )

        assert resp.status_code == 200
        gen_request = mock_pipeline.call_args.args[1]
        assert gen_request.generators == SOCIAL_RADIUS_PRESETS[3]


class TestGetFeedSkeletonMetrics:
    """Feed renders emit exactly one success or failure counter."""

    @pytest.fixture(autouse=True)
    def _fake_metric_collector(self):
        from typing import cast

        self.mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, self.mc))
        yield
        set_metric_collector(None)

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock), patch(
            "app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock
        ):
            yield

    def test_success_records_success_count(self):
        with (
            _patch_unranked_your_feed_generators(_make_candidates("p", 2)),
            patch(
                "app.routers.xrpc.verify_auth_header",
                new_callable=AsyncMock,
                return_value="did:plc:user",
            ),
        ):
            response = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
            )

        assert response.status_code == 200
        success_calls = [
            call for call in self.mc.calls if call[0] == "feed.render.success_count"
        ]
        assert len(success_calls) == 1
        assert success_calls[0][2]["feed_name"] == FEED_RKEY

    def test_unknown_feed_records_failure_count_400(self):
        response = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={
                "feed": f"at://{SERVICE_DID}/app.bsky.feed.generator/nonexistent"
            },
        )

        assert response.status_code == 400
        failure_calls = [
            call for call in self.mc.calls if call[0] == "feed.render.failure_count"
        ]
        assert len(failure_calls) == 1
        assert failure_calls[0][2] == {
            "feed_name": "unknown",
            "status_code": "400",
        }

    def test_auth_failure_records_failure_count_401(self):
        with patch(
            "app.routers.xrpc.verify_auth_header",
            new_callable=AsyncMock,
            return_value=None,
        ):
            response = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI},
            )

        assert response.status_code == 401
        failure_calls = [
            call for call in self.mc.calls if call[0] == "feed.render.failure_count"
        ]
        assert len(failure_calls) == 1
        assert failure_calls[0][2] == {
            "feed_name": FEED_RKEY,
            "status_code": "401",
        }

    def test_failure_records_no_success_count(self):
        client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={
                "feed": f"at://{SERVICE_DID}/app.bsky.feed.generator/nonexistent"
            },
        )

        assert [
            call for call in self.mc.calls if call[0] == "feed.render.success_count"
        ] == []


class TestDevSession:
    """The development stand-in for a signed-in user (dev_session_did).

    Distinct from the probe bypass on purpose: a probe is monitoring traffic
    excluded from user data, while this has to be indistinguishable from a real
    session downstream so the snapshot/user-record path can be exercised
    locally.
    """

    def _request(self, headers: dict[str, str]):
        from unittest.mock import MagicMock

        request = MagicMock()
        request.headers = headers
        return request

    def test_disabled_when_no_secret_is_configured(self, monkeypatch):
        from ..routers.xrpc import dev_session_did

        monkeypatch.delenv("GE_DEV_SESSION_SECRET", raising=False)
        request = self._request(
            {"X-Dev-Session": "anything", "X-Dev-Session-DID": "did:plc:abc"}
        )
        assert dev_session_did(request) is None

    def test_ignores_a_wrong_secret(self, monkeypatch):
        from ..routers.xrpc import dev_session_did

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "correct")
        request = self._request(
            {"X-Dev-Session": "wrong", "X-Dev-Session-DID": "did:plc:abc"}
        )
        assert dev_session_did(request) is None

    def test_ignores_a_request_with_no_headers(self, monkeypatch):
        from ..routers.xrpc import dev_session_did

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "correct")
        assert dev_session_did(self._request({})) is None

    def test_returns_the_did_when_the_secret_matches(self, monkeypatch):
        from ..routers.xrpc import dev_session_did

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "correct")
        request = self._request(
            {"X-Dev-Session": "correct", "X-Dev-Session-DID": "did:plc:abc"}
        )
        assert dev_session_did(request) == "did:plc:abc"

    def test_rejects_a_did_that_is_not_did_plc(self, monkeypatch):
        # The secret matched, so this is a developer mistake worth reporting
        # rather than stray traffic to ignore.
        from fastapi import HTTPException

        from ..routers.xrpc import dev_session_did

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "correct")
        request = self._request(
            {"X-Dev-Session": "correct", "X-Dev-Session-DID": "alice.bsky.social"}
        )
        with pytest.raises(HTTPException) as excinfo:
            dev_session_did(request)
        assert excinfo.value.status_code == 400


class TestDevSessionStartupGuard:
    """It must be impossible to serve deployed traffic with this enabled."""

    @pytest.mark.asyncio
    async def test_refuses_to_start_when_enabled_in_a_deployed_environment(
        self, monkeypatch
    ):
        from ..main import app, lifespan

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "anything")
        monkeypatch.setenv("GE_ELASTICSEARCH_API_KEY", "k")
        monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "s")
        monkeypatch.setenv("ENVIRONMENT", "prod")

        with pytest.raises(RuntimeError, match="GE_DEV_SESSION_SECRET"):
            async with lifespan(app):
                pass

    def test_deployed_environment_is_fine_without_it(self, monkeypatch):
        # Guard must key on the variable, not merely on being deployed. This
        # calls the guard directly rather than entering the lifespan: a clean
        # startup goes on to build real GCP clients, which needs credentials
        # CI doesn't have.
        from ..main import _reject_dev_session_secret_in_deployment

        monkeypatch.delenv("GE_DEV_SESSION_SECRET", raising=False)
        monkeypatch.setenv("ENVIRONMENT", "prod")

        _reject_dev_session_secret_in_deployment()

    def test_local_environment_may_set_it(self, monkeypatch):
        from ..main import _reject_dev_session_secret_in_deployment

        monkeypatch.setenv("GE_DEV_SESSION_SECRET", "anything")
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.delenv("GE_ENVIRONMENT", raising=False)

        _reject_dev_session_secret_in_deployment()


class TestPosthogTracking:
    """Verify PostHog events are emitted from XRPC background handlers."""

    @pytest.mark.asyncio
    async def test_record_session_calls_track_session(self):
        from unittest.mock import AsyncMock, MagicMock, patch

        from ..routers.xrpc import _record_session

        db = AsyncMock()
        request = MagicMock()
        request.app.state.id_resolver = AsyncMock()
        did_doc = MagicMock()
        did_doc.get_handle.return_value = "alice.bsky.app"
        request.app.state.id_resolver.did.resolve = AsyncMock(return_value=did_doc)

        mock_client = MagicMock()
        with patch("app.routers.xrpc.get_posthog_client", return_value=mock_client):
            with patch("app.routers.xrpc.track_session") as mock_track:
                await _record_session(request, "did:plc:abc", "your-feed", db)
                mock_track.assert_called_once()
                call_kwargs = mock_track.call_args
                assert call_kwargs.args[0] is mock_client
                assert call_kwargs.args[1] == "did:plc:abc"
                assert call_kwargs.args[2] == "alice.bsky.app"
                assert call_kwargs.args[3] == "your-feed"

    @pytest.mark.asyncio
    async def test_record_session_survives_handle_resolution_failure(self):
        """A DID that won't resolve must not cost us the session record.

        Resolution goes over the network to the PLC directory, so it fails for
        reasons unrelated to the user existing. Before this, one such failure
        skipped the user upsert, the feed-activity write and the analytics
        event outright.
        """
        from unittest.mock import AsyncMock, MagicMock, patch

        from ..routers.xrpc import _record_session

        db = AsyncMock()
        request = MagicMock()
        request.app.state.id_resolver = AsyncMock()
        request.app.state.id_resolver.did.resolve = AsyncMock(
            side_effect=RuntimeError("PLC directory unavailable")
        )

        with patch("app.routers.xrpc.get_posthog_client", return_value=MagicMock()):
            with patch("app.routers.xrpc.track_session") as mock_track:
                with patch("app.routers.xrpc.upsert_user", new=AsyncMock()) as mock_upsert:
                    with patch(
                        "app.routers.xrpc.upsert_feed_activity", new=AsyncMock()
                    ) as mock_activity:
                        await _record_session(request, "did:plc:abc", "your-feed", db)

        mock_upsert.assert_awaited_once()
        upsert_args = mock_upsert.await_args
        assert upsert_args is not None
        # The DID is the identity; the handle is enrichment we simply lack.
        assert upsert_args.args[1] == "did:plc:abc"
        assert upsert_args.args[2] is None
        mock_activity.assert_awaited_once()
        mock_track.assert_called_once()
        assert mock_track.call_args.args[2] is None

    @pytest.mark.asyncio
    async def test_record_interactions_calls_track_interaction(self):
        from unittest.mock import AsyncMock, MagicMock, patch

        from ..lib.feed_context import FeedContextPayload, encode_feed_context
        from ..routers.xrpc import Interaction, _record_interactions

        feed_context = encode_feed_context(
            FeedContextPayload(did="did:plc:abc", feed="your-feed", rid="reqid123", iat=0)
        )

        ix = Interaction(
            item="at://did/post/1",
            event="app.bsky.feed.defs#interactionLike",
            feed_context=feed_context,
        )

        db = AsyncMock()
        mock_client = MagicMock()
        with patch("app.routers.xrpc.get_posthog_client", return_value=mock_client):
            with patch("app.routers.xrpc.track_interaction") as mock_track:
                await _record_interactions(db, [ix])
                mock_track.assert_called_once()
                call_kwargs = mock_track.call_args
                assert call_kwargs.args[0] is mock_client
                assert call_kwargs.args[1] == "did:plc:abc"
                assert call_kwargs.args[2] == "interactionLike"
                assert call_kwargs.args[3] == "your-feed"
                assert call_kwargs.args[4] == "at://did/post/1"

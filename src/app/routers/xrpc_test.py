"""Tests for the XRPC feed generator endpoints."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..models import CandidatePost
from ..lib.candidates.base import CandidateResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SERVICE_DID = "did:web:test.example.com"
PUBLISHER_DID = "did:plc:publisherabc123"
FEED_RKEY = "basic-similarity"
FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{FEED_RKEY}"
RANDOM_FEED_RKEY = "random"
RANDOM_FEED_URI = f"at://{SERVICE_DID}/app.bsky.feed.generator/{RANDOM_FEED_RKEY}"
# The AppView sends the publisher DID in the feed URI, not the service DID.
FEED_URI_FROM_APPVIEW = f"at://{PUBLISHER_DID}/app.bsky.feed.generator/{FEED_RKEY}"
TEST_USERNAME = "testuser.bsky.app"


def _make_candidates(prefix: str, n: int, generator_name: str = "test") -> list[CandidatePost]:
    return [
        CandidatePost(at_uri=f"at://{prefix}/{i}", content=f"post {i}", minilm_l12_embedding=None, score=None, generator_name=generator_name)
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_feed_generator_did(monkeypatch):
    """Ensure a deterministic service DID for all tests."""
    monkeypatch.setenv("GE_FEED_GENERATOR_DID", SERVICE_DID)


@pytest.fixture(autouse=True)
def fake_app_es():
    """Attach a fake ES client so the app doesn't need a real connection."""
    app.state.es = AsyncMock()
    app.state.id_resolver = AsyncMock()
    did_doc = MagicMock()
    did_doc.get_handle.return_value = TEST_USERNAME
    app.state.id_resolver.did.resolve = AsyncMock(return_value=did_doc)
    app.state.firestore = AsyncMock()
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

    def test_feeds_list_length(self):
        data = client.get("/xrpc/app.bsky.feed.describeFeedGenerator").json()
        assert len(data["feeds"]) == 2


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
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock):
            yield

    def _patch_generators(self, primary_candidates, infill_candidates=None):
        """Return a context-manager that patches get_generator.

        ``primary_candidates`` and ``infill_candidates`` are lists of
        ``CandidatePost`` (or ``None`` to simulate an unregistered generator).
        """
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="post_similarity",
            candidates=primary_candidates,
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity",
            candidates=infill_candidates or [],
        )

        def fake_get_generator(name):
            if name == "post_similarity":
                return primary_gen
            if name == "popularity":
                return infill_gen
            return None

        return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator)

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
        primary = _make_candidates("prim", 5, "post_similarity")
        with self._patch_generators(primary) as mock_get:
            data = client.get(
                "/xrpc/app.bsky.feed.getFeedSkeleton",
                params={"feed": FEED_URI, "limit": 5},
            ).json()
        assert len(data["feed"]) == 5
        # Infill generator's generate method should not have been called
        infill_gen = mock_get.side_effect("popularity")
        infill_gen.generate.assert_not_called()

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

        def fake_get(name):
            return {"post_similarity": primary_gen, "popularity": infill_gen}.get(name)

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
# Authentication integration
# ---------------------------------------------------------------------------

class TestGetFeedSkeletonAuth:
    """Tests that getFeedSkeleton correctly passes through the authenticated DID."""

    def _patch_generators(self, primary_candidates):
        primary_gen = AsyncMock()
        primary_gen.generate.return_value = CandidateResult(
            generator_name="post_similarity",
            candidates=primary_candidates,
        )
        infill_gen = AsyncMock()
        infill_gen.generate.return_value = CandidateResult(
            generator_name="popularity",
            candidates=[],
        )

        def fake_get_generator(name):
            if name == "post_similarity":
                return primary_gen
            if name == "popularity":
                return infill_gen
            return None

        return patch("app.lib.candidates.generate.get_generator", side_effect=fake_get_generator)

    @pytest.fixture(autouse=True)
    def _mock_firestore_upsert(self):
        """Avoid real Firestore interactions unless a test explicitly patches it."""
        with patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock):
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

    def test_username_resolution_failure_is_fatal(self):
        """Username resolution failures should fail the request."""
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

        assert resp.status_code == 500
        assert resp.json()["detail"] == "Username resolution failed"

    def test_firestore_upsert_failure_is_fatal(self):
        """Firestore write errors should fail the request."""
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

        assert resp.status_code == 500
        assert resp.json()["detail"] == "Firestore write failed"

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

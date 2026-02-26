"""Tests for AT Protocol JWT authentication."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from ..lib.atproto_auth import (
    BEARER_PREFIX,
    init_id_resolver,
    verify_auth_header,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SERVICE_DID = "did:web:test.example.com"
USER_DID = "did:plc:user123"


def _build_app_with_auth_endpoint(*, attach_resolver: bool = True) -> FastAPI:
    """Create a minimal FastAPI app with a test endpoint that uses
    ``verify_auth_header``."""
    test_app = FastAPI()

    if attach_resolver:
        test_app.state.id_resolver = MagicMock()  # placeholder, overridden per test

    @test_app.get("/auth-test")
    async def auth_test(request: Request):
        user_did = await verify_auth_header(request, service_did=SERVICE_DID)
        return {"user_did": user_did}

    return test_app


# ---------------------------------------------------------------------------
# init_id_resolver
# ---------------------------------------------------------------------------

class TestInitIdResolver:
    def test_returns_async_id_resolver(self):
        resolver = init_id_resolver()
        # The atproto SDK type
        from atproto_identity.resolver import AsyncIdResolver

        assert isinstance(resolver, AsyncIdResolver)

    def test_resolver_has_did_property(self):
        resolver = init_id_resolver()
        assert hasattr(resolver, "did")

    def test_resolver_has_handle_property(self):
        resolver = init_id_resolver()
        assert hasattr(resolver, "handle")


# ---------------------------------------------------------------------------
# verify_auth_header
# ---------------------------------------------------------------------------

class TestVerifyAuthHeader:
    """Tests for verify_auth_header using mocked JWT verification."""

    @pytest.fixture()
    def app(self):
        return _build_app_with_auth_endpoint()

    # --- no auth header ---

    def test_no_auth_header_returns_empty_string(self, app):
        client = TestClient(app)
        resp = client.get("/auth-test")
        assert resp.json()["user_did"] == ""

    # --- non-Bearer scheme ---

    def test_non_bearer_scheme_returns_empty(self, app):
        client = TestClient(app)
        resp = client.get("/auth-test", headers={"Authorization": "Basic abc123"})
        assert resp.json()["user_did"] == ""

    # --- empty bearer token ---

    def test_empty_bearer_token_returns_empty(self, app):
        client = TestClient(app)
        resp = client.get("/auth-test", headers={"Authorization": "Bearer "})
        assert resp.json()["user_did"] == ""

    def test_bearer_whitespace_only_returns_empty(self, app):
        client = TestClient(app)
        resp = client.get("/auth-test", headers={"Authorization": "Bearer   "})
        assert resp.json()["user_did"] == ""

    # --- no id_resolver on app.state ---

    def test_missing_resolver_returns_empty(self):
        app_no_resolver = _build_app_with_auth_endpoint(attach_resolver=False)
        client = TestClient(app_no_resolver)
        resp = client.get(
            "/auth-test", headers={"Authorization": "Bearer some.jwt.token"}
        )
        assert resp.json()["user_did"] == ""

    # --- successful verification ---

    def test_valid_jwt_returns_user_did(self, app):
        mock_payload = MagicMock()
        mock_payload.iss = USER_DID

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            return_value=mock_payload,
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        assert resp.json()["user_did"] == USER_DID

    def test_verify_called_with_service_did(self, app):
        mock_payload = MagicMock()
        mock_payload.iss = USER_DID

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            return_value=mock_payload,
        ) as mock_verify:
            client = TestClient(app)
            client.get(
                "/auth-test",
                headers={"Authorization": "Bearer valid.jwt.token"},
            )

        mock_verify.assert_called_once()
        _, kwargs = mock_verify.call_args
        assert kwargs.get("own_did") == SERVICE_DID

    # --- expired token ---

    def test_expired_token_returns_empty(self, app):
        from atproto_server.exceptions import TokenExpiredSignatureError

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            side_effect=TokenExpiredSignatureError("expired"),
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer expired.jwt.token"},
            )
        assert resp.json()["user_did"] == ""

    # --- bad audience ---

    def test_invalid_audience_returns_empty(self, app):
        from atproto_server.exceptions import TokenInvalidAudienceError

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            side_effect=TokenInvalidAudienceError("bad aud"),
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer wrong-aud.jwt.token"},
            )
        assert resp.json()["user_did"] == ""

    # --- invalid signature ---

    def test_invalid_signature_returns_empty(self, app):
        from atproto_server.exceptions import TokenInvalidSignatureError

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            side_effect=TokenInvalidSignatureError("bad sig"),
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer bad-sig.jwt.token"},
            )
        assert resp.json()["user_did"] == ""

    # --- decode error ---

    def test_decode_error_returns_empty(self, app):
        from atproto_server.exceptions import TokenDecodeError

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            side_effect=TokenDecodeError("malformed"),
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer garbage"},
            )
        assert resp.json()["user_did"] == ""

    # --- unexpected error ---

    def test_unexpected_error_returns_empty(self, app):
        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            side_effect=RuntimeError("network down"),
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer some.jwt.token"},
            )
        assert resp.json()["user_did"] == ""

    # --- payload with iss=None ---

    def test_payload_iss_none_returns_empty_string(self, app):
        mock_payload = MagicMock()
        mock_payload.iss = None

        with patch(
            "app.lib.atproto_auth.verify_jwt_async",
            new_callable=AsyncMock,
            return_value=mock_payload,
        ):
            client = TestClient(app)
            resp = client.get(
                "/auth-test",
                headers={"Authorization": "Bearer valid.jwt.token"},
            )
        assert resp.json()["user_did"] == ""

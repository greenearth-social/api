from typing import Annotated
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from fastapi.testclient import TestClient

from .documents import ApiKeyDocument
from .lib.api_keys import generate_key
from .main import app
from .security import (
    RequireAdminApiKey,
    RequireApiKey,
    require_api_scope,
    verify_admin_api_key,
    verify_api_key,
)


@pytest.fixture
def client_valid():
    """Client where all API key checks pass, returning a fake key_id."""
    app.dependency_overrides[verify_api_key] = lambda: "a1b2c3d4"
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def client_invalid():
    """Client where all API key checks return 401."""

    def _raise():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )

    app.dependency_overrides[verify_api_key] = _raise
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestRootEndpointAuth:
    def test_root_returns_401_without_api_key(self, client_invalid):
        response = client_invalid.get("/")
        assert response.status_code == 401

    def test_root_returns_401_with_invalid_api_key(self, client_invalid):
        response = client_invalid.get("/", headers={"X-API-Key": "gea_invalid"})
        assert response.status_code == 401

    def test_root_returns_401_response_body(self, client_invalid):
        response = client_invalid.get("/")
        assert response.json() == {"detail": "Invalid or missing API key"}

    def test_root_returns_200_with_valid_api_key(self, client_valid):
        response = client_valid.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "Green Earth API"}


class TestHealthEndpointNoAuth:
    def test_health_returns_200_without_api_key(self, client_invalid):
        response = client_invalid.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self, client_invalid):
        response = client_invalid.get("/health")
        assert response.json()["status"] == "ok"


@pytest.fixture
def key_credentials():
    return generate_key()


@pytest.fixture
def key_doc(key_credentials):
    key_id, _, key_hash = key_credentials
    return ApiKeyDocument(
        key_id=key_id,
        key_hash=key_hash,
        email="test@example.com",
    )


@pytest.fixture
def key_lookup(key_doc, monkeypatch):
    lookup = AsyncMock(return_value=key_doc)
    monkeypatch.setattr("app.lib.api_keys.get_api_key_doc", lookup)
    return lookup


@pytest.fixture
def key_ref():
    return AsyncMock()


@pytest.fixture
def endpoint():
    return MagicMock(side_effect=lambda key_id: {"key_id": key_id})


@pytest.fixture
def scope_client(key_lookup, key_ref, endpoint):
    # An isolated app exercises real auth without conftest's dependency overrides.
    auth_app = FastAPI()
    auth_app.state.firestore = MagicMock()
    auth_app.state.firestore.collection.return_value.document.return_value = key_ref
    default_router = APIRouter(dependencies=[Depends(verify_api_key)])

    @auth_app.get("/default")
    async def default_access(key_id: RequireApiKey):
        return endpoint(key_id)

    @auth_app.get("/implicit-default")
    async def implicit_default_access(key_id: Annotated[str, Depends(require_api_scope())]):
        return endpoint(key_id)

    @auth_app.get("/scoped")
    @auth_app.get("/admin-scoped", dependencies=[Depends(verify_admin_api_key)])
    @default_router.get("/combined")
    async def scoped_access(key_id: Annotated[str, Depends(require_api_scope("user-config"))]):
        return endpoint(key_id)

    @auth_app.get("/admin")
    async def admin_access(key_id: RequireAdminApiKey):
        return endpoint(key_id)

    auth_app.include_router(default_router)
    return TestClient(auth_app)


class TestRequireApiScope:
    @pytest.mark.parametrize("path", ["/default", "/implicit-default", "/scoped", "/combined"])
    @pytest.mark.parametrize("api_key", [None, "invalid"])
    def test_rejects_missing_or_invalid_key(self, scope_client, endpoint, key_ref, path, api_key):
        headers = {"X-API-Key": api_key} if api_key is not None else {}

        response = scope_client.get(path, headers=headers)

        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid or missing API key"}
        endpoint.assert_not_called()
        key_ref.update.assert_not_awaited()

    @pytest.mark.parametrize("reason", ["unknown", "revoked", "wrong-secret"])
    def test_rejects_unusable_key(
        self, scope_client, key_credentials, key_doc, key_lookup, key_ref, endpoint, reason
    ):
        _, full_key, _ = key_credentials
        if reason == "unknown":
            key_lookup.return_value = None
        elif reason == "revoked":
            key_doc.is_active = False
        else:
            key_doc.key_hash = "wrong-hash"

        response = scope_client.get("/scoped", headers={"X-API-Key": full_key})

        assert response.status_code == 401
        endpoint.assert_not_called()
        key_ref.update.assert_not_awaited()

    @pytest.mark.parametrize("is_admin", [False, True])
    @pytest.mark.parametrize("scopes", [[], ["other-scope"]])
    def test_rejects_missing_scope(
        self, scope_client, key_credentials, key_doc, endpoint, scopes, is_admin
    ):
        _, full_key, _ = key_credentials
        key_doc.scopes.extend(scopes)
        key_doc.is_admin = is_admin

        response = scope_client.get("/scoped", headers={"X-API-Key": full_key})

        assert response.status_code == 403
        assert response.json() == {"detail": "API key missing required scope: user-config"}
        endpoint.assert_not_called()

    def test_request_cannot_override_required_scope(
        self, scope_client, key_credentials, endpoint
    ):
        _, full_key, _ = key_credentials

        response = scope_client.get("/scoped?scope=default", headers={"X-API-Key": full_key})

        assert response.status_code == 403
        endpoint.assert_not_called()

    @pytest.mark.parametrize("is_admin", [False, True])
    def test_accepts_explicit_scope(self, scope_client, key_credentials, key_doc, is_admin):
        key_id, full_key, _ = key_credentials
        key_doc.scopes.append("user-config")
        key_doc.is_admin = is_admin

        response = scope_client.get("/scoped", headers={"X-API-Key": full_key})

        assert response.status_code == 200
        assert response.json() == {"key_id": key_id}

    @pytest.mark.parametrize("path", ["/default", "/implicit-default"])
    def test_default_accepts_legacy_key(self, scope_client, key_credentials, path):
        key_id, full_key, _ = key_credentials

        response = scope_client.get(path, headers={"X-API-Key": full_key})

        assert response.status_code == 200
        assert response.json() == {"key_id": key_id}

    @pytest.mark.parametrize("path", ["/combined", "/admin-scoped"])
    def test_combined_guards_authenticate_once_per_request(
        self, scope_client, key_credentials, key_doc, key_lookup, key_ref, path
    ):
        _, full_key, _ = key_credentials
        key_doc.scopes.append("user-config")
        key_doc.is_admin = True

        for request_count in (1, 2):
            response = scope_client.get(path, headers={"X-API-Key": full_key})

            assert response.status_code == 200
            assert key_lookup.await_count == request_count
            assert key_ref.update.await_count == request_count


class TestVerifyAdminApiKey:
    def test_raises_401_when_key_invalid(self, scope_client, endpoint):
        response = scope_client.get("/admin", headers={"X-API-Key": "gea_bad"})

        assert response.status_code == 401
        endpoint.assert_not_called()

    def test_raises_403_when_key_not_admin(self, scope_client, key_credentials, key_doc, endpoint):
        _, full_key, _ = key_credentials
        key_doc.scopes.extend(["admin", "user-config"])

        response = scope_client.get("/admin", headers={"X-API-Key": full_key})

        assert response.status_code == 403
        assert response.json() == {"detail": "Admin API key required"}
        endpoint.assert_not_called()

    def test_returns_key_id_when_admin(self, scope_client, key_credentials, key_doc):
        key_id, full_key, _ = key_credentials
        key_doc.is_admin = True

        response = scope_client.get("/admin", headers={"X-API-Key": full_key})

        assert response.status_code == 200
        assert response.json() == {"key_id": key_id}

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from .documents import ApiKeyDocument
from .main import app
from .security import verify_admin_api_key, verify_api_key


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


def _make_request() -> MagicMock:
    request = MagicMock()
    request.app.state.firestore = MagicMock()
    return request


def _admin_doc(is_admin: bool) -> ApiKeyDocument:
    return ApiKeyDocument(
        key_id="a1b2c3d4",
        key_hash="deadbeef",
        email="test@example.com",
        is_admin=is_admin,
    )


class TestVerifyAdminApiKey:
    @pytest.mark.asyncio
    async def test_raises_401_when_key_invalid(self):
        with patch("app.security.authenticate_api_key", new=AsyncMock(return_value=None)):
            with pytest.raises(HTTPException) as exc_info:
                await verify_admin_api_key(_make_request(), "gea_bad")
        assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED

    @pytest.mark.asyncio
    async def test_raises_403_when_key_not_admin(self):
        doc = _admin_doc(is_admin=False)
        with patch("app.security.authenticate_api_key", new=AsyncMock(return_value=doc)):
            with pytest.raises(HTTPException) as exc_info:
                await verify_admin_api_key(_make_request(), "gea_valid")
        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_returns_key_id_when_admin(self):
        doc = _admin_doc(is_admin=True)
        with patch("app.security.authenticate_api_key", new=AsyncMock(return_value=doc)):
            result = await verify_admin_api_key(_make_request(), "gea_valid")
        assert result == "a1b2c3d4"

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from .main import app
from .security import verify_api_key


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
        assert response.json() == {"status": "ok"}

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from .main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def api_key():
    return "test-api-key-12345"


@pytest.fixture
def client_with_api_key(api_key):
    with patch.dict(os.environ, {"API_KEY": api_key}):
        from importlib import reload

        from . import security

        reload(security)
        yield TestClient(app), api_key


class TestRootEndpointAuth:
    def test_root_returns_401_without_api_key(self, client_with_api_key):
        client, _ = client_with_api_key
        response = client.get("/")
        assert response.status_code == 401

    def test_root_returns_401_with_invalid_api_key(self, client_with_api_key):
        client, _ = client_with_api_key
        response = client.get("/", headers={"X-API-Key": "invalid-key"})
        assert response.status_code == 401

    def test_root_returns_401_response_body(self, client_with_api_key):
        client, _ = client_with_api_key
        response = client.get("/")
        assert response.json() == {"detail": "Invalid or missing API key"}

    def test_root_returns_200_with_valid_api_key(self, client_with_api_key):
        client, api_key = client_with_api_key
        response = client.get("/", headers={"X-API-Key": api_key})
        assert response.status_code == 200
        assert response.json() == {"message": "Green Earth API"}


class TestHealthEndpointNoAuth:
    def test_health_returns_200_without_api_key(self, client_with_api_key):
        client, _ = client_with_api_key
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self, client_with_api_key):
        client, _ = client_with_api_key
        response = client.get("/health")
        assert response.json() == {"status": "ok"}

"""Tests for /api/feeds/llm-query-vectors/{fit,current} (api#492)."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..documents import LlmQueryVectorDocument
from ..lib.firebase_auth import verify_firebase_auth
from ..lib.llm_query_vector_fit import FitResult, PoolTooSmallError
from ..main import app

PATH = "/api/feeds/llm-query-vectors/fit"
CURRENT_PATH = "/api/feeds/llm-query-vectors/current"


@pytest.fixture
def client() -> Generator[TestClient]:
    """Client with Firebase auth bypassed and Firestore + ES stubbed on app state."""
    app.dependency_overrides[verify_firebase_auth] = lambda: "test-user"
    app.state.firestore = MagicMock()
    app.state.es = MagicMock()
    yield TestClient(app)
    app.dependency_overrides.pop(verify_firebase_auth, None)


def _fit_result() -> FitResult:
    return FitResult(
        query_vector=[0.1, 0.2],
        keywords=["science", "hope"],
        n_pool=500,
        n_keyword_posts=80,
        n_random_posts=120,
        n_scored=78,
        n_cancelled=2,
        n_failed=0,
        train_r2=0.45,
        duration_s=9.8,
        cost_usd=0.13,
        input_tokens=1000,
        output_tokens=200,
    )


@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_stores_vector_for_token_user(mock_fit, mock_add, client):
    mock_fit.return_value = _fit_result()
    mock_add.return_value = LlmQueryVectorDocument(
        prompt_key="v1",
        user_did="did:plc:test-user",
        query_vector=[0.1, 0.2],
        prompt="hopeful science",
    )

    response = client.post(PATH, json={"prompt": "hopeful science"})

    assert response.status_code == 200
    assert mock_add.await_args.args[1] == "did:plc:test-user"
    body = response.json()
    assert body["user_did"] == "did:plc:test-user"
    assert body["keywords"] == ["science", "hope"]
    assert body["train_r2"] == 0.45


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_rejects_blank_prompt(mock_fit, client):
    response = client.post(PATH, json={"prompt": "   "})

    assert response.status_code == 422
    mock_fit.assert_not_awaited()


@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_reports_pool_too_small(mock_fit, mock_add, client):
    mock_fit.side_effect = PoolTooSmallError("only 12 posts match")

    response = client.post(PATH, json={"prompt": "very niche topic"})

    assert response.status_code == 422
    assert "12 posts" in response.json()["detail"]
    mock_add.assert_not_awaited()


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_requires_login(mock_fit):
    app.dependency_overrides.pop(verify_firebase_auth, None)
    response = TestClient(app).post(PATH, json={"prompt": "anything"})

    assert response.status_code == 401
    mock_fit.assert_not_awaited()


@patch("app.routers.llm_query_vectors.get_latest_llm_query_vector", new_callable=AsyncMock)
def test_current_returns_newest_prompt_without_vector(mock_latest, client):
    mock_latest.return_value = LlmQueryVectorDocument(
        prompt_key="v2",
        user_did="did:plc:test-user",
        query_vector=[0.1, 0.2],
        prompt="hopeful science",
    )

    response = client.get(CURRENT_PATH)

    assert response.status_code == 200
    assert mock_latest.await_args.args[1] == "did:plc:test-user"
    body = response.json()
    assert body["prompt"] == "hopeful science"
    assert body["prompt_key"] == "v2"
    assert "query_vector" not in body


@patch("app.routers.llm_query_vectors.get_latest_llm_query_vector", new_callable=AsyncMock)
def test_current_is_204_when_nothing_fitted(mock_latest, client):
    mock_latest.return_value = None

    response = client.get(CURRENT_PATH)

    assert response.status_code == 204
    assert response.content == b""

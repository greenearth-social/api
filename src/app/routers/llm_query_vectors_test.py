"""Tests for /api/feeds/llm-query-vectors/{fit,current} (api#492)."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..documents import LlmQueryVectorDocument
from ..lib.firebase_auth import verify_firebase_auth
from ..lib.llm_query_vector_fit import FitError, FitResult, PoolTooSmallError
from ..lib.llm_query_vector_limits import FitQuotaDecision
from ..main import app

PATH = "/api/feeds/llm-query-vectors/fit"
CURRENT_PATH = "/api/feeds/llm-query-vectors/current"


@pytest.fixture(autouse=True)
def quota():
    with patch(
        "app.routers.llm_query_vectors.reserve_fit_quota",
        new_callable=AsyncMock,
        return_value=FitQuotaDecision(),
    ) as reserve:
        yield reserve


@pytest.fixture(autouse=True)
def metrics():
    with patch("app.routers.llm_query_vectors.get_metric_collector") as get_collector:
        yield get_collector.return_value


@pytest.fixture
def client(monkeypatch) -> Generator[TestClient]:
    """Client with Firebase auth bypassed, the llm-cg gate open, and Firestore + ES
    stubbed on app state."""
    monkeypatch.setenv("GE_LLM_CG_OPEN", "true")
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
def test_fit_stores_vector_for_token_user(mock_fit, mock_add, client, quota, metrics):
    mock_fit.return_value = _fit_result()
    mock_add.return_value = LlmQueryVectorDocument(
        prompt_key="v1",
        user_did="did:plc:test-user",
        query_vector=[0.1, 0.2],
        prompt="hopeful science",
    )

    response = client.post(PATH, json={"prompt": "hopeful science"})

    assert response.status_code == 200
    quota.assert_awaited_once_with(app.state.firestore, "did:plc:test-user")
    metrics.record.assert_not_called()
    assert mock_add.await_args.args[1] == "did:plc:test-user"
    body = response.json()
    assert body["user_did"] == "did:plc:test-user"
    assert body["keywords"] == ["science", "hope"]
    assert body["train_r2"] == 0.45


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
@pytest.mark.parametrize("prompt", ["   ", "", "x" * 2001])
def test_fit_rejects_invalid_prompt(mock_fit, client, quota, prompt):
    response = client.post(PATH, json={"prompt": prompt})

    assert response.status_code == 422
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()


@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_reports_pool_too_small(mock_fit, mock_add, client, quota):
    mock_fit.side_effect = PoolTooSmallError("only 12 posts match")

    response = client.post(PATH, json={"prompt": "very niche topic"})

    assert response.status_code == 422
    assert "12 posts" in response.json()["detail"]
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()


@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_is_504_and_stores_nothing_past_the_deadline(
    mock_fit, mock_add, client, monkeypatch, quota
):
    monkeypatch.setattr("app.routers.llm_query_vectors.FIT_TIMEOUT_S", 0.01)

    async def slow_fit(*_args):
        await asyncio.sleep(1)
        return _fit_result()

    mock_fit.side_effect = slow_fit

    response = client.post(PATH, json={"prompt": "hopeful science"})

    assert response.status_code == 504
    assert "nothing stored" in response.json()["detail"]
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_requires_login(mock_fit, quota):
    app.dependency_overrides.pop(verify_firebase_auth, None)
    response = TestClient(app).post(PATH, json={"prompt": "anything"})

    assert response.status_code == 401
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()


@patch("app.routers.llm_query_vectors.get_latest_llm_query_vector", new_callable=AsyncMock)
def test_current_returns_newest_prompt_without_vector(mock_latest, client, quota):
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
    quota.assert_not_awaited()


@patch("app.routers.llm_query_vectors.get_latest_llm_query_vector", new_callable=AsyncMock)
def test_current_is_204_when_nothing_fitted(mock_latest, client):
    mock_latest.return_value = None

    response = client.get(CURRENT_PATH)

    assert response.status_code == 204
    assert response.content == b""


@patch("app.routers.llm_query_vectors.llm_cg_enabled", return_value=False)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_is_403_when_flag_off(mock_fit, mock_enabled, client, quota):
    response = client.post(PATH, json={"prompt": "hopeful science"})
    assert response.status_code == 403
    quota.assert_not_awaited()
    mock_fit.assert_not_called()
    mock_enabled.assert_called_once()
    assert mock_enabled.call_args.args[1] == "did:plc:test-user"


@patch("app.routers.llm_query_vectors.llm_cg_enabled", return_value=False)
@patch("app.routers.llm_query_vectors.get_latest_llm_query_vector", new_callable=AsyncMock)
def test_current_is_403_when_flag_off(mock_latest, mock_enabled, client):
    response = client.get(CURRENT_PATH)
    assert response.status_code == 403
    mock_latest.assert_not_called()


@pytest.mark.parametrize("scopes", [("user",), ("global",), ("user", "global")])
@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_rejects_full_quota(mock_fit, mock_add, client, quota, metrics, scopes):
    now = datetime(2026, 10, 7, 12, tzinfo=UTC)
    retry_at = now + timedelta(seconds=10, microseconds=1)
    quota.return_value = FitQuotaDecision(exhausted_scopes=scopes, retry_at=retry_at)

    with patch("app.routers.llm_query_vectors.datetime") as clock:
        clock.now.return_value = now
        response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 429
    detail = response.json()["detail"]
    assert detail["code"] == "llm_query_vector_rate_limited"
    assert detail["scopes"] == list(scopes)
    assert detail["message"]
    assert datetime.fromisoformat(detail["retry_at"]) == retry_at
    assert detail["retry_after_seconds"] == 11
    assert response.headers["Retry-After"] == "11"
    mock_fit.assert_not_awaited()
    mock_add.assert_not_awaited()
    metrics.record.assert_not_called()


@pytest.mark.parametrize("failure", [RuntimeError("Firestore down"), ValueError("retry limit")])
@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_quota_failure_is_503(mock_fit, mock_add, client, quota, metrics, failure):
    quota.side_effect = failure

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 503
    assert response.json() == {"detail": "Firestore unavailable"}
    mock_fit.assert_not_awaited()
    mock_add.assert_not_awaited()
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_without_firestore_does_not_reserve(mock_fit, client, quota, monkeypatch):
    monkeypatch.setattr(app.state, "firestore", None)

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 503
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()


@pytest.mark.parametrize("scopes", [("user",), ("global",), ("user", "global")])
@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_records_newly_full_caps_before_model_work(
    mock_fit, mock_add, client, quota, metrics, scopes
):
    quota.return_value = FitQuotaDecision(reached_scopes=scopes)

    async def fit(*_args):
        quota.assert_awaited_once()
        assert [call.kwargs for call in metrics.record.call_args_list] == [
            {"scope": scope} for scope in scopes
        ]
        # Failed work still consumed the reserved slot and reached these caps.
        raise FitError("model failed")

    mock_fit.side_effect = fit

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 502
    assert metrics.record.call_count == len(scopes)
    for call in metrics.record.call_args_list:
        assert call.args == ("llm_query_vector.fit.cap_reached_count", 1)
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()


@patch("app.routers.llm_query_vectors.add_llm_query_vector", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_storage_failure_keeps_reservation(mock_fit, mock_add, client, quota, metrics):
    quota.return_value = FitQuotaDecision(reached_scopes=("user",))
    mock_fit.return_value = _fit_result()
    mock_add.side_effect = RuntimeError("storage down")

    with pytest.raises(RuntimeError, match="storage down"):
        client.post(PATH, json={"prompt": "science"})

    quota.assert_awaited_once()
    metrics.record.assert_called_once_with(
        "llm_query_vector.fit.cap_reached_count", 1, scope="user"
    )


def test_fit_rate_limit_response_is_documented():
    responses = app.openapi()["paths"][PATH]["post"]["responses"]
    assert responses["429"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/QueryVectorRateLimitResponse"
    }
    assert responses["429"]["headers"]["Retry-After"]["schema"]["type"] == "integer"

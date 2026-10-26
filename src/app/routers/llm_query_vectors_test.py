"""Tests for /api/feeds/llm-query-vectors/{fit,current} (api#492)."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from ..documents import FeedPreferencesDocument, LlmQueryVectorDocument, SourceWeightsDocument
from ..lib.firebase_auth import verify_firebase_auth
from ..lib.firestore_rate_limiter import QuotaDecision
from ..lib.llm_prompt_state import PromptChangedError
from ..lib.llm_query_vector_fit import (
    FitError,
    FitResult,
    PoolTooSmallError,
    PromptNotAcceptedError,
)
from ..main import app
from .llm_query_vectors import QueryVectorFitRequest, fit_llm_query_vector

PATH = "/api/feeds/llm-query-vectors/fit"
CURRENT_PATH = "/api/feeds/llm-query-vectors/current"


@pytest.fixture(autouse=True)
def quota():
    with patch("app.routers.llm_query_vectors.FIT_RATE_LIMITER", autospec=True) as limiter:
        limiter.reserve.return_value = QuotaDecision()
        yield limiter.reserve


@pytest.fixture(autouse=True)
def prompt_revision():
    with patch(
        "app.routers.llm_query_vectors.get_llm_prompt_revision",
        new_callable=AsyncMock,
        return_value=0,
    ) as revision:
        yield revision


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


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_stores_vector_for_token_user(
    mock_fit, mock_add, client, quota, metrics, prompt_revision
):
    mock_fit.return_value = _fit_result()
    stored = LlmQueryVectorDocument(
        prompt_key="v1",
        user_did="did:plc:test-user",
        query_vector=[0.1, 0.2],
        prompt="hopeful science",
    )

    async def reserve(*_args):
        prompt_revision.assert_awaited_once_with(app.state.firestore, "did:plc:test-user")
        metrics.record.assert_called_once_with("llm_query_vector.fit.attempted_count", 1)
        return QuotaDecision()

    async def store(*_args, expected_revision):
        assert expected_revision == 0
        mock_fit.assert_awaited_once()
        metrics.record.assert_called_once_with("llm_query_vector.fit.attempted_count", 1)
        return stored

    quota.side_effect = reserve
    mock_add.side_effect = store

    response = client.post(PATH, json={"prompt": "hopeful science"})

    assert response.status_code == 200
    quota.assert_awaited_once_with(app.state.firestore, "did:plc:test-user")
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.success_count", 1),
    ]
    assert mock_add.await_args.args[1] == "did:plc:test-user"
    body = response.json()
    assert body["user_did"] == "did:plc:test-user"
    assert body["keywords"] == ["science", "hope"]
    assert body["train_r2"] == 0.45


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
@pytest.mark.parametrize("prompt", ["   ", "", "x" * 2001])
def test_fit_rejects_invalid_prompt(mock_fit, client, quota, metrics, prompt):
    response = client.post(PATH, json={"prompt": prompt})

    assert response.status_code == 422
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_reports_pool_too_small(mock_fit, mock_add, client, quota, metrics):
    mock_fit.side_effect = PoolTooSmallError("only 12 posts match")

    response = client.post(PATH, json={"prompt": "very niche topic"})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "llm_query_vector_pool_too_small"
    assert "broader topic" in response.json()["detail"]["message"]
    assert "12 posts" not in response.text
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="pool_too_small"),
    ]


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_reports_prompt_refusal_and_logs_error_type(
    mock_fit, mock_add, client, quota, metrics, caplog
):
    mock_fit.side_effect = PromptNotAcceptedError("provider-request-123")

    response = client.post(PATH, json={"prompt": "private prompt text"})

    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "llm_query_vector_prompt_not_accepted",
        "message": "Prompt not accepted. Please rephrase your request and try again.",
    }
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="prompt_not_accepted"),
    ]
    assert "error_type=PromptNotAcceptedError" in caplog.text
    assert "provider_request_id=provider-request-123" in caplog.text
    assert f"request_id={response.headers['x-request-id']}" in caplog.text
    assert "private prompt text" not in caplog.text


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_is_504_and_stores_nothing_past_the_deadline(
    mock_fit, mock_add, client, monkeypatch, quota, metrics
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
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="timeout"),
    ]


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_requires_login(mock_fit, quota, metrics):
    app.dependency_overrides.pop(verify_firebase_auth, None)
    response = TestClient(app).post(PATH, json={"prompt": "anything"})

    assert response.status_code == 401
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.get_current_llm_query_vector", new_callable=AsyncMock)
def test_current_returns_newest_prompt_without_vector(mock_latest, client, quota, metrics):
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
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.get_current_llm_query_vector", new_callable=AsyncMock)
def test_current_is_204_when_nothing_fitted(mock_latest, client, metrics):
    mock_latest.return_value = None

    response = client.get(CURRENT_PATH)

    assert response.status_code == 204
    assert response.content == b""
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.llm_cg_enabled", return_value=False)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_is_403_when_flag_off(mock_fit, mock_enabled, client, quota, metrics):
    response = client.post(PATH, json={"prompt": "hopeful science"})
    assert response.status_code == 403
    quota.assert_not_awaited()
    mock_fit.assert_not_called()
    mock_enabled.assert_called_once()
    assert mock_enabled.call_args.args[1] == "did:plc:test-user"
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.llm_cg_enabled", return_value=False)
@patch("app.routers.llm_query_vectors.get_current_llm_query_vector", new_callable=AsyncMock)
def test_current_is_403_when_flag_off(mock_latest, mock_enabled, client, metrics):
    response = client.get(CURRENT_PATH)
    assert response.status_code == 403
    mock_latest.assert_not_called()
    metrics.record.assert_not_called()


@pytest.mark.parametrize("scopes", [("user",), ("global",), ("user", "global")])
@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_rejects_full_quota(mock_fit, mock_add, client, quota, metrics, scopes):
    now = datetime(2026, 10, 7, 12, tzinfo=UTC)
    retry_at = now + timedelta(seconds=10, microseconds=1)
    quota.return_value = QuotaDecision(exhausted_scopes=scopes, retry_at=retry_at)

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
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call(
            "llm_query_vector.fit.failed_limited_count",
            1,
            scope="both" if len(scopes) == 2 else scopes[0],
        ),
    ]


@pytest.mark.parametrize("failure", [RuntimeError("Firestore down"), ValueError("retry limit")])
@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_quota_failure_is_503(mock_fit, mock_add, client, quota, metrics, failure):
    quota.side_effect = failure

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Unable to verify or reserve query-vector quota. Please try again."
    }
    mock_fit.assert_not_awaited()
    mock_add.assert_not_awaited()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="quota_store"),
    ]


@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_without_firestore_does_not_reserve(mock_fit, client, quota, metrics, monkeypatch):
    monkeypatch.setattr(app.state, "firestore", None)

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 503
    quota.assert_not_awaited()
    mock_fit.assert_not_awaited()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="quota_store"),
    ]


def test_fit_validates_prompt_before_firestore_availability(client, quota, metrics, monkeypatch):
    monkeypatch.setattr(app.state, "firestore", None)

    response = client.post(PATH, json={"prompt": "   "})

    assert response.status_code == 422
    quota.assert_not_awaited()
    metrics.record.assert_not_called()


@pytest.mark.parametrize("scopes", [("user",), ("global",), ("user", "global")])
@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_records_newly_full_caps_before_model_work(
    mock_fit, mock_add, client, quota, metrics, scopes
):
    quota.return_value = QuotaDecision(reached_scopes=scopes)

    async def fit(*_args):
        quota.assert_awaited_once()
        assert metrics.record.call_args_list == [
            call("llm_query_vector.fit.attempted_count", 1),
            *[call("llm_query_vector.fit.cap_reached_count", 1, scope=scope) for scope in scopes],
        ]
        # Failed work still consumed the reserved slot and reached these caps.
        raise FitError("model failed")

    mock_fit.side_effect = fit

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 502
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        *[call("llm_query_vector.fit.cap_reached_count", 1, scope=scope) for scope in scopes],
        call("llm_query_vector.fit.failed_exc_count", 1, reason="upstream"),
    ]
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_storage_failure_keeps_reservation(mock_fit, mock_add, client, quota, metrics):
    quota.return_value = QuotaDecision(reached_scopes=("user",))
    mock_fit.return_value = _fit_result()
    mock_add.side_effect = RuntimeError("storage down")

    with pytest.raises(RuntimeError, match="storage down"):
        client.post(PATH, json={"prompt": "science"})

    quota.assert_awaited_once()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.cap_reached_count", 1, scope="user"),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="storage"),
    ]


@pytest.mark.parametrize(
    ("failure", "detail"),
    [
        (FitError("model failed"), "model failed"),
        (RuntimeError("connection closed"), "Elasticsearch or model request failed"),
    ],
)
@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_records_upstream_failures(mock_fit, mock_add, client, quota, metrics, failure, detail):
    mock_fit.side_effect = failure

    response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 502
    assert response.json() == {"detail": detail}
    quota.assert_awaited_once()
    mock_add.assert_not_awaited()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="upstream"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["quota", "fit", "storage"])
@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
async def test_fit_records_and_propagates_cancellation(
    mock_fit, mock_add, client, quota, metrics, stage
):
    mock_fit.return_value = _fit_result()
    cancelled_operation = {"quota": quota, "fit": mock_fit, "storage": mock_add}[stage]
    cancelled_operation.side_effect = asyncio.CancelledError("request cancelled")

    with pytest.raises(asyncio.CancelledError, match="request cancelled"):
        await fit_llm_query_vector(
            QueryVectorFitRequest(prompt="science"),
            Request({"type": "http", "app": app}),
            "did:plc:test-user",
        )

    quota.assert_awaited_once()
    if stage == "quota":
        mock_fit.assert_not_awaited()
    else:
        mock_fit.assert_awaited_once()
    if stage != "storage":
        mock_add.assert_not_awaited()
    else:
        mock_add.assert_awaited_once()
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="cancelled"),
    ]


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_fit_without_metric_collector_still_succeeds(mock_fit, mock_add, client, quota, metrics):
    quota.return_value = QuotaDecision(reached_scopes=("user", "global"))
    mock_fit.return_value = _fit_result()
    mock_add.return_value = LlmQueryVectorDocument(
        prompt_key="v1",
        user_did="did:plc:test-user",
        query_vector=[0.1, 0.2],
        prompt="science",
    )

    with patch("app.routers.llm_query_vectors.get_metric_collector", return_value=None):
        response = client.post(PATH, json={"prompt": "science"})

    assert response.status_code == 200
    quota.assert_awaited_once()
    mock_fit.assert_awaited_once()
    mock_add.assert_awaited_once()
    metrics.record.assert_not_called()


def test_fit_rate_limit_response_is_documented():
    responses = app.openapi()["paths"][PATH]["post"]["responses"]
    assert responses["429"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/QueryVectorRateLimitResponse"
    }
    assert responses["429"]["headers"]["Retry-After"]["schema"]["type"] == "integer"
    assert responses["422"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/QueryVectorPromptErrorResponse"
    }


@patch("app.routers.llm_query_vectors.remove_llm_prompt", new_callable=AsyncMock)
def test_remove_returns_saved_preferences_without_model_work_or_quota(
    mock_remove, client, quota, metrics
):
    mock_remove.return_value = FeedPreferencesDocument(
        source_weights=SourceWeightsDocument(
            following=0.3, network_likes=0.2, authors_topics=0.25, popular=0.25, llm=0
        ),
        freshness=3,
    )
    response = client.delete(CURRENT_PATH, params={"prompt_key": "v1"})
    assert response.status_code == 200
    assert response.json() == {
        "source_weights": {
            "following": 0.3,
            "network_likes": 0.2,
            "authors_topics": 0.25,
            "popular": 0.25,
            "llm": 0,
        },
        "freshness": 3,
        "purpose": None,
        "politics": None,
        "author_penalty": None,
        "topic_penalty": None,
    }
    mock_remove.assert_awaited_once_with(app.state.firestore, "did:plc:test-user", "v1")
    quota.assert_not_awaited()
    metrics.record.assert_not_called()


@patch("app.routers.llm_query_vectors.remove_llm_prompt", new_callable=AsyncMock)
@pytest.mark.parametrize(
    "failure,status", [(PromptChangedError(), 409), (RuntimeError("storage detail"), 503)]
)
def test_remove_handles_conflicts_and_storage_failures(mock_remove, client, failure, status):
    mock_remove.side_effect = failure
    response = client.delete(CURRENT_PATH, params={"prompt_key": "v1"})
    assert response.status_code == status
    assert "storage detail" not in response.text
    if status == 409:
        assert response.json()["detail"]["code"] == "llm_query_vector_prompt_changed"


@patch("app.routers.llm_query_vectors.remove_llm_prompt", new_callable=AsyncMock)
def test_remove_requires_a_prompt_key(mock_remove, client):
    assert client.delete(CURRENT_PATH).status_code == 422
    mock_remove.assert_not_awaited()


@patch("app.routers.llm_query_vectors.llm_cg_enabled", return_value=False)
@patch("app.routers.llm_query_vectors.remove_llm_prompt", new_callable=AsyncMock)
def test_remove_requires_feature_access(mock_remove, _flag, client):
    assert client.delete(CURRENT_PATH, params={"prompt_key": "v1"}).status_code == 403
    mock_remove.assert_not_awaited()


@patch("app.routers.llm_query_vectors.remove_llm_prompt", new_callable=AsyncMock)
def test_remove_requires_firestore(mock_remove, client, monkeypatch):
    monkeypatch.setattr(app.state, "firestore", None)
    assert client.delete(CURRENT_PATH, params={"prompt_key": "v1"}).status_code == 503
    mock_remove.assert_not_awaited()


@patch("app.routers.llm_query_vectors.save_fitted_llm_prompt", new_callable=AsyncMock)
@patch("app.routers.llm_query_vectors.fit_query_vector", new_callable=AsyncMock)
def test_superseded_fit_returns_conflict_and_records_one_outcome(
    mock_fit, mock_save, client, metrics
):
    mock_fit.return_value = _fit_result()
    mock_save.side_effect = PromptChangedError()
    response = client.post(PATH, json={"prompt": "science"})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "llm_query_vector_prompt_changed"
    assert metrics.record.call_args_list == [
        call("llm_query_vector.fit.attempted_count", 1),
        call("llm_query_vector.fit.failed_exc_count", 1, reason="prompt_changed"),
    ]

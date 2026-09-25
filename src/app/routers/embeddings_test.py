"""HTTP contract, production history preparation, and safe embedding export failures."""

import asyncio
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from elastic_transport import ConnectionTimeout
from fastapi.testclient import TestClient

from .. import security
from ..lib import inference, user_history_cache
from ..lib.embeddings import MINILM_L12_EMBEDDING_DIM, MINILM_L12_EMBEDDING_FIELD
from ..lib.user_history_cache import UserHistory, UserHistoryItem
from ..main import app
from ..security import verify_api_key
from . import embeddings

USER_UUID = "1affd684bc7f45f895e488f83dd0a2fa"
POST_UUID = "9b946f280fd84899a7f82246fbc34d17"
VECTOR = [1.0] * MINILM_L12_EMBEDDING_DIM
DID = "did:plc:viewer"


def _history(vector=VECTOR):
    return UserHistory(
        items=[
            UserHistoryItem(
                at_uri="at://post/1",
                liked_at="2026-09-21T12:00:00Z",
                embedding=vector,
                author_did="did:plc:author",
            )
        ]
    )


def _prediction(**changes):
    return {
        "outputs": [[0.6, 0.8]],
        "model_type": "user-tower",
        "model_uuid": USER_UUID,
        "paired_post_model_uuid": POST_UUID,
        **changes,
    }


def _mock_prediction_request() -> AsyncMock:
    """The client fixture replaces the shared HTTP client's post method."""
    return cast(AsyncMock, inference.get_http_client().post)


def _search(hits, **changes):
    return {
        "timed_out": False,
        "_shards": {"total": 2, "successful": 2, "failed": 0},
        "hits": {"total": {"value": len(hits), "relation": "eq"}, "hits": hits},
        **changes,
    }


@pytest.fixture
def client(monkeypatch):
    es = Mock()
    es.info = AsyncMock(return_value={"cluster_uuid": "cluster-1"})
    monkeypatch.setattr(app.state, "es", es, raising=False)
    monkeypatch.setattr(app.state, "firestore", object(), raising=False)
    monkeypatch.setattr(
        embeddings, "get_inference_settings", lambda: ("https://inference", "secret")
    )
    monkeypatch.setattr(
        embeddings, "fetch_user_history_features", AsyncMock(return_value=_history())
    )
    upstream = Mock()
    upstream.post = AsyncMock(return_value=httpx.Response(200, json=_prediction()))
    monkeypatch.setattr(inference, "get_http_client", lambda: upstream)
    return TestClient(app)


def test_export_propagates_actual_pair_source_and_history_without_candidate_search(
    client, monkeypatch
):
    monkeypatch.setattr(
        inference,
        "get_cached_post_tower_uuid",
        AsyncMock(side_effect=AssertionError("Readiness must not supply the paired UUID")),
    )
    app.state.es.search = AsyncMock(side_effect=AssertionError("No candidate search"))
    response = client.post(
        "/embeddings/user", json={"user_did": DID}, headers={"x-request-id": "embedding-test"}
    )
    assert response.status_code == 200
    assert response.json() == {
        "user_did": DID,
        "status": "ok",
        "embedding": [0.6, 0.8],
        "user_model_uuid": USER_UUID,
        "post_model_uuid": POST_UUID,
        "dimension": 2,
        "history_like_count": 1,
        "history_embedding_count": 1,
        "es_cluster_uuid": "cluster-1",
        "likes_index": "likes",
    }
    cast(AsyncMock, embeddings.fetch_user_history_features).assert_awaited_once_with(
        app.state.es, DID
    )
    prediction = _mock_prediction_request()
    prediction.assert_awaited_once()
    assert prediction.call_args.kwargs["json"] == {
        "history_embeddings": [VECTOR],
        "history_author_dids": ["did:plc:author"],
    }
    assert "timeout" not in prediction.call_args.kwargs
    assert (
        prediction.call_args.kwargs["headers"]["x-request-id"] == response.headers["x-request-id"]
    )
    app.state.es.search.assert_not_called()


@pytest.mark.asyncio
async def test_cluster_identity_shared_across_concurrent_users(client):
    async def lookup():
        await asyncio.sleep(0)
        return {"cluster_uuid": "cluster-1"}

    app.state.es.info.side_effect = lookup
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as concurrent_client:
        responses = await asyncio.gather(
            *(
                concurrent_client.post("/embeddings/user", json={"user_did": f"did:plc:user{i}"})
                for i in range(4)
            )
        )
        responses.append(
            await concurrent_client.post("/embeddings/user", json={"user_did": DID})
        )
    assert all(response.status_code == 200 for response in responses)
    assert all(response.json()["es_cluster_uuid"] == "cluster-1" for response in responses)
    app.state.es.info.assert_awaited_once()


def test_replacing_es_client_refreshes_cluster_identity(client, monkeypatch):
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.json()["es_cluster_uuid"] == "cluster-1"
    replacement = Mock(info=AsyncMock(return_value={"cluster_uuid": "cluster-2"}))
    monkeypatch.setattr(app.state, "es", replacement)
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 200
    assert response.json()["es_cluster_uuid"] == "cluster-2"
    replacement.info.assert_awaited_once()


@pytest.mark.parametrize(
    "history,reason",
    [
        (UserHistory(items=[]), "no_likes"),
        (_history(None), "no_embedded_history"),
    ],
)
def test_missing_history_skips_inference_and_excludes_vectors(client, monkeypatch, history, reason):
    monkeypatch.setattr(
        embeddings, "fetch_user_history_features", AsyncMock(return_value=history)
    )
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "skipped"
    assert body["reason"] == reason
    assert body["history_like_count"] == len(history.items)
    assert body["history_embedding_count"] == 0
    assert body["es_cluster_uuid"] == "cluster-1"
    assert not {"embedding", "dimension", "user_model_uuid", "post_model_uuid"} & body.keys()
    _mock_prediction_request().assert_not_called()


def test_authentication_uses_existing_api_key_header(client, monkeypatch):
    app.dependency_overrides.pop(verify_api_key)
    authenticate = AsyncMock(return_value=None)
    monkeypatch.setattr(security, "authenticate_api_key", authenticate)
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 401
    app.state.es.info.assert_not_called()
    authenticate.return_value = SimpleNamespace(key_id="valid")
    response = client.post(
        "/embeddings/user", json={"user_did": DID}, headers={"X-API-Key": "a-key"}
    )
    assert response.status_code == 200
    assert authenticate.call_args.args[1] == "a-key"


@pytest.mark.parametrize(
    "did", ["", "hello", "did:plc:", "did:plc:bad name", " did:plc:a", "did:UPPER:x"]
)
def test_invalid_did_rejected_before_upstream_calls(client, did):
    assert client.post("/embeddings/user", json={"user_did": did}).status_code == 422
    app.state.es.info.assert_not_called()


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"outputs": []}, "invalid_inference_response"),
        ({"outputs": [[]]}, "invalid_inference_response"),
        ({"outputs": [[True, 1]]}, "invalid_inference_response"),
        ({"outputs": [["1", 2]]}, "invalid_inference_response"),
        ({"outputs": [[1, 2], [3, 4]]}, "invalid_inference_response"),
        ({"model_type": "post-tower"}, "invalid_inference_response"),
        ({"model_uuid": "invalid"}, "model_metadata_missing"),
        ({"model_uuid": "0" * 32}, "model_metadata_missing"),
        ({"paired_post_model_uuid": None}, "model_metadata_missing"),
    ],
)
def test_invalid_predictions_return_sanitized_error(client, changes, code):
    _mock_prediction_request().return_value = httpx.Response(200, json=_prediction(**changes))
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 502
    assert response.json() == {"detail": {"code": code}}


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e9999"])
def test_nonfinite_vectors_rejected(client, value):
    import json

    body = json.dumps(_prediction()).replace("[0.6, 0.8]", f"[{value}, 1]")
    _mock_prediction_request().return_value = httpx.Response(200, content=body)
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "invalid_inference_response"


@pytest.mark.parametrize(
    "status,code,error_class",
    [
        (401, "upstream_error", "RuntimeError"),
        (403, "upstream_error", "RuntimeError"),
        (404, "upstream_error", "RuntimeError"),
        (302, "invalid_inference_response", "InferenceResponseFormatError"),
        (500, "upstream_error", "RuntimeError"),
        (429, "upstream_error", "RuntimeError"),
    ],
)
def test_upstream_http_errors_use_existing_inference_handling(
    client, caplog, status, code, error_class
):
    _mock_prediction_request().return_value = httpx.Response(
        status, text="secret-upstream-body"
    )
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == code
    assert "secret-upstream-body" not in response.text
    assert f"stage=inference exception_class={error_class}" in caplog.text
    if status >= 400:
        assert f"user-tower predict failed status={status}" in caplog.text


@pytest.mark.parametrize(
    "exception,status,code",
    [
        (RuntimeError("secret-body"), 502, "upstream_error"),
        (TimeoutError("secret-body"), 504, "upstream_timeout"),
        (ConnectionTimeout("secret-body"), 504, "upstream_timeout"),
        (httpx.ReadTimeout("secret-body"), 504, "upstream_timeout"),
    ],
)
def test_history_failures_prevent_inference(client, monkeypatch, caplog, exception, status, code):
    monkeypatch.setattr(
        embeddings, "fetch_user_history_features", AsyncMock(side_effect=exception)
    )
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert "secret-body" not in response.text + caplog.text
    assert f"stage=history exception_class={type(exception).__name__}" in caplog.text
    _mock_prediction_request().assert_not_called()


@pytest.mark.parametrize("stage", ["source_identity", "history", "inference"])
def test_overall_deadline_covers_all_stages(client, monkeypatch, stage):
    monkeypatch.setattr(embeddings, "REQUEST_TIMEOUT_SECONDS", 0.01)

    async def slow_operation(*args, **kwargs):
        await asyncio.sleep(10)

    if stage == "source_identity":
        app.state.es.info.side_effect = slow_operation
    elif stage == "history":
        cast(AsyncMock, embeddings.fetch_user_history_features).side_effect = slow_operation
    else:
        _mock_prediction_request().side_effect = slow_operation
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 504
    assert response.json()["detail"]["code"] == "upstream_timeout"
    if stage != "inference":
        _mock_prediction_request().assert_not_called()


def test_inference_client_timeout_returns_504(client):
    _mock_prediction_request().side_effect = httpx.ReadTimeout("inference timed out")
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 504
    assert response.json()["detail"]["code"] == "upstream_timeout"


def test_missing_inference_configuration_is_explicit(client, monkeypatch):
    monkeypatch.setattr(
        embeddings, "get_inference_settings", Mock(side_effect=RuntimeError("missing"))
    )
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "inference_not_configured"
    app.state.es.info.assert_not_called()


@pytest.mark.parametrize("cluster_uuid", [None, "", " ", "_na_", 12])
def test_missing_cluster_identity_is_not_exported(client, cluster_uuid):
    app.state.es.info.return_value = {"cluster_uuid": cluster_uuid}
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "upstream_error"
    _mock_prediction_request().assert_not_called()
    app.state.es.info.return_value = {"cluster_uuid": "cluster-1"}
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 200
    assert response.json()["es_cluster_uuid"] == "cluster-1"
    assert app.state.es.info.await_count == 2


@pytest.mark.parametrize("vector", [[0.6, 0.8], [1.0, 2.0, 3.0]])
def test_dimension_comes_from_vector_and_uuid_format_is_canonical(client, vector):
    prediction = _prediction(
        model_uuid="1affd684-bc7f-45f8-95e4-88f83dd0a2fa", outputs=[vector]
    )
    _mock_prediction_request().return_value = httpx.Response(200, json=prediction)
    response = client.post("/embeddings/user", json={"user_did": DID})
    assert response.status_code == 200
    assert response.json()["user_model_uuid"] == USER_UUID
    assert response.json()["dimension"] == len(vector)


@pytest.mark.parametrize("failed_index", [None, "posts", "replies"])
def test_real_history_path_uses_production_preparation_and_best_effort_hydration(
    client, monkeypatch, failed_index
):
    monkeypatch.setattr(
        embeddings,
        "fetch_user_history_features",
        user_history_cache.fetch_user_history_features,
    )
    monkeypatch.setattr(user_history_cache, "get_user_history_cache", lambda: None)
    likes = _search(
        [
            {"_source": {"subject_uri": uri, "created_at": "2026-09-21T12:00:00Z"}}
            for uri in ["at://reply/1", "at://post/1", "at://missing/1"]
        ]
    )
    posts = _search(
        [
            {
                "_source": {
                    "at_uri": "at://post/1",
                    "content": "post",
                    "author_did": "did:plc:postauthor",
                },
                "fields": {MINILM_L12_EMBEDDING_FIELD: [[2.0] * MINILM_L12_EMBEDDING_DIM]},
            }
        ]
    )
    replies = _search(
        [
            {
                "_source": {
                    "at_uri": "at://reply/1",
                    "content": "reply",
                    "author_did": "did:plc:replyauthor",
                },
                "fields": {MINILM_L12_EMBEDDING_FIELD: [VECTOR]},
            }
        ]
    )

    async def search(**kwargs):
        if kwargs["index"] == failed_index:
            raise RuntimeError("Hydration index unavailable")
        return {"likes": likes, "posts": posts, "replies": replies}[kwargs["index"]]

    app.state.es.search = AsyncMock(side_effect=search)
    for _ in range(2):
        response = client.post("/embeddings/user", json={"user_did": DID})
        assert response.status_code == 200
        assert response.json()["history_like_count"] == 3
        assert response.json()["history_embedding_count"] == (2 if failed_index is None else 1)
    assert app.state.es.search.await_count == 6
    first = app.state.es.search.call_args_list[0].kwargs
    assert first["size"] == 64
    assert first["routing"] == DID
    assert first["sort"] == [{"created_at": "desc"}]
    call = _mock_prediction_request().call_args.kwargs["json"]
    expected = [
        (index, author, vector)
        for index, author, vector in (
            ("replies", "did:plc:replyauthor", VECTOR),
            ("posts", "did:plc:postauthor", [2.0] * MINILM_L12_EMBEDDING_DIM),
        )
        if index != failed_index
    ]
    assert call["history_author_dids"] == [author for _, author, _ in expected]
    assert call["history_embeddings"] == [vector for _, _, vector in expected]

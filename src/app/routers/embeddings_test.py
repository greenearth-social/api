"""HTTP contract and upstream behavior for user embedding exports."""

from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from elastic_transport import ConnectionTimeout
from fastapi.testclient import TestClient

from .. import security
from ..lib import inference, user_history_cache
from ..lib.embeddings import MINILM_L12_EMBEDDING_FIELD
from ..lib.user_history_cache import UserHistory, UserHistoryItem
from ..main import app
from ..security import verify_api_key
from . import embeddings


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(app.state, "es", object(), raising=False)
    monkeypatch.setattr(app.state, "firestore", object(), raising=False)
    monkeypatch.setattr(
        embeddings, "get_inference_settings", lambda: ("https://inference.example", "test-key")
    )
    return TestClient(app)


def _history(embedding):
    return UserHistory(items=[
        UserHistoryItem(
            at_uri="at://post/1",
            liked_at="2026-09-17T00:00:00Z",
            embedding=embedding,
            author_did="did:plc:author",
        )
    ])


def test_user_embedding_uses_history_and_prediction_without_candidate_search(client, monkeypatch):
    fetch = AsyncMock(return_value=_history([1.0, 0.0]))
    monkeypatch.setattr(inference, "fetch_user_history_features", fetch)
    monkeypatch.setattr(
        inference,
        "get_cached_post_tower_uuid",
        AsyncMock(side_effect=AssertionError("must not call post-tower readiness")),
    )
    prediction = {
        "outputs": [[0.6, 0.8]],
        "model_type": "user-tower",
        "model_uuid": "actual-user-model",
    }
    upstream = Mock()
    upstream.post = AsyncMock(return_value=httpx.Response(200, json=prediction))
    monkeypatch.setattr(inference, "get_http_client", lambda: upstream)

    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})

    assert response.status_code == 200
    assert response.json() == {
        "user_did": "did:plc:viewer",
        "status": "ok",
        "embedding": [0.6, 0.8],
        "model_uuid": "actual-user-model",
        "dimension": 2,
        "history_like_count": 1,
        "history_embedding_count": 1,
        "reason": None,
    }
    fetch.assert_awaited_once_with(app.state.es, "did:plc:viewer")
    upstream.post.assert_awaited_once()
    args, kwargs = upstream.post.await_args
    assert args == ("https://inference.example/models/user-tower/predict",)
    assert kwargs["json"] == {
        "history_embeddings": [[1.0, 0.0]], "history_author_dids": ["did:plc:author"]
    }
    assert kwargs["headers"]["X-API-Key"] == "test-key"


@pytest.mark.parametrize(
    ("history", "reason", "count"),
    [(UserHistory(items=[]), "no_likes", 0), (_history(None), "no_embedded_history", 1)],
)
def test_user_embedding_skips_missing_history(client, monkeypatch, history, reason, count):
    monkeypatch.setattr(inference, "fetch_user_history_features", AsyncMock(return_value=history))
    upstream = Mock(side_effect=AssertionError("empty history must not call inference"))
    monkeypatch.setattr(inference, "get_http_client", upstream)

    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})

    assert response.status_code == 200
    assert response.json() == {
        "user_did": "did:plc:viewer",
        "status": "skipped",
        "embedding": None,
        "model_uuid": None,
        "dimension": None,
        "history_like_count": count,
        "history_embedding_count": 0,
        "reason": reason,
    }
    upstream.assert_not_called()


class _HistoryEs:
    """Exercise the actual history/ES helpers with separate post and reply storage."""

    def __init__(self, liked_uris, *, failed_index=None):
        self.liked_uris = liked_uris
        self.failed_index = failed_index
        self.calls = []

    async def search(self, *, index, **kwargs):
        self.calls.append((index, kwargs))
        if index == self.failed_index:
            raise RuntimeError("history index unavailable")
        if index == "likes":
            return {"hits": {"hits": [
                {"_source": {"subject_uri": uri, "created_at": "2026-09-18T00:00:00Z"}}
                for uri in self.liked_uris
            ]}}
        if index == "posts":
            hits = []
            if "at://liked/post" in self.liked_uris:
                hits.append({
                    "_source": {
                        "at_uri": "at://liked/post",
                        "content": "A liked post",
                        "author_did": "did:plc:post-author",
                        "like_count": 10,
                    },
                    "fields": {MINILM_L12_EMBEDDING_FIELD: [[1.0, 0.0]]},
                })
            return {"hits": {"hits": hits}}
        raise AssertionError(f"User history must not query {index}")


@pytest.fixture
def uncached_history(monkeypatch):
    monkeypatch.setattr(user_history_cache, "get_user_history_cache", lambda: None)


def test_user_embedding_main_history_sends_only_posts_to_model(
    client, monkeypatch, uncached_history
):
    es = _HistoryEs(["at://liked/post", "at://liked/reply", "at://liked/missing"])
    monkeypatch.setattr(app.state, "es", es)
    upstream = Mock()
    upstream.post = AsyncMock(return_value=httpx.Response(200, json={
        "outputs": [[0.6, 0.8]],
        "model_type": "user-tower",
        "model_uuid": "actual-user-model",
    }))
    monkeypatch.setattr(inference, "get_http_client", lambda: upstream)

    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["history_like_count"] == 3
    assert response.json()["history_embedding_count"] == 1
    assert [index for index, _kwargs in es.calls] == ["likes", "posts"]
    assert es.calls[0][1]["size"] == 50
    assert es.calls[0][1]["routing"] == "did:plc:viewer"
    upstream.post.assert_awaited_once()
    assert upstream.post.await_args.kwargs["json"] == {
        "history_embeddings": [[1.0, 0.0]],
        "history_author_dids": ["did:plc:post-author"],
    }


def test_user_embedding_main_history_skips_reply_only_likes(
    client, monkeypatch, uncached_history
):
    es = _HistoryEs(["at://liked/reply"])
    monkeypatch.setattr(app.state, "es", es)
    upstream = Mock(side_effect=AssertionError("reply-only history must not call inference"))
    monkeypatch.setattr(inference, "get_http_client", upstream)

    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})

    assert response.status_code == 200
    assert response.json() == {
        "user_did": "did:plc:viewer",
        "status": "skipped",
        "embedding": None,
        "model_uuid": None,
        "dimension": None,
        "history_like_count": 1,
        "history_embedding_count": 0,
        "reason": "no_embedded_history",
    }
    assert [index for index, _kwargs in es.calls] == ["likes", "posts"]
    upstream.assert_not_called()


@pytest.mark.parametrize("failed_index", ["likes", "posts"])
def test_user_embedding_main_history_failure_is_502(
    client, monkeypatch, uncached_history, failed_index
):
    monkeypatch.setattr(app.state, "es", _HistoryEs(["at://liked/post"], failed_index=failed_index))
    upstream = Mock(side_effect=AssertionError("failed history must not call inference"))
    monkeypatch.setattr(inference, "get_http_client", upstream)

    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "upstream_error"
    upstream.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status_code", "code"),
    [
        (RuntimeError("backend unavailable"), 502, "upstream_error"),
        (httpx.ConnectError("connection refused"), 502, "upstream_error"),
        (TimeoutError(), 504, "upstream_timeout"),
        (httpx.ReadTimeout("read timeout"), 504, "upstream_timeout"),
        (ConnectionTimeout("ES timeout"), 504, "upstream_timeout"),
        (
            inference.InferenceResponseFormatError("missing UUID"),
            502,
            "invalid_inference_response",
        ),
    ],
)
def test_user_embedding_errors_are_not_skips(client, monkeypatch, error, status_code, code):
    monkeypatch.setattr(
        embeddings, "compute_user_embedding_result", AsyncMock(side_effect=error)
    )
    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})
    assert response.status_code == status_code
    assert response.json()["detail"]["code"] == code


def test_user_embedding_reports_missing_configuration(client, monkeypatch):
    monkeypatch.setattr(
        embeddings, "get_inference_settings", Mock(side_effect=RuntimeError("missing settings"))
    )
    compute = AsyncMock()
    monkeypatch.setattr(embeddings, "compute_user_embedding_result", compute)
    response = client.post("/embeddings/user", json={"user_did": "did:plc:viewer"})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "inference_not_configured"
    compute.assert_not_awaited()


@pytest.mark.parametrize("api_key", [None, "bad-key"])
def test_user_embedding_rejects_unauthenticated_request(client, monkeypatch, api_key):
    app.dependency_overrides.pop(verify_api_key, None)
    authenticate = AsyncMock(return_value=None)
    monkeypatch.setattr(security, "authenticate_api_key", authenticate)
    response = client.post(
        "/embeddings/user",
        json={"user_did": "did:plc:viewer"},
        headers={"X-API-Key": api_key} if api_key else {},
    )
    assert response.status_code == 401
    authenticate.assert_awaited_once_with(app.state.firestore, api_key)


@pytest.mark.parametrize("payload", [{}, {"user_did": ""}])
def test_user_embedding_requires_user_did(client, payload):
    assert client.post("/embeddings/user", json=payload).status_code == 422

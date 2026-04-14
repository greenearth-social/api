"""Tests for the rank router."""

import asyncio
import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from ..lib.rankers import RankerExecutionError
from ..models import CandidatePost, RankPredictRequest
from . import rank as rank_module
from .rank import router


@pytest.fixture(autouse=True)
def fake_app_state():
    """Set a test API key for every test and restore it afterward."""
    prev = os.environ.get("API_KEY")
    os.environ["API_KEY"] = "testkey"
    yield
    if prev is None:
        del os.environ["API_KEY"]
    else:
        os.environ["API_KEY"] = prev


HEADERS = {"X-API-Key": "testkey"}


@pytest.fixture
def app():
    app = FastAPI()
    app.state.es = object()
    app.include_router(router)
    return app


def test_list_models(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.get("/rank/models")

    assert resp.status_code == 200
    assert resp.json() == {
        "rankers": [
            "candidate_score",
            "two_tower",
        ]
    }


def test_predict_ranks_candidates_by_score_desc(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "user_did": "did:plc:user1",
            "candidates": [
                {"at_uri": "at://post/low", "score": 0.1, "generator_name": "random_posts"},
                {"at_uri": "at://post/high", "score": 0.9, "generator_name": "popularity"},
                {"at_uri": "at://post/mid", "score": 0.4},
            ]
        },
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "rankings": [
            {
                "at_uri": "at://post/high",
                "rank": 1,
                "rank_score": 0.9,
            },
            {
                "at_uri": "at://post/mid",
                "rank": 2,
                "rank_score": 0.4,
            },
            {
                "at_uri": "at://post/low",
                "rank": 3,
                "rank_score": 0.1,
            },
        ],
    }


def test_predict_keeps_duplicate_candidates_and_stable_tie_order(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "user_did": "did:plc:user1",
            "candidates": [
                {"at_uri": "at://post/a", "score": 0.5, "content": "first"},
                {"at_uri": "at://post/a", "score": 0.9, "content": "duplicate"},
                {"at_uri": "at://post/b", "score": 0.5, "content": "second"},
            ]
        },
    )

    assert resp.status_code == 200
    assert resp.json()["rankings"] == [
        {
            "at_uri": "at://post/a",
            "rank": 1,
            "rank_score": 0.9,
        },
        {
            "at_uri": "at://post/a",
            "rank": 2,
            "rank_score": 0.5,
        },
        {
            "at_uri": "at://post/b",
            "rank": 3,
            "rank_score": 0.5,
        },
    ]


def test_predict_unknown_model_returns_404(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "model": "does_not_exist",
            "user_did": "did:plc:user1",
            "candidates": [{"at_uri": "at://post/1", "score": 0.5}],
        },
    )

    assert resp.status_code == 404


def test_predict_rejects_missing_at_uri(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={"user_did": "did:plc:user1", "candidates": [{"score": 0.5}]},
    )

    assert resp.status_code == 400
    assert resp.json() == {"detail": "All candidates must include at_uri"}


def test_predict_requires_user_did(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={"candidates": [{"at_uri": "at://post/1", "score": 0.5}]},
    )

    assert resp.status_code == 422


def test_predict_requires_auth(app):
    client = TestClient(app)
    resp = client.post(
        "/rank/predict",
        json={"user_did": "did:plc:user1", "candidates": [{"at_uri": "at://post/1", "score": 0.5}]},
    )

    assert resp.status_code == 401


def test_rank_predict_maps_ranker_execution_error_to_502(monkeypatch):
    async def fake_run_predict(payload, es):
        raise RankerExecutionError("two_tower", "downstream boom")

    monkeypatch.setattr(rank_module, "run_predict", fake_run_predict)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(es=object())))
    payload = RankPredictRequest(
        model="two_tower",
        user_did="did:plc:user1",
        candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(rank_module.rank_predict(request, payload))

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == "Ranker 'two_tower' failed: downstream boom"

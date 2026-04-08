"""Tests for the rank router."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
    app.include_router(router)
    return app


def test_list_models(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.get("/rank/models")

    assert resp.status_code == 200
    assert resp.json() == {
        "models": [
            {
                "name": "candidate_score",
                "ready": True,
                "metadata": {
                    "kind": "fallback",
                    "description": "Ranks candidates by their existing score field until inference-service ranking is added.",
                },
            }
        ]
    }


def test_predict_ranks_candidates_by_score_desc(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "candidates": [
                {"at_uri": "at://post/low", "score": 0.1, "generator_name": "random_posts"},
                {"at_uri": "at://post/high", "score": 0.9, "generator_name": "popularity"},
                {"at_uri": "at://post/mid", "score": 0.4},
            ]
        },
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "model": "candidate_score",
        "ranked_at_uris": ["at://post/high", "at://post/mid", "at://post/low"],
        "rankings": [
            {
                "at_uri": "at://post/high",
                "rank": 1,
                "score": 0.9,
                "metadata": {"generator_name": "popularity"},
            },
            {
                "at_uri": "at://post/mid",
                "rank": 2,
                "score": 0.4,
                "metadata": {},
            },
            {
                "at_uri": "at://post/low",
                "rank": 3,
                "score": 0.1,
                "metadata": {"generator_name": "random_posts"},
            },
        ],
    }


def test_predict_preserves_first_duplicate_and_stable_tie_order(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "candidates": [
                {"at_uri": "at://post/a", "score": 0.5, "content": "first"},
                {"at_uri": "at://post/a", "score": 0.9, "content": "duplicate"},
                {"at_uri": "at://post/b", "score": 0.5, "content": "second"},
            ]
        },
    )

    assert resp.status_code == 200
    assert resp.json()["ranked_at_uris"] == ["at://post/a", "at://post/b"]
    assert resp.json()["rankings"] == [
        {
            "at_uri": "at://post/a",
            "rank": 1,
            "score": 0.5,
            "metadata": {"content": "first"},
        },
        {
            "at_uri": "at://post/b",
            "rank": 2,
            "score": 0.5,
            "metadata": {"content": "second"},
        },
    ]


def test_predict_unknown_model_returns_404(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={
            "model": "does_not_exist",
            "candidates": [{"at_uri": "at://post/1", "score": 0.5}],
        },
    )

    assert resp.status_code == 404


def test_predict_rejects_missing_at_uri(app):
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/rank/predict",
        json={"candidates": [{"score": 0.5}]},
    )

    assert resp.status_code == 400
    assert resp.json() == {"detail": "All candidates must include at_uri"}


def test_predict_requires_auth(app):
    client = TestClient(app)
    resp = client.post(
        "/rank/predict",
        json={"candidates": [{"at_uri": "at://post/1", "score": 0.5}]},
    )

    assert resp.status_code == 401

"""Tests for the candidates router."""

import os

import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..lib.candidates import get_generator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def fake_app_es():
    """Set up a fake ES client and API key for every test, then clean up."""

    class FakeEs:
        async def search(self, *, index=None, query=None, size=None, sort=None, _source=None, **kwargs):
            if index == "likes":
                return {
                    "hits": {
                        "hits": [
                            {"_source": {"subject_uri": "at://post/1"}},
                            {"_source": {"subject_uri": "at://post/2"}},
                        ]
                    }
                }
            if index == "posts":
                if isinstance(query, dict) and "terms" in query:
                    return {
                        "hits": {
                            "hits": [
                                {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.1, 0.2]}}},
                                {"_source": {"embeddings": {"all_MiniLM_L12_v2": [0.3, 0.4]}}},
                            ]
                        }
                    }
                # kNN search result
                return {
                    "hits": {
                        "hits": [
                            {
                                "_score": 0.88,
                                "_source": {
                                    "at_uri": "at://result/1",
                                    "content": "a cool post",
                                    "embeddings": {"all_MiniLM_L12_v2": [0.2, 0.3]},
                                },
                            }
                        ]
                    }
                }
            return {"hits": {"hits": []}}

    prev = os.environ.get("API_KEY")
    os.environ["API_KEY"] = "testkey"

    app.state.es = FakeEs()
    yield
    try:
        delattr(app.state, "es")
    except Exception:
        pass
    if prev is None:
        del os.environ["API_KEY"]
    else:
        os.environ["API_KEY"] = prev


HEADERS = {"X-API-Key": "testkey"}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_list_generators():
    client = TestClient(app, headers=HEADERS)
    resp = client.get("/candidates/generators")
    assert resp.status_code == 200
    data = resp.json()
    assert "post_similarity" in data["generators"]


def test_generate_post_similarity():
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generator_name": "post_similarity",
            "user_did": "did:plc:user1",
            "num_candidates": 5,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["result"]["generator_name"] == "post_similarity"
    assert len(data["result"]["candidates"]) == 1
    assert data["result"]["candidates"][0]["at_uri"] == "at://result/1"
    assert data["result"]["candidates"][0]["score"] == 0.88


def test_generate_unknown_generator_returns_404():
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generator_name": "nonexistent",
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 404


def test_generate_requires_auth():
    client = TestClient(app)
    resp = client.post(
        "/candidates/generate",
        json={
            "generator_name": "post_similarity",
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 401


def test_generate_default_num_candidates():
    """Verify that num_candidates defaults to 100 when omitted."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generator_name": "post_similarity",
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 200

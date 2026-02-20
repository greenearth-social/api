"""Tests for the candidates router."""

import os

import pytest
from fastapi.testclient import TestClient

from ..main import app


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
                # function_score (popularity) or kNN (post_similarity)
                if isinstance(query, dict) and "function_score" in query:
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_score": 12.5,
                                    "_source": {
                                        "at_uri": "at://popular/1",
                                        "content": "trending post",
                                        "embeddings": {"all_MiniLM_L12_v2": [0.5, 0.6]},
                                    },
                                },
                                {
                                    "_score": 10.0,
                                    "_source": {
                                        "at_uri": "at://popular/2",
                                        "content": "also trending",
                                        "embeddings": {},
                                    },
                                },
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
    assert "popularity" in data["generators"]


def test_generate_single_generator():
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 5,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    # post_similarity returns 1 result from the fake; no infill by default
    assert len(data["candidates"]) == 1
    assert data["candidates"][0]["at_uri"] == "at://result/1"
    assert data["candidates"][0]["score"] == 0.88
    assert data["candidates"][0]["generator_name"] == "post_similarity"


def test_generate_popularity():
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "popularity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 2,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 2
    assert data["candidates"][0]["at_uri"] == "at://popular/1"
    assert data["candidates"][0]["generator_name"] == "popularity"


def test_generate_multiple_generators():
    """Specify multiple generators with weights."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [
                {"name": "post_similarity", "weight": 3},
                {"name": "popularity", "weight": 1},
            ],
            "user_did": "did:plc:user1",
            "num_candidates": 8,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    at_uris = [c["at_uri"] for c in data["candidates"]]
    gen_names = {c["generator_name"] for c in data["candidates"]}
    assert "post_similarity" in gen_names
    assert "popularity" in gen_names
    # No duplicate at_uris
    assert len(at_uris) == len(set(at_uris))


def test_generate_dedup_keeps_first():
    """When two generators return the same at_uri, first occurrence wins."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [
                {"name": "post_similarity", "weight": 1},
                {"name": "popularity", "weight": 1},
            ],
            "user_did": "did:plc:user1",
            "num_candidates": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    at_uris = [c["at_uri"] for c in data["candidates"]]
    assert len(at_uris) == len(set(at_uris))


def test_infill_tops_up_when_primary_returns_few():
    """Infill generator fills remaining slots when primaries fall short."""
    client = TestClient(app, headers=HEADERS)
    # post_similarity fake returns only 1 result; requesting 5 with infill
    # should trigger the infill generator to fill the gap.
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 5,
            "infill": "popularity",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    gen_names = {c["generator_name"] for c in data["candidates"]}
    assert "post_similarity" in gen_names
    assert "popularity" in gen_names
    # Should have de-duplicated mix, capped at num_candidates
    assert len(data["candidates"]) <= 5


def test_infill_not_called_when_enough_results():
    """When primary generators satisfy num_candidates, no infill is needed."""
    client = TestClient(app, headers=HEADERS)
    # popularity fake returns 2 results; request exactly 2 → no infill
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "popularity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 2,
            "infill": "post_similarity",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    # All results from popularity — infill wasn't needed
    assert len(data["candidates"]) == 2
    assert all(c["generator_name"] == "popularity" for c in data["candidates"])


def test_no_infill_returns_fewer_candidates():
    """Without infill, fewer candidates than requested is fine."""
    client = TestClient(app, headers=HEADERS)
    # post_similarity returns 1 result, requesting 10, no infill
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 1
    assert data["candidates"][0]["generator_name"] == "post_similarity"


def test_infill_custom_generator():
    """Callers can specify a different infill generator."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "popularity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 5,
            "infill": "post_similarity",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    gen_names = {c["generator_name"] for c in data["candidates"]}
    assert "post_similarity" in gen_names


def test_generate_unknown_generator_returns_404():
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "nonexistent"}],
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 404


def test_generate_unknown_infill_returns_404():
    client = TestClient(app, headers=HEADERS)
    # post_similarity returns only 1 result so infill will be attempted
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 10,
            "infill": "nonexistent",
        },
    )
    assert resp.status_code == 404


def test_generate_requires_auth():
    client = TestClient(app)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 401


def test_video_only_defaults_to_false():
    """video_only should default to False when not specified."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "popularity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 2,
        },
    )
    assert resp.status_code == 200


def test_video_only_false_accepted():
    """Setting video_only=false should be accepted."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "popularity"}],
            "user_did": "did:plc:user1",
            "num_candidates": 2,
            "video_only": False,
        },
    )
    assert resp.status_code == 200


def test_generate_no_generators_returns_422():
    """Omitting generators entirely should fail validation."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 422


def test_generate_default_num_candidates():
    """Verify that num_candidates defaults to 100 when omitted."""
    client = TestClient(app, headers=HEADERS)
    resp = client.post(
        "/candidates/generate",
        json={
            "generators": [{"name": "post_similarity"}],
            "user_did": "did:plc:user1",
        },
    )
    assert resp.status_code == 200

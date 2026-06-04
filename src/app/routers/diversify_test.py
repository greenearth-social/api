"""Tests for the /diversify endpoint."""

import os

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from ..main import app
from ..security import verify_api_key

HEADERS = {"X-API-Key": "testkey"}


@pytest.fixture(autouse=True)
def set_api_key():
    prev = os.environ.get("API_KEY")
    os.environ["API_KEY"] = "testkey"
    yield
    if prev is None:
        del os.environ["API_KEY"]
    else:
        os.environ["API_KEY"] = prev


def test_auth_required():
    def _raise():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")

    app.dependency_overrides[verify_api_key] = _raise
    try:
        client = TestClient(app)
        resp = client.post("/diversify", json={"candidates": []})
        assert resp.status_code == 401
    finally:
        app.dependency_overrides.pop(verify_api_key, None)


def test_empty_input_returns_200():
    client = TestClient(app, headers=HEADERS)
    resp = client.post("/diversify", json={"candidates": []})
    assert resp.status_code == 200
    assert resp.json()["candidates"] == []


def test_all_candidates_preserved():
    client = TestClient(app, headers=HEADERS)
    candidates = [
        {"at_uri": "at://a/1", "score": 0.9, "author_did": "did:plc:a"},
        {"at_uri": "at://b/1", "score": 0.7, "author_did": "did:plc:b"},
        {"at_uri": "at://c/1", "score": 0.5, "author_did": "did:plc:c"},
    ]
    resp = client.post("/diversify", json={"candidates": candidates})
    assert resp.status_code == 200
    uris = [c["at_uri"] for c in resp.json()["candidates"]]
    assert set(uris) == {"at://a/1", "at://b/1", "at://c/1"}


def test_same_author_spread():
    client = TestClient(app, headers=HEADERS)
    candidates = [
        {"at_uri": "at://alice/1", "score": 1.0, "author_did": "did:plc:alice"},
        {"at_uri": "at://alice/2", "score": 0.9, "author_did": "did:plc:alice"},
        {"at_uri": "at://alice/3", "score": 0.8, "author_did": "did:plc:alice"},
        {"at_uri": "at://bob/1", "score": 0.5, "author_did": "did:plc:bob"},
    ]
    resp = client.post("/diversify", json={"candidates": candidates})
    assert resp.status_code == 200
    uris = [c["at_uri"] for c in resp.json()["candidates"]]
    assert uris[0] == "at://alice/1"
    assert uris.index("at://bob/1") < uris.index("at://alice/2")

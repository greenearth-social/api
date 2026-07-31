import os
from unittest import mock

from fastapi.testclient import TestClient

from ..main import app

client = TestClient(app)


def test_healthcheck_returns_200():
    response = client.get("/health")
    assert response.status_code == 200


def test_healthcheck_response_body():
    response = client.get("/health")
    body = response.json()
    assert body["status"] == "ok"


def test_healthcheck_response_structure():
    response = client.get("/health")
    data = response.json()
    assert "status" in data
    assert isinstance(data["status"], str)


def test_healthcheck_reports_git_sha_when_set():
    with mock.patch.dict(os.environ, {"GE_GIT_SHA": "abc1234"}):
        response = client.get("/health")
    assert response.json()["git_sha"] == "abc1234"


def test_healthcheck_git_sha_null_when_unset():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GE_GIT_SHA", None)
        response = client.get("/health")
    assert response.json()["git_sha"] is None

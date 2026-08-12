"""Tests for app-level middleware in main.py."""

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from .lib import inflight
from .main import _es_connections_per_node, _is_deployed_environment, _resolve_endpoint, app


def _request_for(path: str, method: str = "GET") -> Request:
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "app": app,
    }
    return Request(scope)


def test_resolve_endpoint_returns_route_name():
    assert (
        _resolve_endpoint(_request_for("/xrpc/app.bsky.feed.getFeedSkeleton"))
        == "get_feed_skeleton"
    )
    assert (
        _resolve_endpoint(_request_for("/candidates/generate", method="POST"))
        == "candidates_generate"
    )
    assert _resolve_endpoint(_request_for("/health")) == "healthcheck"


def test_resolve_endpoint_none_for_unknown_path():
    assert _resolve_endpoint(_request_for("/no/such/route")) is None


def test_es_connections_per_node_default(monkeypatch):
    monkeypatch.delenv("GE_ES_CONNECTIONS_PER_NODE", raising=False)
    assert _es_connections_per_node() == 100


def test_es_connections_per_node_from_env(monkeypatch):
    monkeypatch.setenv("GE_ES_CONNECTIONS_PER_NODE", "25")
    assert _es_connections_per_node() == 25


def test_es_connections_per_node_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("GE_ES_CONNECTIONS_PER_NODE", "lots")
    assert _es_connections_per_node() == 100


@pytest.mark.parametrize("env_value", ["prod", "production", "stage", "staging", "PROD", " stage "])
def test_is_deployed_environment_true_for_stage_and_prod(monkeypatch, env_value):
    monkeypatch.setenv("ENVIRONMENT", env_value)
    monkeypatch.delenv("GE_ENVIRONMENT", raising=False)
    assert _is_deployed_environment() is True


@pytest.mark.parametrize("env_value", ["local", "dev", "development", "", "test"])
def test_is_deployed_environment_false_for_local_and_dev(monkeypatch, env_value):
    monkeypatch.setenv("ENVIRONMENT", env_value)
    monkeypatch.delenv("GE_ENVIRONMENT", raising=False)
    assert _is_deployed_environment() is False


def test_is_deployed_environment_false_when_unset(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("GE_ENVIRONMENT", raising=False)
    assert _is_deployed_environment() is False


def test_is_deployed_environment_checks_ge_environment_fallback(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("GE_ENVIRONMENT", "prod")
    assert _is_deployed_environment() is True


def test_inflight_middleware_tracks_and_releases_requests():
    """The counter must rise inside the handler and return to zero after."""
    inflight.reset_for_test()
    seen: list[int] = []

    @app.get("/_inflight_probe")
    async def _probe():
        seen.append(inflight.current())
        return {"ok": True}

    try:
        # No context manager: entering it would run the lifespan, which
        # requires real ES/Firestore configuration this test does not need.
        assert TestClient(app).get("/_inflight_probe").status_code == 200
    finally:
        app.router.routes = [
            r for r in app.router.routes if getattr(r, "path", None) != "/_inflight_probe"
        ]

    assert seen == [1]
    assert inflight.current() == 0


def test_inflight_middleware_releases_on_handler_exception():
    """A failing handler must not leak the process into a permanently-busy state,
    which would make the event-loop monitor record throttled samples forever."""
    inflight.reset_for_test()

    @app.get("/_inflight_boom")
    async def _boom():
        raise RuntimeError("boom")

    try:
        TestClient(app, raise_server_exceptions=False).get("/_inflight_boom")
    finally:
        app.router.routes = [
            r for r in app.router.routes if getattr(r, "path", None) != "/_inflight_boom"
        ]

    assert inflight.current() == 0

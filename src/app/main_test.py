"""Tests for app-level middleware in main.py."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from . import main
from .lib import average_user_embedding as prior_module
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


@pytest.mark.parametrize("configured", [False, True])
def test_lifespan_loads_prior_once_and_keeps_api_available_on_failure(
    monkeypatch, tmp_path, configured
):
    """Optional prior loading cannot take unrelated API routes offline."""
    from pathlib import Path

    monkeypatch.setenv("GE_ELASTICSEARCH_API_KEY", "test-key")
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "test-secret")
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.delenv("GE_POSTHOG_API_KEY", raising=False)
    monkeypatch.delenv("GE_DEV_SESSION_SECRET", raising=False)
    if configured:
        uri = (
            Path(__file__).resolve().parents[2] / "scripts/fixtures/average_user_embedding_v1.json"
        )
    else:
        uri = tmp_path / "missing.json"
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", str(uri))

    load = MagicMock(wraps=prior_module._load_average_user_embedding)
    monkeypatch.setattr(prior_module, "_load_average_user_embedding", load)
    monkeypatch.setattr(main, "AsyncElasticsearch", MagicMock(return_value=AsyncMock()))
    monkeypatch.setattr(main, "SlowQueryLoggingES", lambda es: es)
    metrics = MagicMock(shutdown=AsyncMock())
    monkeypatch.setattr(main, "MetricCollector", MagicMock(return_value=metrics))
    for name in (
        "init_id_resolver",
        "init_firestore_client",
        "init_firebase_auth",
        "init_http_client",
        "start_eventloop_monitor",
        "set_metric_collector",
        "set_popularity_cache",
        "set_followed_users_cache",
        "set_user_history_cache",
        "set_posthog_client",
        "FirestoreFeedCache",
    ):
        monkeypatch.setattr(main, name, MagicMock())
    for name in ("PopularityCache", "FollowedUsersCache", "FirestoreUserHistoryCache"):
        monkeypatch.setattr(main, name, MagicMock(return_value=MagicMock(drain=AsyncMock())))
    for name in ("close_http_client", "close_perspective_client", "stop_eventloop_monitor"):
        monkeypatch.setattr(main, name, AsyncMock())
    monkeypatch.setattr(main, "get_posthog_client", lambda: None)

    local_app = FastAPI(lifespan=main.lifespan)
    local_app.include_router(main.health.router)
    with TestClient(local_app) as client:
        assert (prior_module.get_average_user_embedding() is not None) is configured
        assert client.get("/health").status_code == 200
        assert client.get("/health").status_code == 200
        load.assert_called_once_with(str(uri))

    assert prior_module.get_average_user_embedding() is None

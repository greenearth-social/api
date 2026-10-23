"""Tests for app-level middleware in main.py."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from . import main
from .lib import average_user_embedding as prior_module
from .lib import inflight
from .main import (
    _es_connections_per_node,
    _is_deployed_environment,
    _reject_llm_cg_open_in_deployment,
    _resolve_endpoint,
    app,
)


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


# The guard is called directly rather than through the lifespan: a clean
# startup goes on to build real GCP clients, which needs credentials CI lacks.
@pytest.mark.parametrize("env_value", ["prod", "stage"])
def test_llm_cg_open_is_rejected_when_deployed(monkeypatch, env_value):
    monkeypatch.setenv("GE_LLM_CG_OPEN", "true")
    monkeypatch.setenv("ENVIRONMENT", env_value)
    with pytest.raises(RuntimeError, match="GE_LLM_CG_OPEN"):
        _reject_llm_cg_open_in_deployment()


def test_llm_cg_open_is_fine_locally(monkeypatch):
    monkeypatch.setenv("GE_LLM_CG_OPEN", "true")
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("GE_ENVIRONMENT", raising=False)
    _reject_llm_cg_open_in_deployment()


def test_deployed_is_fine_without_llm_cg_open(monkeypatch):
    monkeypatch.setenv("GE_LLM_CG_OPEN", "")
    monkeypatch.setenv("ENVIRONMENT", "prod")
    _reject_llm_cg_open_in_deployment()


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
    from .lib.rankers import anti_ranker

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

    # Exercise real loading and lifespan cleanup with a valid or missing file.
    # Stub unrelated startup services so API availability needs no cloud access.
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
        assert anti_ranker._db is local_app.state.firestore
        assert client.get("/health").status_code == 200
        assert client.get("/health").status_code == 200
        load.assert_called_once_with(str(uri))

    assert prior_module.get_average_user_embedding() is None
    assert anti_ranker._db is None


def _api_routes(routes):
    """Every APIRoute, including those inside included routers.

    FastAPI keeps an included router as a wrapper route rather than flattening
    it into ``app.routes``; walk it the same way ``main._match_route`` does.
    """
    for route in routes:
        if isinstance(route, APIRoute):
            yield route
            continue
        sub = getattr(route, "routes", None)
        if sub is None:
            sub = getattr(getattr(route, "original_router", None), "routes", None)
        if sub is not None:
            yield from _api_routes(sub)


def _required_scopes(dependant) -> set[str]:
    """Named scopes enforced anywhere in a route's dependency tree."""
    scopes: set[str] = set()
    for dependency in dependant.dependencies:
        scope = getattr(dependency.call, "required_scope", None)
        if scope is not None:
            scopes.add(scope)
        scopes |= _required_scopes(dependency)
    return scopes


def test_scoped_endpoints_document_their_scope():
    """Every endpoint that requires a non-default scope says so in its docs.

    A partner reading /docs must be able to tell which scope their key needs,
    so both the endpoint description and its documented 403 name the scope.
    """
    spec = app.openapi()
    scoped = []
    for route in _api_routes(app.routes):
        for scope in _required_scopes(route.dependant) - {"default"}:
            for method in route.methods or ():
                op = spec["paths"][route.path_format][method.lower()]
                scoped.append(f"{method} {route.path_format}")
                assert f"`{scope}`" in op.get("description", ""), (method, route.path)
                assert scope in op["responses"]["403"]["description"], (method, route.path)
                assert "401" in op["responses"], (method, route.path)

    # The check only means something if it found the scoped endpoints.
    assert "POST /slate/generate" in scoped
    assert "POST /slate/interactions" in scoped
    assert "GET /users/{user_did}/feeds/preferences" in scoped
    assert "PATCH /users/{user_did}/feeds/preferences/{feed_name}" in scoped

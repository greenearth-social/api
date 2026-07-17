"""Tests for app-level middleware in main.py."""

import pytest
from fastapi import Request

from .main import _is_deployed_environment, _resolve_endpoint, app


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

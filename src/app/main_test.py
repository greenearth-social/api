"""Tests for app-level middleware in main.py."""

from fastapi import Request

from .main import _resolve_endpoint, app


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

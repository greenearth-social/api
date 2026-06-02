"""Process-wide shared ``httpx.AsyncClient``.

Inference and Bluesky calls previously constructed a fresh
``httpx.AsyncClient`` per invocation, paying a TLS handshake every
time. This module hoists one client to app scope so its connection
pools live across requests.

The lifespan handler calls :func:`init_http_client` at startup and
:func:`close_http_client` at shutdown. Other callers obtain the
client via :func:`get_http_client`; outside of app lifetime (e.g.
tests that bypass lifespan) it lazily creates a client so callers
never get ``None``.
"""

from __future__ import annotations

import httpx

_client: httpx.AsyncClient | None = None


def init_http_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0)
    return _client


async def close_http_client() -> None:
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        finally:
            _client = None


def get_http_client() -> httpx.AsyncClient:
    if _client is None:
        return init_http_client()
    return _client

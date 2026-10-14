"""Per-request ID propagated via a ContextVar.

A short, server-generated UUID is stamped on each inbound HTTP request
by the request-ID middleware in :mod:`app.main` and stored here so that
log lines emitted anywhere on the request path can include it without
the caller having to thread an ID parameter through every function.

The ID is **always** server-generated; we deliberately do not honor an
inbound ``x-request-id`` header. Trusting client-supplied IDs would let
any caller inject arbitrary values into our logs.

ContextVar propagation through ``asyncio.gather`` is identical to
``request_cache_scope``: child tasks inherit the parent's context at
spawn time, so concurrent generators and the two-tower side branches
all see the same ``rid``.
"""

from __future__ import annotations

from contextvars import ContextVar, Token

_request_id: ContextVar[str | None] = ContextVar("ge_request_id", default=None)

# The name of the API endpoint handling the current request (the matched
# route's name, e.g. ``get_feed_skeleton`` or ``candidates_generate``). Set by
# the endpoint middleware in :mod:`app.main` so that metrics recorded anywhere
# on the request path can be tagged with their originating endpoint without
# threading it through every callsite. Propagates through ``asyncio`` the same
# way as ``rid``, so background tasks spawned from a request inherit it.
_endpoint: ContextVar[str | None] = ContextVar("ge_endpoint", default=None)

# The traffic class of the current request: ``real`` (a genuine user or the
# default), ``probe`` (Cloud Scheduler liveness probe), ``load_test`` (a
# simulated load-test session), or ``logged_out`` (a feed served to an
# unauthenticated visitor). Set by the feed endpoint so that every metric
# recorded on the request path — including from background tasks, which inherit
# this context at spawn time — can be split by traffic class and test traffic
# filtered out of dashboards. Propagates through ``asyncio`` like ``rid``.
_traffic: ContextVar[str | None] = ContextVar("ge_traffic", default=None)


def get_request_id() -> str | None:
    return _request_id.get()


def set_request_id(rid: str) -> Token:
    return _request_id.set(rid)


def reset_request_id(token: Token) -> None:
    _request_id.reset(token)


def get_endpoint() -> str | None:
    return _endpoint.get()


def set_endpoint(endpoint: str) -> Token:
    return _endpoint.set(endpoint)


def reset_endpoint(token: Token) -> None:
    _endpoint.reset(token)


def get_traffic() -> str | None:
    return _traffic.get()


def set_traffic(traffic: str) -> Token:
    """Set the traffic class (``real`` | ``probe`` | ``load_test`` | ``logged_out``)."""
    return _traffic.set(traffic)


def reset_traffic(token: Token) -> None:
    _traffic.reset(token)

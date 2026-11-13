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

import time
from contextvars import ContextVar, Token
from dataclasses import dataclass, field

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

# The shape of the current feed request: ``page`` (``first`` | ``cursor`` |
# ``check``, the last being AppView's one-item reachability check) and, on
# getFeedSkeleton, ``surface`` (``feed`` | ``explore``). Set by the feed
# endpoints so latency can be read for the requests a target is about, e.g.
# first pages outside Explore. Tagged onto every metric like ``traffic``.
_request_shape: ContextVar[dict[str, str] | None] = ContextVar("ge_request_shape", default=None)


@dataclass
class FeedRequestTiming:
    """Wallclock of one feed request, from the start of generation to the response.

    The middleware that records it runs ``call_next`` in a separate task, which
    copies the context: a ContextVar *set* by the handler is invisible to the
    middleware afterwards. So the middleware installs this mutable object
    first, and the handler marks it in place.
    """

    start: float | None = None
    labels: dict[str, str] = field(default_factory=dict)


_feed_timing: ContextVar[FeedRequestTiming | None] = ContextVar("ge_feed_timing", default=None)


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
    if (timing := _feed_timing.get()) is not None:
        timing.labels["traffic"] = traffic
    return _traffic.set(traffic)


def reset_traffic(token: Token) -> None:
    _traffic.reset(token)


def get_request_shape() -> dict[str, str]:
    return _request_shape.get() or {}


def set_request_shape(*, page: str, surface: str | None = None) -> None:
    """Set the feed request's shape labels (see ``_request_shape``)."""
    shape = {"page": page} if surface is None else {"page": page, "surface": surface}
    _request_shape.set(shape)
    if (timing := _feed_timing.get()) is not None:
        timing.labels.update(shape)


def begin_feed_timing() -> tuple[FeedRequestTiming, Token]:
    """Install a fresh ``FeedRequestTiming`` for the request about to run."""
    timing = FeedRequestTiming()
    return timing, _feed_timing.set(timing)


def reset_feed_timing(token: Token) -> None:
    _feed_timing.reset(token)


def start_feed_timing() -> None:
    """Mark the start of feed generation: everything after caller authentication.

    A no-op outside a request (e.g. a handler called directly in a test).
    """
    if (timing := _feed_timing.get()) is not None and timing.start is None:
        timing.start = time.monotonic()

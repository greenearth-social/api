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


def get_request_id() -> str | None:
    return _request_id.get()


def set_request_id(rid: str) -> Token:
    return _request_id.set(rid)


def reset_request_id(token: Token) -> None:
    _request_id.reset(token)

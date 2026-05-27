"""Per-request cache for expensive lookups.

A ``ContextVar`` holds the current request's cache so helper functions
(e.g. the Elasticsearch query wrappers in ``lib/elasticsearch.py``) can
consult it transparently without callers passing a cache parameter
through every layer.

The router enters ``request_cache_scope()`` at the top of each request;
the scope is per-task, so concurrent requests get independent caches
and child tasks spawned via ``asyncio.gather`` inherit the parent's
cache automatically.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextvars import ContextVar
from typing import Any, Awaitable, Callable

_current_cache: ContextVar["RequestCache | None"] = ContextVar(
    "ge_request_cache", default=None
)


class RequestCache:
    """Async-safe single-flight cache keyed by any hashable value.

    Two concurrent calls with the same key share one underlying
    computation, so duplicate ES queries within a request collapse
    into a single round-trip.
    """

    def __init__(self) -> None:
        self._store: dict[Any, Any] = {}
        self._locks: dict[Any, asyncio.Lock] = {}

    async def get_or_compute(
        self, key: Any, factory: Callable[[], Awaitable[Any]]
    ) -> Any:
        if key in self._store:
            return self._store[key]
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key in self._store:
                return self._store[key]
            value = await factory()
            self._store[key] = value
            return value


def get_request_cache() -> RequestCache | None:
    return _current_cache.get()


@contextlib.asynccontextmanager
async def request_cache_scope():
    """Install a fresh ``RequestCache`` for the duration of the block."""
    cache = RequestCache()
    token = _current_cache.set(cache)
    try:
        yield cache
    finally:
        _current_cache.reset(token)

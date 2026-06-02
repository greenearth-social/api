"""Tests for the per-request cache."""

import asyncio

import pytest

from .request_cache import (
    RequestCache,
    get_request_cache,
    request_cache_scope,
)


def test_get_request_cache_returns_none_outside_scope():
    assert get_request_cache() is None


def test_request_cache_scope_installs_and_clears():
    async def run():
        async with request_cache_scope() as cache:
            assert get_request_cache() is cache
        assert get_request_cache() is None

    asyncio.run(run())


def test_request_cache_memoizes_within_scope():
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        return calls

    async def run():
        cache = RequestCache()
        v1 = await cache.get_or_compute("k", factory)
        v2 = await cache.get_or_compute("k", factory)
        assert v1 == 1 and v2 == 1
        return calls

    assert asyncio.run(run()) == 1


def test_request_cache_collapses_concurrent_calls():
    """Two concurrent calls with the same key share one underlying computation."""
    started = 0

    async def factory():
        nonlocal started
        started += 1
        await asyncio.sleep(0)  # yield so a racing caller would see no value yet
        return "result"

    async def run():
        cache = RequestCache()
        results = await asyncio.gather(
            cache.get_or_compute("k", factory),
            cache.get_or_compute("k", factory),
        )
        return started, results

    started_count, results = asyncio.run(run())
    assert results == ["result", "result"]
    assert started_count == 1


def test_request_cache_distinguishes_keys():
    async def run():
        cache = RequestCache()
        a = await cache.get_or_compute("a", _async_value(1))
        b = await cache.get_or_compute("b", _async_value(2))
        return a, b

    assert asyncio.run(run()) == (1, 2)


def _async_value(v):
    async def factory():
        return v

    return factory

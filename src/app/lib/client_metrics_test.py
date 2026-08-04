"""Tests for client queue-position metrics."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest

from app.lib import client_metrics


# The trace callbacks ignore their session and params arguments; the cast
# keeps the type checker happy without fabricating aiohttp internals.
UNUSED = cast(Any, None)


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.mark.asyncio
async def test_aiohttp_trace_records_pool_wait(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(client_metrics, "get_metric_collector", lambda: collector)

    tc = client_metrics.aiohttp_trace_config("perspective")
    ctx = SimpleNamespace()
    await tc.on_connection_queued_start[0](UNUSED, ctx, UNUSED)
    await tc.on_connection_queued_end[0](UNUSED, ctx, UNUSED)

    [(name, value, attrs)] = collector.records
    assert name == "client.pool.wait_ms"
    assert value >= 0
    assert attrs == {"client": "perspective"}


@pytest.mark.asyncio
async def test_aiohttp_trace_records_connect_duration(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(client_metrics, "get_metric_collector", lambda: collector)

    tc = client_metrics.aiohttp_trace_config("perspective")
    ctx = SimpleNamespace()
    await tc.on_connection_create_start[0](UNUSED, ctx, UNUSED)
    await tc.on_connection_create_end[0](UNUSED, ctx, UNUSED)

    [(name, _, attrs)] = collector.records
    assert name == "client.connect.duration_ms"
    assert attrs == {"client": "perspective"}


@pytest.mark.asyncio
async def test_aiohttp_trace_end_without_start_records_nothing(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(client_metrics, "get_metric_collector", lambda: collector)
    tc = client_metrics.aiohttp_trace_config("perspective")
    await tc.on_connection_queued_end[0](UNUSED, SimpleNamespace(), UNUSED)
    assert collector.records == []


class _BlockingTransport(httpx.AsyncBaseTransport):
    """Inner transport that parks until released, to overlap requests."""

    def __init__(self):
        self.release = asyncio.Event()

    async def handle_async_request(self, request):
        await self.release.wait()
        return httpx.Response(200, request=request)


@pytest.mark.asyncio
async def test_in_flight_transport_counts_concurrent_requests(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(client_metrics, "get_metric_collector", lambda: collector)

    inner = _BlockingTransport()
    transport = client_metrics.InFlightTransport(inner)
    client = httpx.AsyncClient(transport=transport)

    async def _get():
        return await client.get("http://inference.test/x")

    t1 = asyncio.create_task(_get())
    t2 = asyncio.create_task(_get())
    await asyncio.sleep(0.01)  # let both reach the transport
    inner.release.set()
    await asyncio.gather(t1, t2)
    await client.aclose()

    values = [v for n, v, a in collector.records
              if n == "client.in_flight" and a == {"host": "inference.test"}]
    assert sorted(values) == [1, 2]


@pytest.mark.asyncio
async def test_in_flight_transport_decrements_on_error(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(client_metrics, "get_metric_collector", lambda: collector)

    class _FailingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("boom", request=request)

    transport = client_metrics.InFlightTransport(_FailingTransport())
    client = httpx.AsyncClient(transport=transport)
    with pytest.raises(httpx.ConnectError):
        await client.get("http://inference.test/x")

    # After the failure the counter must be back at zero: a fresh request
    # records in-flight 1, not 2.
    inner = _BlockingTransport()
    transport2 = client_metrics.InFlightTransport(inner)
    # same tracker instance matters — reuse transport, swap inner:
    transport._inner = inner
    inner.release.set()
    await client.get("http://inference.test/x")
    await client.aclose()

    values = [v for n, v, a in collector.records if n == "client.in_flight"]
    assert values == [1, 1]

"""Queue-position metrics for concurrency-limited clients.

Any pooled client can reproduce #344's pattern: work queues client-side
while the backend idles. Where the transport exposes queue events
(aiohttp), record pool wait time; where it doesn't (httpx), record the
in-flight count — a p95 pinned at the configured cap is saturation even
before latency shows it.
"""

from __future__ import annotations

import time

import aiohttp
import httpx


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def aiohttp_trace_config(client: str) -> aiohttp.TraceConfig:
    tc = aiohttp.TraceConfig()

    async def _queued_start(session, ctx, params):
        ctx.queued_start = time.monotonic()

    async def _queued_end(session, ctx, params):
        start = getattr(ctx, "queued_start", None)
        if start is None:
            return
        collector = get_metric_collector()
        if collector is not None:
            collector.record(
                "client.pool.wait_ms", (time.monotonic() - start) * 1000, client=client
            )

    async def _create_start(session, ctx, params):
        ctx.create_start = time.monotonic()

    async def _create_end(session, ctx, params):
        start = getattr(ctx, "create_start", None)
        if start is None:
            return
        collector = get_metric_collector()
        if collector is not None:
            collector.record(
                "client.connect.duration_ms", (time.monotonic() - start) * 1000, client=client
            )

    tc.on_connection_queued_start.append(_queued_start)
    tc.on_connection_queued_end.append(_queued_end)
    tc.on_connection_create_start.append(_create_start)
    tc.on_connection_create_end.append(_create_end)
    return tc


class InFlightTransport(httpx.AsyncBaseTransport):
    """Wraps a transport, sampling concurrent requests per host.

    The sample is taken before the inner transport (i.e. before pool
    acquisition), so queued requests count — sustained samples at the
    pool cap mean requests are waiting, not working.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner
        self._counts: dict[str, int] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host or "unknown"
        self._counts[host] = self._counts.get(host, 0) + 1
        collector = get_metric_collector()
        if collector is not None:
            collector.record("client.in_flight", self._counts[host], host=host)
        try:
            return await self._inner.handle_async_request(request)
        finally:
            self._counts[host] = self._counts.get(host, 1) - 1

    async def aclose(self) -> None:
        await self._inner.aclose()

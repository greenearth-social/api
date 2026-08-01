"""Slow-query logging wrapper around ``AsyncElasticsearch``.

When ``.search()`` exceeds ``GE_SLOW_ES_THRESHOLD_MS`` (default 500 ms),
the wrapper logs a single WARNING line carrying the full request body
so the query can be replayed verbatim — paste the ``body=`` JSON into
curl or Kibana to reproduce.

Records metrics:
- ``es.query.duration_ms``: client-side elapsed time (histogram, labeled by ``op``)
- ``es.query.took_ms``: server-side time from ES response (histogram, labeled by ``op``; omitted if missing)

Other attributes (e.g. ``close``, ``indices``) are exposed transparently
via ``__getattr__`` so the wrapper is a drop-in for the underlying
``AsyncElasticsearch`` client.
"""

from __future__ import annotations

import json
import logging
import os
import time

from elastic_transport import ConnectionTimeout

from .request_context import get_request_id

logger = logging.getLogger(__name__)


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def _extract_took(resp) -> float | None:
    body = getattr(resp, "body", resp)
    if isinstance(body, dict):
        took = body.get("took")
        if isinstance(took, (int, float)):
            return float(took)
    return None


def _record_query_metrics(op: str, elapsed_ms: float, took_ms: float | None) -> None:
    collector = get_metric_collector()
    if collector is None:
        return
    collector.record("es.query.duration_ms", elapsed_ms, op=op)
    if took_ms is not None:
        collector.record("es.query.took_ms", took_ms, op=op)


def _slow_threshold_ms() -> float:
    """Read the threshold each call so it tracks env changes during dev."""
    try:
        return float(os.environ.get("GE_SLOW_ES_THRESHOLD_MS", "500"))
    except ValueError:
        return 500.0


class SlowQueryLoggingES:
    """Proxy that times each ``.search()`` and logs slow ones."""

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped

    def __getattr__(self, name: str):
        # Delegated for every attribute other than the ones explicitly
        # defined on this class — including `close`, `indices`, `transport`,
        # context-manager protocols, etc.
        return getattr(self._wrapped, name)

    async def search(self, *args, op: str = "unlabeled", **kwargs):
        start = time.monotonic()
        timed_out = False
        took_ms: float | None = None
        try:
            resp = await self._wrapped.search(*args, **kwargs)
            took_ms = _extract_took(resp)
            return resp
        except ConnectionTimeout:
            timed_out = True
            elapsed_ms = (time.monotonic() - start) * 1000
            _log_timeout_search(elapsed_ms, args, kwargs)
            _record_query_metrics(op, elapsed_ms, None)
            raise
        finally:
            if not timed_out:
                elapsed_ms = (time.monotonic() - start) * 1000
                _record_query_metrics(op, elapsed_ms, took_ms)
                if elapsed_ms >= _slow_threshold_ms():
                    _log_slow_search(elapsed_ms, args, kwargs)


def _log_timeout_search(elapsed_ms: float, args: tuple, kwargs: dict) -> None:
    """Emit a WARNING when the ES query raised ConnectionTimeout."""
    rid = get_request_id() or "-"
    body = {k: v for k, v in kwargs.items() if k not in ("index", "request_timeout")}
    try:
        body_str = json.dumps(body, default=str)
    except Exception:
        body_str = repr(body)
    logger.warning(
        "es_query_timeout rid=%s elapsed_ms=%.1f index=%s body=%s",
        rid,
        elapsed_ms,
        kwargs.get("index"),
        body_str,
    )


def _log_slow_search(elapsed_ms: float, args: tuple, kwargs: dict) -> None:
    """Emit the WARNING line. Split out so it can be unit-tested in isolation."""
    rid = get_request_id() or "-"
    # `index` and `request_timeout` are client-side parameters that aren't
    # part of the request body proper; pull them out so the body= field is
    # exactly what you'd POST to ES.
    body = {k: v for k, v in kwargs.items() if k not in ("index", "request_timeout")}
    try:
        body_str = json.dumps(body, default=str)
    except Exception:
        body_str = repr(body)
    logger.warning(
        "slow_es_query rid=%s elapsed_ms=%.1f index=%s body=%s",
        rid,
        elapsed_ms,
        kwargs.get("index"),
        body_str,
    )

"""Slow-query logging wrapper around ``AsyncElasticsearch``.

When ``.search()`` exceeds ``GE_SLOW_ES_THRESHOLD_MS`` (default 500 ms),
the wrapper logs a single WARNING line carrying the full request body
so the query can be replayed verbatim — paste the ``body=`` JSON into
curl or Kibana to reproduce.

Other attributes (e.g. ``close``, ``indices``) are exposed transparently
via ``__getattr__`` so the wrapper is a drop-in for the underlying
``AsyncElasticsearch`` client.
"""

from __future__ import annotations

import json
import logging
import os
import time

from .request_context import get_request_id

logger = logging.getLogger(__name__)


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

    async def search(self, *args, **kwargs):
        start = time.monotonic()
        try:
            return await self._wrapped.search(*args, **kwargs)
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            threshold = _slow_threshold_ms()
            if elapsed_ms >= threshold:
                _log_slow_search(elapsed_ms, args, kwargs)


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

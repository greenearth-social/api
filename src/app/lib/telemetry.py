"""Telemetry helpers for timing API and Elasticsearch operations."""

import logging
import os
import time
from contextlib import asynccontextmanager

from .request_context import FeedRequestTiming, get_request_id


@asynccontextmanager
async def timed(
    logger: logging.Logger,
    label: str,
    *,
    record_metric: bool = False,
    metric_attrs: dict[str, str] | None = None,
    **extra: object,
):
    """Async context manager that times a labelled operation.

    *label* is used as the log label and, when *record_metric* is ``True``,
    as the metric name emitted to the configured ``MetricCollector``.

    Timing logs are only emitted when ``GE_TIMING_LOGS=1`` is set.  Metric
    recording is unconditional when *record_metric* is ``True`` and a
    collector is configured.

    Parameters
    ----------
    logger:
        Logger to emit timing lines to.
    label:
        Human-readable label for logging; also used as the metric name when
        *record_metric* is ``True`` (e.g. ``"feed.render.duration_ms"``).
    record_metric:
        When ``True``, record elapsed milliseconds as a metric via the
        active ``MetricCollector``.  Defaults to ``False``.
    metric_attrs:
        Mapping of low-cardinality label key/values passed to the metric
        (e.g. ``{"feed_name": "nature"}``).  Kept separate from *extra*
        because continuous or high-cardinality values (like counts) are
        useful in logs but produce unhelpful metric label explosions.
    **extra:
        Additional key/value context included only in log lines — not in
        metrics.  Useful for counts, offsets, model names, etc.

    Usage::

        async with timed(logger, "feed.render.duration_ms", record_metric=True,
                         metric_attrs={"feed_name": feed_name}):
            result = await _render_feed(...)

        async with timed(logger, "feedcache_retrieve", cache_id=parsed.id):
            cached = await feed_cache.retrieve(parsed.id)
    """
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed_ms = (time.monotonic() - start) * 1000

        if os.environ.get("GE_TIMING_LOGS") == "1":
            rid = get_request_id()
            log_extra: dict[str, object] = {"elapsed_ms": elapsed_ms, **extra}
            if metric_attrs:
                log_extra.update(metric_attrs)
            if rid is not None:
                log_extra["rid"] = rid
                logger.info(
                    "%s rid=%s elapsed_ms=%.1f",
                    label,
                    rid,
                    elapsed_ms,
                    extra=log_extra,
                )
            else:
                logger.info(
                    "%s elapsed_ms=%.1f",
                    label,
                    elapsed_ms,
                    extra=log_extra,
                )

        if record_metric:
            from .metrics import get_metric_collector
            collector = get_metric_collector()
            if collector is not None:
                collector.record(label, elapsed_ms, **(metric_attrs or {}))


FEED_REQUEST_METRIC = "feed.request.duration_ms"


def _status_class(status_code: int) -> str:
    return f"{status_code // 100}xx"


def record_feed_request(
    logger: logging.Logger, timing: FeedRequestTiming, status_code: int
) -> None:
    """Record a feed request's wallclock, from generation start to response start.

    Only requests whose handler called ``start_feed_timing`` are recorded, so
    this covers exactly the feed endpoints, and only once the caller is
    authenticated: auth is excluded, while user lookup, feature flags,
    generation and response serialization are included.
    """
    if timing.start is None:
        return
    elapsed_ms = (time.monotonic() - timing.start) * 1000
    labels = {**timing.labels, "status": _status_class(status_code)}

    if os.environ.get("GE_TIMING_LOGS") == "1":
        rid = get_request_id()
        logger.info(
            "%s rid=%s elapsed_ms=%.1f %s",
            FEED_REQUEST_METRIC,
            rid,
            elapsed_ms,
            " ".join(f"{k}={v}" for k, v in sorted(labels.items())),
            extra={"elapsed_ms": elapsed_ms, "rid": rid, **labels},
        )

    from .metrics import get_metric_collector
    collector = get_metric_collector()
    if collector is not None:
        collector.record(FEED_REQUEST_METRIC, elapsed_ms, **labels)

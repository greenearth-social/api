"""Telemetry helpers for timing API and Elasticsearch operations."""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
from elastic_transport import ConnectionTimeout

from .request_context import FeedRequestTiming, get_request_id


@asynccontextmanager
async def timed(
    logger: logging.Logger,
    label: str,
    *,
    record_metric: bool = False,
    record_outcome: bool = False,
    log_failures: bool = False,
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
    record_outcome:
        Add a bounded ``outcome`` metric label: success, error, timeout, or
        cancelled. Cancellation samples include time spent before a caller's
        deadline, so dependency latency does not hide its slowest operations.
    log_failures:
        Log unsuccessful operations at WARNING even without ``GE_TIMING_LOGS``.
        Request IDs and exception types stay in logs, never metric labels.
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
    outcome = "success"
    error_type = None
    try:
        yield
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            outcome = "cancelled"
        elif isinstance(exc, (TimeoutError, httpx.TimeoutException, ConnectionTimeout)):
            outcome = "timeout"
        else:
            outcome = "error"
        error_type = type(exc).__name__
        raise
    finally:
        elapsed_ms = (time.monotonic() - start) * 1000
        attrs = dict(metric_attrs or {})
        if record_outcome:
            attrs["outcome"] = outcome

        if log_failures and outcome != "success":
            rid = get_request_id()
            logger.warning(
                "%s rid=%s elapsed_ms=%.1f outcome=%s error_type=%s %s",
                label,
                rid or "-",
                elapsed_ms,
                outcome,
                error_type,
                " ".join(f"{key}={value}" for key, value in sorted((metric_attrs or {}).items())),
                extra={
                    **extra,
                    **attrs,
                    "rid": rid,
                    "elapsed_ms": elapsed_ms,
                    "outcome": outcome,
                    "error_type": error_type,
                },
            )
        elif os.environ.get("GE_TIMING_LOGS") == "1":
            rid = get_request_id()
            log_extra: dict[str, object] = {"elapsed_ms": elapsed_ms, **extra}
            log_extra.update(attrs)
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
                collector.record(label, elapsed_ms, **attrs)


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

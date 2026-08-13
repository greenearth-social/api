"""Event-loop lag monitor: the direct "api instance saturated" signal.

A background task sleeps for a fixed interval and records how far past the
deadline it actually woke up. Under a healthy loop the overshoot is ~0-2 ms;
when coroutines or sync work monopolize the loop, every pending callback —
including responses already received from ES/inference/Perspective — waits
this long to run. See issue #343 for the attribution design.

Samples are only recorded for intervals during which a request was in flight
the whole time. Cloud Run runs this service with ``cpuIdle: true``, so an idle
instance is CPU-throttled and this task's own wakeups are delayed by hundreds
of milliseconds to seconds — throttling that no request ever experienced. Left
ungated the metric reports that as loop lag: prod p99 was ~800 ms and p99.9
~4.7 s while p50 sat at a healthy 1.5 ms, with the distribution splitting into
two modes (74% under 3 ms, a second population from 25 ms up) that tracked
instance idleness rather than load — flat across a 10x traffic swing. See
issue #389.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from . import inflight


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


DEFAULT_INTERVAL_SEC = 0.25


async def _monitor_loop(interval_sec: float) -> None:
    while True:
        snap = inflight.snapshot()
        target = time.monotonic() + interval_sec
        await asyncio.sleep(interval_sec)
        lag_ms = max(0.0, (time.monotonic() - target) * 1000)
        if not inflight.busy_throughout(snap):
            continue
        collector = get_metric_collector()
        if collector is not None:
            collector.record("eventloop.lag_ms", lag_ms)


def start_eventloop_monitor(interval_sec: float = DEFAULT_INTERVAL_SEC) -> asyncio.Task:
    return asyncio.create_task(_monitor_loop(interval_sec), name="eventloop-monitor")


async def stop_eventloop_monitor(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

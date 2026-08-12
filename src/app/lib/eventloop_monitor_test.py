import asyncio

import pytest

from app.lib import eventloop_monitor, inflight


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.fixture(autouse=True)
def _reset_inflight():
    inflight.reset_for_test()
    yield
    inflight.reset_for_test()


def _lags(collector):
    return [v for n, v, _ in collector.records if n == "eventloop.lag_ms"]


@pytest.mark.asyncio
async def test_monitor_records_lag_samples_while_requests_in_flight(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    inflight.begin()
    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.08)
    await eventloop_monitor.stop_eventloop_monitor(task)

    lag_records = [r for r in collector.records if r[0] == "eventloop.lag_ms"]
    assert len(lag_records) >= 3
    for _, value, attrs in lag_records:
        assert value >= 0
        assert attrs == {}


@pytest.mark.asyncio
async def test_monitor_records_nothing_while_idle(monkeypatch):
    """On Cloud Run with cpuIdle=true an idle instance is CPU-throttled, so a
    sample taken with no request in flight measures throttling, not loop lag."""
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.08)
    await eventloop_monitor.stop_eventloop_monitor(task)

    assert _lags(collector) == []


@pytest.mark.asyncio
async def test_monitor_skips_interval_that_straddles_idle(monkeypatch):
    """A window that began idle is partly throttled time and must be dropped."""
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.05)
    await asyncio.sleep(0.01)
    inflight.begin()  # arrives mid-window
    await asyncio.sleep(0.06)
    await eventloop_monitor.stop_eventloop_monitor(task)

    # The straddling window is skipped; only fully-busy windows may record.
    assert len(_lags(collector)) <= 1


@pytest.mark.asyncio
async def test_monitor_measures_overshoot(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    inflight.begin()
    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    # Block the loop synchronously for ~50ms; the next sample must see it.
    await asyncio.sleep(0.02)
    import time as _time
    _time.sleep(0.05)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)

    assert max(_lags(collector)) >= 30  # ms — blocked well past the 10ms interval


@pytest.mark.asyncio
async def test_stop_is_clean(monkeypatch):
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: None)
    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)
    assert task.cancelled() or task.done()

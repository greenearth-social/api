import asyncio

import pytest

from app.lib import eventloop_monitor


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.mark.asyncio
async def test_monitor_records_lag_samples(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.08)
    await eventloop_monitor.stop_eventloop_monitor(task)

    lag_records = [r for r in collector.records if r[0] == "eventloop.lag_ms"]
    assert len(lag_records) >= 3
    for _, value, attrs in lag_records:
        assert value >= 0
        assert attrs == {}


@pytest.mark.asyncio
async def test_monitor_measures_overshoot(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: collector)

    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    # Block the loop synchronously for ~50ms; the next sample must see it.
    await asyncio.sleep(0.02)
    import time as _time
    _time.sleep(0.05)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)

    lags = [v for n, v, _ in collector.records if n == "eventloop.lag_ms"]
    assert max(lags) >= 30  # ms — blocked well past the 10ms interval


@pytest.mark.asyncio
async def test_stop_is_clean(monkeypatch):
    monkeypatch.setattr(eventloop_monitor, "get_metric_collector", lambda: None)
    task = eventloop_monitor.start_eventloop_monitor(interval_sec=0.01)
    await asyncio.sleep(0.02)
    await eventloop_monitor.stop_eventloop_monitor(task)
    assert task.cancelled() or task.done()

"""Tests for telemetry.timed()."""

import asyncio
import logging

import httpx
import pytest
from elastic_transport import ConnectionTimeout
from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader

from .metrics import MetricCollector, set_metric_collector
from .request_context import reset_request_id, set_request_id
from .telemetry import timed


def _make_collector() -> tuple[MetricCollector, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    collector = MetricCollector._from_reader(reader, service_name="test", env="test")
    return collector, reader


def _get_metrics_data(reader: InMemoryMetricReader):
    data = reader.get_metrics_data()
    assert data is not None
    return data


@pytest.mark.asyncio
async def test_records_metric_when_record_metric_true():
    collector, reader = _make_collector()
    set_metric_collector(collector)
    try:
        async with timed(logging.getLogger("test"), "op.duration_ms", record_metric=True):
            pass
        data = _get_metrics_data(reader)
        names = {
            metric.name
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
        }
        assert "op.duration_ms" in names
    finally:
        set_metric_collector(None)


@pytest.mark.asyncio
async def test_no_metric_when_record_metric_false():
    collector, reader = _make_collector()
    set_metric_collector(collector)
    try:
        async with timed(logging.getLogger("test"), "op.duration_ms"):
            pass
        data = reader.get_metrics_data()
        names: set[str] = set()
        if data is not None:
            names = {
                metric.name
                for rm in data.resource_metrics
                for sm in rm.scope_metrics
                for metric in sm.metrics
            }
        assert not names
    finally:
        set_metric_collector(None)


@pytest.mark.asyncio
async def test_no_metric_without_collector():
    set_metric_collector(None)
    async with timed(logging.getLogger("test"), "op.duration_ms", record_metric=True):
        pass


@pytest.mark.asyncio
async def test_metric_attrs_become_metric_labels():
    collector, reader = _make_collector()
    set_metric_collector(collector)
    try:
        async with timed(
            logging.getLogger("test"),
            "feed.render.duration_ms",
            record_metric=True,
            metric_attrs={"feed_name": "nature"},
        ):
            pass
        data = reader.get_metrics_data()
        found = False
        if data is not None:
            for rm in data.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        if metric.name == "feed.render.duration_ms":
                            dp = metric.data.data_points[0]
                            attrs = dp.attributes or {}
                            assert attrs.get("feed_name") == "nature"
                            found = True
        assert found
    finally:
        set_metric_collector(None)


@pytest.mark.asyncio
async def test_extra_kwargs_not_in_metric_attrs():
    """**extra (log-only) kwargs must not appear as metric attributes."""
    collector, reader = _make_collector()
    set_metric_collector(collector)
    try:
        async with timed(
            logging.getLogger("test"),
            "feed.render.duration_ms",
            record_metric=True,
            metric_attrs={"feed_name": "nature"},
            count=42,
        ):
            pass
        data = reader.get_metrics_data()
        if data is not None:
            for rm in data.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        if metric.name == "feed.render.duration_ms":
                            dp = metric.data.data_points[0]
                            attrs = dp.attributes or {}
                            assert "count" not in attrs
    finally:
        set_metric_collector(None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,outcome",
    [
        (None, "success"),
        (ValueError("invalid response"), "error"),
        (TimeoutError("dependency deadline"), "timeout"),
        (httpx.ReadTimeout("response deadline"), "timeout"),
        (ConnectionTimeout("ES deadline"), "timeout"),
        (asyncio.CancelledError(), "cancelled"),
    ],
)
async def test_outcome_timing_preserves_exceptions_and_records_failed_samples(
    error, outcome, caplog, monkeypatch
):
    monkeypatch.delenv("GE_TIMING_LOGS", raising=False)
    collector, reader = _make_collector()
    set_metric_collector(collector)
    token = set_request_id("abc123def456")
    attrs = {"stage": "history", "feed_name": "your-feed"}

    async def operation():
        async with timed(
            logging.getLogger("test"),
            "dependency.duration_ms",
            record_metric=True,
            record_outcome=True,
            log_failures=True,
            metric_attrs=attrs,
            n_history=64,
        ):
            if error is not None:
                raise error

    try:
        if error is None:
            await operation()
        else:
            with pytest.raises(type(error)) as raised:
                await operation()
            assert raised.value is error
        data = _get_metrics_data(reader)
        metrics = [m for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics]
        assert len(metrics) == 1
        point = metrics[0].data.data_points[0]
        assert isinstance(point, HistogramDataPoint)
        assert point.count == 1
        assert point.sum >= 0
        assert dict(point.attributes or {}) == {**attrs, "outcome": outcome}
        assert attrs == {"stage": "history", "feed_name": "your-feed"}
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == (0 if error is None else 1)
        if warnings:
            record = warnings[0]
            assert record.rid == "abc123def456"
            assert record.outcome == outcome
            assert record.error_type == type(error).__name__
            assert record.n_history == 64
            assert "stage=history" in record.message
            assert "feed_name=your-feed" in record.message
            assert "rid=abc123def456" in record.message
    finally:
        reset_request_id(token)
        set_metric_collector(None)


@pytest.mark.asyncio
async def test_outcome_timing_keeps_parent_cancellation():
    collector, reader = _make_collector()
    set_metric_collector(collector)
    started = asyncio.Event()

    async def operation():
        async with timed(
            logging.getLogger("test"),
            "dependency.duration_ms",
            record_metric=True,
            record_outcome=True,
        ):
            started.set()
            await asyncio.Future()

    task = asyncio.create_task(operation())
    try:
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        data = _get_metrics_data(reader)
        points = [
            point
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            for point in metric.data.data_points
        ]
        assert len(points) == 1
        point = points[0]
        assert isinstance(point, HistogramDataPoint)
        assert point.attributes is not None
        assert point.attributes["outcome"] == "cancelled"
        assert point.count == 1
    finally:
        task.cancel()
        set_metric_collector(None)

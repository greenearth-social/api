"""Tests for telemetry.timed()."""

import logging
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from .metrics import MetricCollector, set_metric_collector
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

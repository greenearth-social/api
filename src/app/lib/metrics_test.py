"""Tests for MetricCollector."""

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricExportResult,
)

from .metrics import MetricCollector, get_metric_collector, set_metric_collector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_collector(service_name: str = "test-svc", env: str = "test") -> tuple[MetricCollector, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    collector = MetricCollector._from_reader(reader, service_name=service_name, env=env)
    return collector, reader


def _get_metrics_data(reader: InMemoryMetricReader):
    data = reader.get_metrics_data()
    assert data is not None
    return data


def _collect_names_from_data(data) -> set[str]:
    names: set[str] = set()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                names.add(metric.name)
    return names


# ---------------------------------------------------------------------------
# Instrument type inference
# ---------------------------------------------------------------------------

def test_counter_inferred_for_count_suffix():
    collector, reader = _make_collector()
    collector.record("requests_count", 5)
    data = _get_metrics_data(reader)
    found = False
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "requests_count":
                    from opentelemetry.sdk.metrics._internal.point import Sum
                    assert isinstance(metric.data, Sum)
                    found = True
    assert found, "requests_count not found in exported metrics"


def test_gauge_inferred_for_rate_suffix():
    collector, reader = _make_collector()
    collector.record("throughput_rate", 42.5)
    data = _get_metrics_data(reader)
    found = False
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "throughput_rate":
                    from opentelemetry.sdk.metrics._internal.point import Gauge
                    assert isinstance(metric.data, Gauge)
                    found = True
    assert found, "throughput_rate not found in exported metrics"


def test_histogram_inferred_for_ms_suffix():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 123.4)
    data = _get_metrics_data(reader)
    found = False
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "feed.render.duration_ms":
                    from opentelemetry.sdk.metrics._internal.point import Histogram
                    assert isinstance(metric.data, Histogram)
                    found = True
    assert found, "feed.render.duration_ms not found in exported metrics"


def test_histogram_inferred_for_arbitrary_name():
    collector, reader = _make_collector()
    collector.record("something.latency", 99.0)
    data = _get_metrics_data(reader)
    assert _collect_names_from_data(data) == {"something.latency"}


# ---------------------------------------------------------------------------
# Attributes (labels)
# ---------------------------------------------------------------------------

def test_attributes_attached_to_histogram():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 50.0, feed_name="nature")
    data = _get_metrics_data(reader)
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "feed.render.duration_ms":
                    dp = metric.data.data_points[0]
                    attrs = dp.attributes or {}
                    assert attrs.get("feed_name") == "nature"


def _attrs_for(reader, name: str) -> dict:
    data = _get_metrics_data(reader)
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    return dict(metric.data.data_points[0].attributes or {})
    raise AssertionError(f"{name} not found in exported metrics")


def test_endpoint_label_added_from_context():
    from .request_context import reset_endpoint, set_endpoint

    collector, reader = _make_collector()
    token = set_endpoint("get_feed_skeleton")
    try:
        collector.record("feed.render.duration_ms", 50.0, feed_name="nature")
    finally:
        reset_endpoint(token)

    attrs = _attrs_for(reader, "feed.render.duration_ms")
    assert attrs.get("endpoint") == "get_feed_skeleton"
    assert attrs.get("feed_name") == "nature"


def test_no_endpoint_label_outside_request_context():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 50.0)
    assert "endpoint" not in _attrs_for(reader, "feed.render.duration_ms")


def test_explicit_endpoint_attribute_wins():
    from .request_context import reset_endpoint, set_endpoint

    collector, reader = _make_collector()
    token = set_endpoint("get_feed_skeleton")
    try:
        collector.record("something.latency", 1.0, endpoint="explicit")
    finally:
        reset_endpoint(token)

    assert _attrs_for(reader, "something.latency").get("endpoint") == "explicit"


# ---------------------------------------------------------------------------
# Lazy instrument reuse
# ---------------------------------------------------------------------------

def test_same_instrument_reused_across_calls():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 10.0)
    collector.record("feed.render.duration_ms", 20.0)
    data = _get_metrics_data(reader)
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "feed.render.duration_ms":
                    from opentelemetry.sdk.metrics._internal.point import Histogram
                    assert isinstance(metric.data, Histogram)
                    # Both values should be in the same histogram
                    dp = metric.data.data_points[0]
                    assert dp.count == 2


# ---------------------------------------------------------------------------
# Local/dev (non-deployed environment)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_local_env_records_without_exporting(capsys):
    """Local/dev should record metrics without printing a resource_metrics blob."""
    collector = MetricCollector(
        service_name="test-svc",
        env="local",
        export_interval_sec=60,
    )
    collector.record("some.metric_ms", 1.0)
    # Force a flush; nothing should be written to stdout/stderr in dev.
    await collector.shutdown()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "resource_metrics" not in captured.out


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def test_set_and_get_metric_collector():
    collector, _ = _make_collector()
    set_metric_collector(collector)
    assert get_metric_collector() is collector
    set_metric_collector(None)
    assert get_metric_collector() is None


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_does_not_raise():
    collector, _ = _make_collector()
    await collector.shutdown()

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
    data = reader.get_metrics_data()
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
    data = reader.get_metrics_data()
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
    data = reader.get_metrics_data()
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
    data = reader.get_metrics_data()
    assert _collect_names_from_data(data) == {"something.latency"}


# ---------------------------------------------------------------------------
# Attributes (labels)
# ---------------------------------------------------------------------------

def test_attributes_attached_to_histogram():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 50.0, feed_name="nature")
    data = reader.get_metrics_data()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "feed.render.duration_ms":
                    dp = metric.data.data_points[0]
                    assert dp.attributes.get("feed_name") == "nature"


# ---------------------------------------------------------------------------
# Lazy instrument reuse
# ---------------------------------------------------------------------------

def test_same_instrument_reused_across_calls():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 10.0)
    collector.record("feed.render.duration_ms", 20.0)
    data = reader.get_metrics_data()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == "feed.render.duration_ms":
                    # Both values should be in the same histogram
                    dp = metric.data.data_points[0]
                    assert dp.count == 2


# ---------------------------------------------------------------------------
# Stdout fallback (non-deployed environment)
# ---------------------------------------------------------------------------

def test_stdout_fallback_for_local_env(capsys):
    """Non-stage/prod environments should construct without error using stdout exporter."""
    collector = MetricCollector(
        service_name="test-svc",
        env="local",
        export_interval_sec=60,
    )
    collector.record("some.metric_ms", 1.0)
    # No exception means stdout fallback worked


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

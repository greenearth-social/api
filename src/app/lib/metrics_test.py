"""Tests for MetricCollector."""

from typing import Any
from unittest.mock import patch

import pytest
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
)

from .metrics import MetricCollector, get_metric_collector, set_metric_collector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_collector(
    service_name: str = "test-svc", env: str = "test"
) -> tuple[MetricCollector, InMemoryMetricReader]:
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


def test_traffic_label_added_from_context():
    from .request_context import reset_traffic, set_traffic

    collector, reader = _make_collector()
    token = set_traffic("load_test")
    try:
        collector.record("feed.render.duration_ms", 50.0)
    finally:
        reset_traffic(token)

    assert _attrs_for(reader, "feed.render.duration_ms").get("traffic") == "load_test"


def test_no_traffic_label_outside_request_context():
    collector, reader = _make_collector()
    collector.record("feed.render.duration_ms", 50.0)
    assert "traffic" not in _attrs_for(reader, "feed.render.duration_ms")


def test_explicit_traffic_attribute_wins():
    from .request_context import reset_traffic, set_traffic

    collector, reader = _make_collector()
    token = set_traffic("load_test")
    try:
        collector.record("something.latency", 1.0, traffic="explicit")
    finally:
        reset_traffic(token)

    assert _attrs_for(reader, "something.latency").get("traffic") == "explicit"


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
# GCP exporter construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["stage", "prod"])
def test_gcp_exporter_uses_unique_identifier(env):
    """Cloud Run scales greenearth-api to multiple concurrent instances, each
    running its own exporter. Without a unique identifier per exporter, two
    instances exporting the same metric+label combination in the same
    interval collide on GCP's cumulative point ordering and the whole batch
    write is rejected (see issue #263)."""
    with patch(
        "opentelemetry.exporter.cloud_monitoring.CloudMonitoringMetricsExporter"
    ) as mock_exporter_cls:
        MetricCollector(
            service_name="test-svc",
            env=env,
            export_interval_sec=60,
        )
    _, kwargs = mock_exporter_cls.call_args
    assert kwargs.get("add_unique_identifier") is True


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


# ---------------------------------------------------------------------------
# Histogram bucket boundaries
#
# The OTel default boundaries (…, 1000, 2500, 5000, 7500, 10000) leave only
# four buckets above 1s, so a p95 anywhere in the 1–5s serving range is an
# interpolation across a 1.5–2.5s-wide bucket. Ratio metrics are worse: every
# value in [0, 1] falls in the first default bucket.
# ---------------------------------------------------------------------------

from .metrics import (  # noqa: E402
    CONCURRENCY_BOUNDARIES,
    EVENTLOOP_LAG_MS_BOUNDARIES,
    FAST_MS_BOUNDARIES,
    LATENCY_MS_BOUNDARIES,
    RATIO_BOUNDARIES,
    histogram_boundaries,
)


class TestHistogramBoundaries:
    @pytest.mark.parametrize(
        "name",
        [
            "feed.render.duration_ms",
            "feed.mmr.duration_ms",
            "candidates.generate.duration_ms",
            "rank.model.duration_ms",
            "perspective.score.duration_ms",
            "es.query.duration_ms",
            "es.query.took_ms",
        ],
    )
    def test_latency_metrics(self, name):
        assert histogram_boundaries(name) == LATENCY_MS_BOUNDARIES

    @pytest.mark.parametrize(
        "name",
        ["client.pool.wait_ms", "client.connect.duration_ms"],
    )
    def test_near_zero_metrics(self, name):
        assert histogram_boundaries(name) == FAST_MS_BOUNDARIES

    def test_eventloop_lag_has_extended_tail_coverage(self):
        assert histogram_boundaries("eventloop.lag_ms") == EVENTLOOP_LAG_MS_BOUNDARIES
        assert EVENTLOOP_LAG_MS_BOUNDARIES[-1] == 60_000

    @pytest.mark.parametrize(
        "name",
        [
            "client.in_flight",
            "es.client.in_flight",
            "feed.slate.exclusion_size",
            "feed.mmr.input_size",
        ],
    )
    def test_concurrency_metrics(self, name):
        assert histogram_boundaries(name) == CONCURRENCY_BOUNDARIES

    @pytest.mark.parametrize(
        "name",
        [
            "candidates.generate.retrieved_share",
            "feed.slate.kept_share",
            "feed.mean_similarity_score",
        ],
    )
    def test_ratio_metrics(self, name):
        assert histogram_boundaries(name) == RATIO_BOUNDARIES

    def test_unknown_metric_falls_back_to_sdk_default(self):
        assert histogram_boundaries("something.unrecognised") is None

    def test_boundaries_are_sorted_and_unique(self):
        for bounds in (
            LATENCY_MS_BOUNDARIES,
            FAST_MS_BOUNDARIES,
            EVENTLOOP_LAG_MS_BOUNDARIES,
            CONCURRENCY_BOUNDARIES,
            RATIO_BOUNDARIES,
        ):
            assert list(bounds) == sorted(bounds)
            assert len(set(bounds)) == len(bounds)

    def test_dashboard_threshold_lines_are_exact_bucket_edges(self):
        """monitoring/dashboards/bottleneck.json.tmpl draws baseline lines at
        these values. A threshold that is itself a boundary makes the
        percentile estimate exact where it matters — at the threshold."""
        assert 2500 in LATENCY_MS_BOUNDARIES  # feed.render p95
        assert 100 in EVENTLOOP_LAG_MS_BOUNDARIES  # eventloop.lag_ms p95
        assert 10 in FAST_MS_BOUNDARIES  # client.pool.wait_ms p95
        assert 100 in CONCURRENCY_BOUNDARIES  # pool caps

    def test_serving_range_is_finely_resolved(self):
        """The 1–5s range is where the serving SLOs live; the OTel default has
        two buckets there, which is what made p95 unmeasurable."""
        in_range = [b for b in LATENCY_MS_BOUNDARIES if 1000 <= b <= 5000]
        assert len(in_range) >= 10
        widest = max(b - a for a, b in zip(in_range, in_range[1:], strict=False))
        assert widest <= 500


class TestHistogramBoundariesApplied:
    # data_points is a union of point types; the concrete one depends on the
    # instrument, so return Any rather than narrowing at every call site.
    def _points(self, reader, name) -> Any:
        for rm in reader.get_metrics_data().resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name == name:
                        return list(metric.data.data_points)[0]
        raise AssertionError(f"metric {name} not exported")

    def test_latency_histogram_uses_custom_bounds(self):
        collector, reader = _make_collector()
        collector.record("feed.render.duration_ms", 2425.0)
        point = self._points(reader, "feed.render.duration_ms")
        assert tuple(point.explicit_bounds) == tuple(LATENCY_MS_BOUNDARIES)

    def test_nearby_multi_second_values_separate_into_buckets(self):
        """2425 and 4881 sat in 1000–2500 and 2500–5000 under the defaults, so
        every p95 in between was an interpolation. They must now be resolvable
        to a few hundred ms."""
        collector, reader = _make_collector()
        for value in (2425.0, 2485.0, 4881.0, 4937.0):
            collector.record("feed.render.duration_ms", value)
        point = self._points(reader, "feed.render.duration_ms")
        occupied = [i for i, count in enumerate(point.bucket_counts) if count]
        assert len(occupied) >= 2
        bounds = list(point.explicit_bounds)
        for index in occupied:
            lower = bounds[index - 1] if index else 0
            upper = bounds[index] if index < len(bounds) else float("inf")
            assert upper - lower <= 500

    def test_ratio_histogram_separates_values_in_zero_to_one(self):
        """Under the SDK default every share/score landed in the first bucket."""
        collector, reader = _make_collector()
        for value in (0.05, 0.5, 0.95):
            collector.record("feed.slate.kept_share", value)
        point = self._points(reader, "feed.slate.kept_share")
        assert len([i for i, count in enumerate(point.bucket_counts) if count]) == 3

    def test_concurrency_histogram_separates_small_integers(self):
        """in_flight of 1 vs 4 both fell in the default 0–5 bucket, which made
        the pool-saturation signal unreadable at the low end."""
        collector, reader = _make_collector()
        for value in (1, 2, 4):
            collector.record("es.client.in_flight", value)
        point = self._points(reader, "es.client.in_flight")
        assert len([i for i, count in enumerate(point.bucket_counts) if count]) == 3

    def test_counters_are_unaffected(self):
        collector, reader = _make_collector()
        collector.record("feed.render.failure_count", 1)
        point = self._points(reader, "feed.render.failure_count")
        assert point.value == 1

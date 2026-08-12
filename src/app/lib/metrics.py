"""OpenTelemetry-based metric collector for GCP Cloud Monitoring.

When ``ENVIRONMENT`` is ``stage`` or ``prod``, metrics are exported to GCP
Cloud Monitoring under the prefix ``custom.googleapis.com/greenearth-api/``.
In local/dev environments metrics are still recorded but not exported anywhere
-- the console exporter dumps a large ``resource_metrics`` JSON blob on every
interval, which is just noise during local development.

Instrument type is inferred from the metric name suffix:
  - ``_count`` → Int64Counter  (cumulative sum)
  - ``_rate``  → ObservableGauge (current value)
  - everything else → Float64Histogram  (timing, durations, etc.)

Histograms carry explicit bucket boundaries chosen per metric family (see
``histogram_boundaries``): the SDK defaults are too coarse to answer the
questions these metrics exist to answer.
"""

from __future__ import annotations

import asyncio
from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

_metric_collector: "MetricCollector | None" = None

# ---------------------------------------------------------------------------
# Histogram bucket boundaries
#
# A distribution's percentiles are only as precise as its buckets: a value
# falling inside a bucket is interpolated across that bucket's whole width.
# The OTel defaults -- (0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500,
# 5000, 7500, 10000) -- leave four buckets above 1s, so every p95 in the 1-5s
# serving range interpolates across a 1.5-2.5s-wide bucket and sub-second
# latency targets cannot be evaluated at all. Ratios are worse: everything in
# [0, 1] lands in the single first bucket.
#
# Each set below is dense where that family's decisions get made, and includes
# the dashboard's baseline threshold values as exact edges so the percentile
# estimate is precise at the threshold it is being compared against.
# ---------------------------------------------------------------------------

# Request/stage/dependency latency. Dense to 250ms through 3s (the serving
# range), coarsening beyond the 10s probe ceiling.
LATENCY_MS_BOUNDARIES: tuple[float, ...] = (
    1, 2, 5, 10, 20, 35, 50, 75, 100, 150, 200, 300, 400, 500, 650, 800,
    1000, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3500, 4000, 4500,
    5000, 6000, 7500, 10000, 15000,
)

# Signals that are near zero when healthy and only interesting once they are
# not: event-loop lag, connection-pool wait, connect time. Sub-millisecond
# resolution at the bottom, saturation coverage at the top.
FAST_MS_BOUNDARIES: tuple[float, ...] = (
    0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 25, 50, 75, 100, 150, 250, 500,
    1000, 2500, 5000,
)

# Small-integer counts: in-flight requests against a pool cap, slate sizes.
# Distinguishing 1 from 4 matters -- that is the difference between an idle
# pool and one about to queue.
CONCURRENCY_BOUNDARIES: tuple[float, ...] = (
    1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 75, 90, 100,
    125, 150, 200,
)

# Fractions in [0, 1]: retrieved/kept share, mean similarity score.
RATIO_BOUNDARIES: tuple[float, ...] = (
    0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0,
)

_NEAR_ZERO_MS_METRICS = frozenset(
    {"eventloop.lag_ms", "client.pool.wait_ms", "client.connect.duration_ms"}
)


def histogram_boundaries(name: str) -> tuple[float, ...] | None:
    """Bucket boundaries for *name*, or ``None`` to accept the SDK default.

    Matching is by metric-name suffix so new metrics in an existing family
    are covered without touching this function.
    """
    if name.endswith("in_flight") or name.endswith("_size"):
        return CONCURRENCY_BOUNDARIES
    if name.endswith("_share") or name.endswith("_score"):
        return RATIO_BOUNDARIES
    if name in _NEAR_ZERO_MS_METRICS:
        return FAST_MS_BOUNDARIES
    if name.endswith("_ms"):
        return LATENCY_MS_BOUNDARIES
    return None


def set_metric_collector(collector: "MetricCollector | None") -> None:
    global _metric_collector
    _metric_collector = collector


def get_metric_collector() -> "MetricCollector | None":
    return _metric_collector


class MetricCollector:
    """Process-level OTel metric collector.

    Construct via the normal ``__init__`` for production use, or via
    ``_from_reader`` in tests to inject a controllable reader.
    """

    def __init__(
        self,
        service_name: str,
        env: str,
        export_interval_sec: int,
    ) -> None:
        if env in ("stage", "prod"):
            from opentelemetry.exporter.cloud_monitoring import (
                CloudMonitoringMetricsExporter,
            )
            exporter = CloudMonitoringMetricsExporter(
                prefix="custom.googleapis.com/greenearth-api",
                # Cloud Run scales this service to multiple concurrent
                # instances, each running its own exporter. GCP's generic_node
                # monitored resource doesn't distinguish between instances, so
                # without a per-exporter unique identifier, two instances
                # exporting the same metric+label combination in the same
                # interval collide on GCP's cumulative-point ordering check
                # and the entire batch write is rejected -- silently dropping
                # metrics for every series in that batch (see issue #263).
                add_unique_identifier=True,
            )
            reader: Any | None = PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=export_interval_sec * 1000,
            )
        else:
            # Local/dev: record metrics but don't export them. A console
            # exporter would print a big resource_metrics JSON blob each
            # interval, which is just noise locally.
            reader = None

        self._init(reader, service_name, env)

    @classmethod
    def _from_reader(
        cls,
        reader: Any,
        service_name: str,
        env: str,
    ) -> "MetricCollector":
        instance = cls.__new__(cls)
        instance._init(reader, service_name, env)
        return instance

    def _init(self, reader: Any | None, service_name: str, env: str) -> None:
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": env,
            }
        )
        self._provider = MeterProvider(
            resource=resource,
            metric_readers=[reader] if reader is not None else [],
        )
        self._meter = self._provider.get_meter("greenearth/api")
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._counters: dict[str, Any] = {}

    def record(self, name: str, value: float, **attributes: str) -> None:
        """Record a single metric observation.

        Instruments are lazily created based on the name suffix (see module
        docstring).  Attributes become GCP metric labels.

        When recorded on a request path, the metric is automatically tagged
        with an ``endpoint`` label naming the originating API endpoint (e.g.
        ``get_feed_skeleton`` vs ``candidates_generate``) and a ``traffic``
        label naming the traffic class (``real`` | ``probe`` | ``load_test`` |
        ``logged_out``).
        Both are read from ContextVars set on the request path, so every
        callsite gets them for free — including background tasks, which inherit
        the context at spawn time. Explicitly passed attributes win.
        """
        from .request_context import get_endpoint, get_traffic

        labels: dict[str, str] = dict(attributes)
        endpoint = get_endpoint()
        if endpoint is not None:
            labels.setdefault("endpoint", endpoint)
        traffic = get_traffic()
        if traffic is not None:
            labels.setdefault("traffic", traffic)
        attrs = labels or None
        if name.endswith("_count"):
            self._get_counter(name).add(int(value), attrs)
        elif name.endswith("_rate"):
            self._get_gauge(name).set(value, attrs)
        else:
            self._get_histogram(name).record(value, attrs)

    async def shutdown(self) -> None:
        await asyncio.get_event_loop().run_in_executor(None, self._provider.shutdown)

    # ------------------------------------------------------------------
    # Lazy instrument creation
    # ------------------------------------------------------------------

    def _get_histogram(self, name: str) -> Any:
        h = self._histograms.get(name)
        if h is None:
            boundaries = histogram_boundaries(name)
            if boundaries is None:
                h = self._meter.create_histogram(name)
            else:
                h = self._meter.create_histogram(
                    name, explicit_bucket_boundaries_advisory=list(boundaries)
                )
            self._histograms[name] = h
        return h

    def _get_gauge(self, name: str) -> Any:
        g = self._gauges.get(name)
        if g is None:
            g = self._meter.create_gauge(name)
            self._gauges[name] = g
        return g

    def _get_counter(self, name: str) -> Any:
        c = self._counters.get(name)
        if c is None:
            c = self._meter.create_counter(name)
            self._counters[name] = c
        return c

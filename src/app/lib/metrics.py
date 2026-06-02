"""OpenTelemetry-based metric collector for GCP Cloud Monitoring.

When ``ENVIRONMENT`` is ``stage`` or ``prod``, metrics are exported to GCP
Cloud Monitoring under the prefix ``custom.googleapis.com/greenearth-api/``.
Otherwise the stdout exporter is used for local development.

Instrument type is inferred from the metric name suffix:
  - ``_count`` → Int64Counter  (cumulative sum)
  - ``_rate``  → ObservableGauge (current value)
  - everything else → Float64Histogram  (timing, durations, etc.)
"""

from __future__ import annotations

import asyncio
from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource

_metric_collector: "MetricCollector | None" = None


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
            )
        else:
            exporter = ConsoleMetricExporter()

        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=export_interval_sec * 1000,
        )
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

    def _init(self, reader: Any, service_name: str, env: str) -> None:
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": env,
            }
        )
        self._provider = MeterProvider(
            resource=resource,
            metric_readers=[reader],
        )
        self._meter = self._provider.get_meter("greenearth/api")
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._counters: dict[str, Any] = {}

    def record(self, name: str, value: float, **attributes: str) -> None:
        """Record a single metric observation.

        Instruments are lazily created based on the name suffix (see module
        docstring).  Attributes become GCP metric labels.
        """
        attrs = dict(attributes) if attributes else None
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
            h = self._meter.create_histogram(name)
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

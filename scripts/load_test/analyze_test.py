"""Tests for analyze.py.

Two concerns live here: the Cloud Monitoring query builder behind the
server-side percentile summary, and the dashboard deep-link that ``run()``
prints alongside it (see ``load_test.lib.dashboard_url`` and its tests in
``lib_test.py``).
"""

import argparse
import io
import json

from google.cloud import monitoring_v3
from rich.console import Console

from load_test import analyze
from load_test.analyze import ENV_RESOURCE_LABEL, build_percentile_request


def _interval():
    return monitoring_v3.TimeInterval(
        {"start_time": {"seconds": 1_000}, "end_time": {"seconds": 2_000}}
    )


def test_filters_by_metric_type_and_environment():
    req = build_percentile_request(
        monitoring_v3, "greenearth-471522", "custom.googleapis.com/x", "prod",
        _interval(), 95, alignment_seconds=1000,
    )
    assert 'metric.type = "custom.googleapis.com/x"' in req.filter
    assert f'resource.labels.{ENV_RESOURCE_LABEL} = "prod"' in req.filter
    assert req.name == "projects/greenearth-471522"


def _write_results(tmp_path, records):
    path = tmp_path / "results.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return str(path)


def _run_and_capture(tmp_path, monkeypatch, records, dashboard_return):
    results = _write_results(tmp_path, records)
    buf = io.StringIO()
    monkeypatch.setattr(analyze, "console", Console(file=buf, width=200))
    monkeypatch.setattr(analyze, "dashboard_url", lambda *a, **kw: dashboard_return)

    args = argparse.Namespace(
        results=[results],
        environment="stage",
        no_server=True,
        project="greenearth-471522",
        start=None,
        end=None,
        pad_min=2.0,
    )
    analyze.run(args)
    return buf.getvalue()


_RECORD = {
    "ts": "2026-07-31T02:00:00+00:00",
    "feed": "your-feed",
    "phase": "initial",
    "cohort": "existing",
    "status": 200,
    "latency_ms": 100,
}


def test_run_prints_dashboard_link_when_available(tmp_path, monkeypatch):
    out = _run_and_capture(
        tmp_path, monkeypatch, [_RECORD], "https://console.cloud.google.com/monitoring/x"
    )
    assert "Dashboard (run window pre-set)" in out
    assert "https://console.cloud.google.com/monitoring/x" in out


def test_run_prints_hint_when_no_dashboard_id(tmp_path, monkeypatch):
    out = _run_and_capture(tmp_path, monkeypatch, [_RECORD], None)
    assert "No dashboard ID recorded for this environment" in out
    assert "monitoring/deploy.sh" in out

def test_aggregates_percentile_grouped_by_traffic():
    req = build_percentile_request(
        monitoring_v3, "p", "m", "stage", _interval(), 99, alignment_seconds=600,
    )
    agg = req.aggregation
    # ALIGN_DELTA is the only aligner valid on a CUMULATIVE DISTRIBUTION; the
    # percentile is taken by the cross-series reducer, not the aligner.
    assert agg.per_series_aligner == monitoring_v3.Aggregation.Aligner.ALIGN_DELTA
    assert agg.cross_series_reducer == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99
    assert list(agg.group_by_fields) == ["metric.label.traffic"]
    assert agg.alignment_period.seconds == 600


def test_each_percentile_maps_to_its_reducer():
    reducers = {
        p: build_percentile_request(
            monitoring_v3, "p", "m", "stage", _interval(), p, alignment_seconds=60
        ).aggregation.cross_series_reducer
        for p in (50, 95, 99)
    }
    assert reducers[50] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_50
    assert reducers[95] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_95
    assert reducers[99] == monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99
    # Every percentile is aligned the same way — with ALIGN_DELTA.
    aligners = {
        p: build_percentile_request(
            monitoring_v3, "p", "m", "stage", _interval(), p, alignment_seconds=60
        ).aggregation.per_series_aligner
        for p in (50, 95, 99)
    }
    assert set(aligners.values()) == {monitoring_v3.Aggregation.Aligner.ALIGN_DELTA}

"""Tests for analyze.py.

The Cloud Monitoring query-building logic this module used to test
(``build_percentile_request``, ``ENV_RESOURCE_LABEL``) was removed along with
the server-side metrics/logs sections in favor of a deep-link to the Cloud
Monitoring dashboard — see ``load_test.lib.dashboard_url`` and its tests in
``lib_test.py``. What's left worth testing here is that ``run()`` wires the
dashboard link (or its absence) into the printed report.
"""

import argparse
import io
import json

from rich.console import Console

from load_test import analyze


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

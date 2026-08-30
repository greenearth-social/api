#!/usr/bin/env python3
"""Analyze a load-test run (issue api#189).

Reports client-side latency/error stats from the generator's JSONL output,
then prints a deep-link to the Cloud Monitoring dashboard (with the run
window pre-set) for server-side signals — Cloud Monitoring metrics and
Cloud Run logs, comparing load-test traffic against real traffic over the
run window. See ``monitoring/README.md`` for the dashboard playbook.

This script deliberately reads **nothing from Firestore** — all cohort/DID
context it needs is stamped on every record in the results file — so it can be
run after scripts/load_test/cleanup.py has already removed the test data.

Run from the api/ directory:

    pipenv run python scripts/load_test/analyze.py --results results.jsonl \
        --environment stage
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scripts/

from load_test.lib import (
    CLOUD_RUN_SERVICES,
    GCP_PROJECT,
    dashboard_url,
    gcloud_env,
    percentiles,
)

console = Console()


def _read_records(paths: list[str]) -> list[dict]:
    records: list[dict] = []
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    if not records:
        raise SystemExit("No records found in results file(s)")
    return records


def _window(records: list[dict], pad_min: float) -> tuple[datetime, datetime]:
    times = [datetime.fromisoformat(r["ts"]) for r in records if r.get("ts")]
    start = min(times) - timedelta(minutes=pad_min)
    end = max(times) + timedelta(minutes=pad_min)
    return start, end


def _client_summary(records: list[dict]) -> None:
    console.print("\n[bold]Client-side latency by feed × phase × cohort[/bold]")
    table = Table(box=None)
    table.add_column("feed")
    table.add_column("phase")
    table.add_column("cohort")
    table.add_column("count", justify="right")
    table.add_column("errors", justify="right")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("p99", justify="right")

    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in records:
        groups[(r.get("feed", "?"), r.get("phase", "?"), r.get("cohort", "?"))].append(r)

    for feed, phase, cohort in sorted(groups):
        rows = groups[(feed, phase, cohort)]
        errors = sum(1 for r in rows if r.get("status") != 200 or r.get("error"))
        lat = percentiles([r["latency_ms"] for r in rows])
        table.add_row(
            feed,
            phase,
            cohort,
            str(len(rows)),
            str(errors),
            f"{lat[50]:.0f}",
            f"{lat[95]:.0f}",
            f"{lat[99]:.0f}",
        )
    console.print(table)

    total = len(records)
    errors = sum(1 for r in records if r.get("status") != 200 or r.get("error"))
    timeouts = sum(1 for r in records if r.get("status") == 0)
    console.print(
        f"[dim]total requests {total}, errors {errors} "
        f"({errors / total:.1%}), client timeouts/network errors {timeouts}[/dim]"
    )


# OTel resource attribute service.namespace (set to the environment in
# metrics.py) surfaces as this monitored-resource label in Cloud Monitoring.
# It separates stage from prod, which share the project and the metric type.
ENV_RESOURCE_LABEL = "namespace"
METRIC_PREFIX = "custom.googleapis.com/greenearth-api"

# The percentiles to pull, mapped to Cloud Monitoring cross-series reducers.
# feed.render.duration_ms is exported as a CUMULATIVE DISTRIBUTION, and the
# ALIGN_PERCENTILE_* *aligners* reject that metric kind. Instead we align with
# ALIGN_DELTA (valid on cumulative → a per-period delta distribution) and take
# the percentile with a REDUCE_PERCENTILE_* *reducer*, which accepts
# distribution-typed series. It's all server-side, so we never reconstruct the
# distribution client-side (no cumulative double-counting, no mean-of-means);
# and reducing merges the per-instance distributions before taking the
# percentile, rather than averaging per-instance percentiles.
_PERCENTILE_REDUCERS = {
    50: "REDUCE_PERCENTILE_50",
    95: "REDUCE_PERCENTILE_95",
    99: "REDUCE_PERCENTILE_99",
}


def build_percentile_request(
    monitoring_v3,
    project: str,
    metric_type: str,
    env: str,
    interval,
    percentile: int,
    alignment_seconds: int,
):
    """Build a ListTimeSeries request for one percentile, grouped by traffic class.

    Each series is aligned with ALIGN_DELTA over the whole window (converting the
    cumulative distribution to a single delta distribution per series), then
    reduced across series (e.g. per-instance) within each ``traffic`` label to the
    requested percentile. Filtered to one environment so stage and prod — which
    write the same metric type to the same project — are never mixed.
    """
    reducer = getattr(monitoring_v3.Aggregation.Reducer, _PERCENTILE_REDUCERS[percentile])
    aggregation = monitoring_v3.Aggregation(
        alignment_period={"seconds": alignment_seconds},
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_DELTA,
        cross_series_reducer=reducer,
        group_by_fields=["metric.label.traffic"],
    )
    return monitoring_v3.ListTimeSeriesRequest(
        name=f"projects/{project}",
        filter=f'metric.type = "{metric_type}" AND resource.labels.{ENV_RESOURCE_LABEL} = "{env}"',
        interval=interval,
        aggregation=aggregation,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )


def _server_metrics(project: str, env: str, start: datetime, end: datetime) -> None:
    try:
        from google.cloud import monitoring_v3
    except ImportError:
        console.print("[yellow]google-cloud-monitoring not installed; skipping metrics.[/yellow]")
        return

    client = monitoring_v3.MetricServiceClient()
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": {"seconds": int(start.timestamp())},
            "end_time": {"seconds": int(end.timestamp())},
        }
    )
    # One alignment bucket spanning the whole window: each series collapses to a
    # single aligned percentile point.
    alignment_seconds = max(60, int((end - start).total_seconds()))
    metric = f"{METRIC_PREFIX}/feed.render.duration_ms"

    console.print(
        f"\n[bold]Server-side feed.render.duration_ms by traffic class[/bold] "
        f"[dim]({env})[/dim]"
    )

    # traffic -> {percentile -> value}
    by_traffic: dict[str, dict[int, float]] = defaultdict(dict)
    try:
        for percentile in _PERCENTILE_REDUCERS:
            request = build_percentile_request(
                monitoring_v3, project, metric, env, interval, percentile, alignment_seconds
            )
            for series in client.list_time_series(request=request):
                traffic = series.metric.labels.get("traffic", "(unlabeled)")
                if series.points:
                    # Mean across aligned points (usually one) for this series.
                    vals = [p.value.double_value for p in series.points]
                    by_traffic[traffic][percentile] = sum(vals) / len(vals)
    except Exception as exc:  # pragma: no cover - network dependent
        console.print(f"[yellow]Cloud Monitoring query failed: {exc}[/yellow]")
        return

    if not by_traffic:
        console.print(
            "[yellow]No feed.render.duration_ms series in the window "
            f"(check the '{ENV_RESOURCE_LABEL}' resource label matches '{env}').[/yellow]"
        )
        return

    table = Table(box=None)
    table.add_column("traffic")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("p99", justify="right")
    for traffic in sorted(by_traffic):
        vals = by_traffic[traffic]
        table.add_row(
            traffic,
            f"{vals.get(50, 0):.0f}",
            f"{vals.get(95, 0):.0f}",
            f"{vals.get(99, 0):.0f}",
        )
    console.print(table)


def _server_logs(service: str, project: str, start: datetime, end: datetime) -> None:
    console.print("\n[bold]Cloud Run log counts in the window[/bold]")
    base_filter = (
        f'resource.type="cloud_run_revision" '
        f'resource.labels.service_name="{service}" '
        f'timestamp>="{start.isoformat()}" timestamp<="{end.isoformat()}"'
    )
    for label, extra in (
        ("errors (severity>=ERROR)", "severity>=ERROR"),
        ("slow_es_query", 'textPayload=~"slow_es_query"'),
    ):
        try:
            out = subprocess.check_output(
                [
                    "gcloud",
                    "logging",
                    "read",
                    f"{base_filter} {extra}",
                    "--project",
                    project,
                    "--format",
                    "value(timestamp)",
                    "--limit",
                    "1000",
                ],
                text=True,
                env=gcloud_env(),
            )
            count = len([ln for ln in out.splitlines() if ln.strip()])
            console.print(f"  {label}: [bold]{count}[/bold]")
        except subprocess.CalledProcessError as exc:  # pragma: no cover
            console.print(f"  {label}: [yellow]query failed ({exc})[/yellow]")


def run(args: argparse.Namespace) -> None:
    records = _read_records(args.results)
    start, end = _window(records, args.pad_min)
    if args.start:
        start = datetime.fromisoformat(args.start)
    if args.end:
        end = datetime.fromisoformat(args.end)
    console.print(
        f"[dim]Window: {start.astimezone(timezone.utc).isoformat()} → "
        f"{end.astimezone(timezone.utc).isoformat()}[/dim]"
    )

    _client_summary(records)

    url = dashboard_url(args.environment, start, end)
    if url:
        console.print(f"\n[bold]Dashboard (run window pre-set):[/bold] {url}")
    else:
        console.print(
            "\n[yellow]No dashboard ID recorded for this environment — "
            "run monitoring/deploy.sh first.[/yellow]"
        )

    if args.no_server:
        return
    _server_metrics(args.project, args.environment, start, end)
    _server_logs(CLOUD_RUN_SERVICES[args.environment], args.project, start, end)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a Green Earth load-test run")
    parser.add_argument("--results", nargs="+", required=True, help="JSONL file(s) from run.py")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["stage", "prod"],
        default="stage",
    )
    parser.add_argument("--project", default=GCP_PROJECT)
    parser.add_argument("--start", help="ISO override for window start")
    parser.add_argument("--end", help="ISO override for window end")
    parser.add_argument("--pad-min", type=float, default=2.0, help="Padding around results window")
    parser.add_argument("--no-server", action="store_true", help="Client-side only")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

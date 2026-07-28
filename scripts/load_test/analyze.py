#!/usr/bin/env python3
"""Analyze a load-test run (issue api#189).

Reports client-side latency/error stats from the generator's JSONL output and
server-side signals from Cloud Monitoring and Cloud Run logs, comparing
load-test traffic against real traffic over the run window.

This script deliberately reads **nothing from Firestore** — all cohort/DID
context it needs is stamped on every record in the results file — so it can be
run after scripts/load_test/cleanup.py has already removed the test data.

Run from the api/ directory (server-side sections need
``gcloud auth application-default login``):

    pipenv run python scripts/load_test/analyze.py --results results.jsonl \
        --environment stage

``--no-server`` skips the Cloud Monitoring / logs sections (client-side only).
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

from load_test.lib import CLOUD_RUN_SERVICES, GCP_PROJECT, percentiles

console = Console()

METRIC_PREFIX = "custom.googleapis.com/greenearth-api"


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
    console.print("\n[bold]Client-side latency by phase × cohort[/bold]")
    table = Table(box=None)
    table.add_column("phase")
    table.add_column("cohort")
    table.add_column("count", justify="right")
    table.add_column("errors", justify="right")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("p99", justify="right")

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in records:
        groups[(r.get("phase", "?"), r.get("cohort", "?"))].append(r)

    for phase, cohort in sorted(groups):
        rows = groups[(phase, cohort)]
        errors = sum(1 for r in rows if r.get("status") != 200 or r.get("error"))
        lat = percentiles([r["latency_ms"] for r in rows])
        table.add_row(
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


def _server_metrics(project: str, start: datetime, end: datetime) -> None:
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

    console.print("\n[bold]Server-side feed.render.duration_ms by traffic class[/bold]")
    table = Table(box=None)
    table.add_column("traffic")
    table.add_column("samples", justify="right")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("p99", justify="right")

    metric = f"{METRIC_PREFIX}/feed.render.duration_ms"
    request = monitoring_v3.ListTimeSeriesRequest(
        name=f"projects/{project}",
        filter=f'metric.type = "{metric}"',
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )
    by_traffic: dict[str, list[float]] = defaultdict(list)
    try:
        for series in client.list_time_series(request=request):
            traffic = series.metric.labels.get("traffic", "(unlabeled)")
            for point in series.points:
                dist = point.value.distribution_value
                # Approximate: use the distribution mean weighted by count.
                if dist.count:
                    by_traffic[traffic].extend([dist.mean] * min(int(dist.count), 1000))
    except Exception as exc:  # pragma: no cover - network dependent
        console.print(f"[yellow]Cloud Monitoring query failed: {exc}[/yellow]")
        return

    if not by_traffic:
        console.print("[yellow]No feed.render.duration_ms series in the window.[/yellow]")
        return
    for traffic in sorted(by_traffic):
        vals = by_traffic[traffic]
        lat = percentiles(vals)
        table.add_row(
            traffic,
            str(len(vals)),
            f"{lat[50]:.0f}",
            f"{lat[95]:.0f}",
            f"{lat[99]:.0f}",
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

    if args.no_server:
        return
    _server_metrics(args.project, start, end)
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

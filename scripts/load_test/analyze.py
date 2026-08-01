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
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scripts/

from load_test.lib import GCP_PROJECT, dashboard_url, percentiles

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
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

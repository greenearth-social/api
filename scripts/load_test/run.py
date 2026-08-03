#!/usr/bin/env python3
"""Generate simulated feed load against the Green Earth API (issue api#189).

Drives ``getFeedSkeleton`` (plus cursor paging and ``sendInteractions``) for a
set of real user DIDs selected by scripts/load_test/select_users.py. Requests carry the
load-test bypass headers (``X-Load-Test-Secret`` / ``X-Load-Test-DID``) so the
server skips AT Protocol auth, tags all resulting data as test traffic, and
skips analytics — see load_test_did in src/app/routers/xrpc.py.

This script *generates* load, records raw per-request results to a JSONL file,
and — unless ``--skip-cleanup`` is given — deletes the data it created by
invoking scripts/load_test/cleanup.py --execute when the run finishes (with
``--skip-cleanup`` it prints that command for you to run later). Analyze the
results afterwards with scripts/load_test/analyze.py, which reads only the JSONL
file (and Cloud Monitoring / logs) and never touches Firestore — so cleanup
running first never affects analysis.

Run from the api/ directory:

    pipenv run python scripts/load_test/run.py --users load_test_users.json \
        --environment stage --rate 60 --duration 10 --out results.jsonl

The secret is read from --secret, else $GE_LOAD_TEST_SECRET, else
``gcloud secrets versions access latest --secret=load-test-secret-<env>``.
``--dry-run`` prints the planned schedule and makes no requests.

WARNING: this disrupts real production traffic. Run against prod only in a
low-traffic window, and start small.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timezone

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scripts/

from load_test.lib import (
    CLOUD_RUN_REGION,
    CLOUD_RUN_SERVICES,
    assign_feeds,
    build_interactions,
    feed_uri_from_describe,
    gcloud_env,
    interactions_request_body,
    parse_feed_spec,
    percentiles,
    sample_page_depth,
    session_start_offsets,
    weighted_cohort_choice,
)

console = Console()

# Soft guardrail: above this sessions/minute, require --force so a fat-fingered
# rate can't accidentally hammer prod.
RATE_CONFIRM_THRESHOLD = 300


def _resolve_api_url(args: argparse.Namespace) -> str:
    if args.api_url:
        return args.api_url.rstrip("/")
    service = CLOUD_RUN_SERVICES[args.environment]
    out = subprocess.check_output(
        [
            "gcloud",
            "run",
            "services",
            "describe",
            service,
            "--region",
            CLOUD_RUN_REGION,
            "--format",
            "value(status.url)",
        ],
        text=True,
        env=gcloud_env(),
    ).strip()
    if not out:
        raise SystemExit(f"Could not resolve Cloud Run URL for {service}")
    return out.rstrip("/")


def _resolve_secret(args: argparse.Namespace) -> str:
    if args.secret:
        return args.secret
    env_secret = os.environ.get("GE_LOAD_TEST_SECRET")
    if env_secret:
        return env_secret
    secret_name = f"load-test-secret-{args.environment}"
    try:
        return subprocess.check_output(
            ["gcloud", "secrets", "versions", "access", "latest", "--secret", secret_name],
            text=True,
            env=gcloud_env(),
        ).strip()
    except subprocess.CalledProcessError as exc:  # pragma: no cover - env dependent
        raise SystemExit(f"Could not read {secret_name} from Secret Manager: {exc}") from exc


def _load_users(path: str) -> list[dict]:
    with open(path) as f:
        data = json.load(f)
    users = data.get("users", [])
    if not users:
        raise SystemExit(f"No users in {path}")
    return users


class ResultWriter:
    """Serialises one JSONL record per HTTP call, guarded by a lock."""

    def __init__(self, path: str) -> None:
        self._f = open(path, "w")
        self._lock = asyncio.Lock()
        self.records: list[dict] = []

    async def write(self, record: dict) -> None:
        async with self._lock:
            self.records.append(record)
            self._f.write(json.dumps(record) + "\n")
            self._f.flush()

    def close(self) -> None:
        self._f.close()


async def _timed_get(
    client: httpx.AsyncClient, url: str, params: dict, headers: dict
) -> tuple[int, float, dict | None, str | None]:
    start = time.monotonic()
    try:
        resp = await client.get(url, params=params, headers=headers)
        latency_ms = (time.monotonic() - start) * 1000
        body = resp.json() if resp.status_code == 200 else None
        return resp.status_code, latency_ms, body, None
    except Exception as exc:  # network error / timeout
        latency_ms = (time.monotonic() - start) * 1000
        return 0, latency_ms, None, type(exc).__name__


async def run_session(
    session_id: int,
    user: dict,
    *,
    client: httpx.AsyncClient,
    api_url: str,
    feed_uri: str,
    headers_for: dict,
    limit: int,
    rng: random.Random,
    interaction_share: float,
    think_time_ms: float,
    writer: ResultWriter,
) -> None:
    """One simulated user session: initial fetch, some paging, maybe interactions."""
    did = user["did"]
    cohort = user.get("cohort", "unknown")
    feed_rkey = user.get("feed", "?")
    headers = {**headers_for, "X-Load-Test-DID": did}
    skeleton_url = f"{api_url}/xrpc/app.bsky.feed.getFeedSkeleton"
    interactions_url = f"{api_url}/xrpc/app.bsky.feed.sendInteractions"

    depth = sample_page_depth(rng, args_mean_pages())
    cursor: str | None = None
    collected_items: list[dict] = []

    for page_index in range(depth):
        params: dict = {"feed": feed_uri, "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        status, latency_ms, body, error = await _timed_get(client, skeleton_url, params, headers)
        num_items = len(body.get("feed", [])) if body else 0
        cursor = body.get("cursor") if body else None
        if body:
            collected_items.extend(body.get("feed", []))
        await writer.write(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "did": did,
                "cohort": cohort,
                "feed": feed_rkey,
                "session_id": session_id,
                "phase": "initial" if page_index == 0 else "page",
                "page_index": page_index,
                "status": status,
                "latency_ms": round(latency_ms, 1),
                "num_items": num_items,
                "has_cursor": cursor is not None,
                "error": error,
            }
        )
        if cursor is None or num_items == 0:
            break
        if page_index < depth - 1:
            await asyncio.sleep(_jittered_think(rng, think_time_ms))

    # Optionally report interactions for the served items.
    if collected_items and rng.random() < interaction_share:
        specs = build_interactions(collected_items, rng)
        if specs:
            body_json = interactions_request_body(specs)
            start = time.monotonic()
            try:
                resp = await client.post(interactions_url, json=body_json, headers=headers)
                latency_ms = (time.monotonic() - start) * 1000
                status, error = resp.status_code, None
            except Exception as exc:
                latency_ms = (time.monotonic() - start) * 1000
                status, error = 0, type(exc).__name__
            await writer.write(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "did": did,
                    "cohort": cohort,
                    "feed": feed_rkey,
                    "session_id": session_id,
                    "phase": "interactions",
                    "page_index": None,
                    "status": status,
                    "latency_ms": round(latency_ms, 1),
                    "num_items": len(specs),
                    "has_cursor": False,
                    "error": error,
                }
            )


# --mean-pages / --think-time are read once into module state so run_session
# (spawned many times) doesn't need them threaded through every call.
_MEAN_PAGES = 3.0


def args_mean_pages() -> float:
    return _MEAN_PAGES


def _jittered_think(rng: random.Random, think_time_ms: float) -> float:
    if think_time_ms <= 0:
        return 0.0
    # ±50% uniform jitter, in seconds.
    return rng.uniform(think_time_ms * 0.5, think_time_ms * 1.5) / 1000.0


async def run(args: argparse.Namespace) -> None:
    global _MEAN_PAGES
    _MEAN_PAGES = args.mean_pages

    users = _load_users(args.users)
    try:
        feed_weights = parse_feed_spec(args.feed)
    except ValueError as exc:
        raise SystemExit(f"Invalid --feed spec: {exc}") from exc

    rng = random.Random(args.seed)
    # Pin each user to one feed (weighted buckets) before scheduling, so a user's
    # sessions all hit the same feed the way a real user mostly sticks to one.
    assign_feeds(users, feed_weights, rng)
    offsets = session_start_offsets(args.rate, args.duration, rng)

    if args.rate > RATE_CONFIRM_THRESHOLD and not args.force:
        raise SystemExit(
            f"--rate {args.rate}/min exceeds the {RATE_CONFIRM_THRESHOLD}/min guardrail; "
            f"re-run with --force if that is intended."
        )

    console.print(
        f"[bold]Plan:[/bold] {len(offsets)} sessions over {args.duration} min "
        f"(~{args.rate}/min), concurrency {args.concurrency}, {len(users)} users"
    )
    _print_feed_plan(users, feed_weights)

    if args.dry_run:
        table = Table(title="First 10 scheduled sessions", title_justify="left")
        table.add_column("#", justify="right")
        table.add_column("start (s)", justify="right")
        for i, off in enumerate(offsets[:10]):
            table.add_row(str(i), f"{off:.2f}")
        console.print(table)
        console.print("[yellow]--dry-run: no requests sent.[/yellow]")
        return

    api_url = _resolve_api_url(args)
    secret = _resolve_secret(args)
    headers_for = {"X-Load-Test-Secret": secret}
    console.print(f"[dim]API: {api_url}[/dim]")

    writer = ResultWriter(args.out)
    semaphore = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(max_connections=args.concurrency * 2)
    async with httpx.AsyncClient(timeout=args.timeout, limits=limits, verify=True) as client:
        # Resolve each requested feed's AT URI by rkey from describeFeedGenerator.
        describe = await client.get(f"{api_url}/xrpc/app.bsky.feed.describeFeedGenerator")
        described = describe.json()
        feed_uris: dict[str, str] = {}
        missing: list[str] = []
        for rkey, _ in feed_weights:
            uri = feed_uri_from_describe(described, rkey)
            if uri is None:
                missing.append(rkey)
            else:
                feed_uris[rkey] = uri
        if missing:
            raise SystemExit(
                f"Feed rkey(s) not found in describeFeedGenerator: {', '.join(missing)}"
            )
        for rkey, uri in feed_uris.items():
            console.print(f"[dim]Feed {rkey}: {uri}[/dim]")

        start_wall = time.monotonic()
        progress = Progress(
            TextColumn("[bold]running"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("sessions"),
            TimeElapsedColumn(),
            TextColumn("[dim]eta[/dim]"),
            TimeRemainingColumn(),
            console=console,
            # Off explicitly with --no-progress, and auto-off when stdout isn't a
            # terminal (piped to a file / CI) so we don't spew control codes.
            disable=True if args.no_progress else None,
        )

        with progress:
            task_id = progress.add_task("run", total=len(offsets))

            async def _launch(session_id: int, delay: float) -> None:
                now = time.monotonic() - start_wall
                if delay > now:
                    await asyncio.sleep(delay - now)
                async with semaphore:
                    user = weighted_cohort_choice(rng, users)
                    await run_session(
                        session_id,
                        user,
                        client=client,
                        api_url=api_url,
                        feed_uri=feed_uris[user["feed"]],
                        headers_for=headers_for,
                        limit=args.limit,
                        rng=random.Random(rng.random()),
                        interaction_share=args.interaction_share,
                        think_time_ms=args.think_time_ms,
                        writer=writer,
                    )
                progress.advance(task_id)

            tasks = [asyncio.create_task(_launch(i, off)) for i, off in enumerate(offsets)]
            await asyncio.gather(*tasks)

    writer.close()
    _print_summary(writer.records)
    console.print(f"[green]Wrote {args.out} ({len(writer.records)} records)[/green]")

    _run_cleanup(args)


def _cleanup_command(args: argparse.Namespace) -> list[str]:
    """The cleanup invocation matching this run — deletes everything it created."""
    cleanup_py = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cleanup.py")
    return [
        sys.executable,
        cleanup_py,
        "--environment",
        args.environment,
        "--users",
        args.users,
        "--execute",
    ]


def _cleanup_display(args: argparse.Namespace) -> str:
    """Copy-pasteable form of the cleanup command (relative, no venv python path)."""
    return (
        "pipenv run python scripts/load_test/cleanup.py "
        f"--environment {args.environment} --users {args.users} --execute"
    )


def _run_cleanup(args: argparse.Namespace) -> None:
    """Delete this run's data unless --skip-cleanup; if skipped, show how to later."""
    if args.skip_cleanup:
        console.print(
            "\n[yellow]--skip-cleanup: test data left in Firestore. "
            "Remove it later with:[/yellow]\n"
            f"  [bold]{_cleanup_display(args)}[/bold]"
        )
        return
    console.print(
        "\n[bold]Cleaning up test data[/bold] [dim](pass --skip-cleanup to keep it)[/dim]"
    )
    result = subprocess.run(_cleanup_command(args))
    if result.returncode != 0:
        console.print(
            f"[red]Cleanup failed (exit {result.returncode}). Re-run manually:[/red]\n"
            f"  [bold]{_cleanup_display(args)}[/bold]"
        )


def _print_feed_plan(users: list[dict], feed_weights: list[tuple[str, float]]) -> None:
    assigned: dict[str, int] = {}
    for u in users:
        assigned[u.get("feed", "?")] = assigned.get(u.get("feed", "?"), 0) + 1
    total_w = sum(w for _, w in feed_weights)
    table = Table(title="Feed assignment", title_justify="left")
    table.add_column("feed")
    table.add_column("share", justify="right")
    table.add_column("users", justify="right")
    for rkey, weight in feed_weights:
        table.add_row(rkey, f"{weight / total_w:.0%}", str(assigned.get(rkey, 0)))
    console.print(table)


def _print_summary(records: list[dict]) -> None:
    table = Table(title="Load-test client-side summary", title_justify="left")
    table.add_column("phase")
    table.add_column("count", justify="right")
    table.add_column("errors", justify="right")
    table.add_column("p50 ms", justify="right")
    table.add_column("p95 ms", justify="right")
    table.add_column("p99 ms", justify="right")
    for phase in ("initial", "page", "interactions"):
        rows = [r for r in records if r["phase"] == phase]
        if not rows:
            continue
        errors = sum(1 for r in rows if r["status"] != 200 or r["error"])
        lat = percentiles([r["latency_ms"] for r in rows])
        table.add_row(
            phase,
            str(len(rows)),
            str(errors),
            f"{lat[50]:.0f}",
            f"{lat[95]:.0f}",
            f"{lat[99]:.0f}",
        )
    console.print(table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate simulated feed load")
    parser.add_argument("--users", required=True, help="JSON file from select_users.py")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["stage", "prod"],
        default="stage",
    )
    parser.add_argument("--api-url", help="Override API base URL (else resolved via gcloud)")
    parser.add_argument(
        "--feed",
        default="your-feed",
        help="Feed rkey, or several with weights: 'your-feed:90,random:10'. Each "
        "user is pinned to one feed, bucketed across users by weight (default your-feed).",
    )
    parser.add_argument("--rate", type=float, default=60, help="Sessions per minute (default 60)")
    parser.add_argument("--duration", type=float, default=10, help="Minutes (default 10)")
    parser.add_argument("--concurrency", type=int, default=40, help="Max in-flight sessions")
    parser.add_argument("--limit", type=int, default=30, help="Feed page size (default 30)")
    parser.add_argument("--mean-pages", type=float, default=3.0, help="Mean pages per session")
    parser.add_argument("--think-time-ms", type=float, default=2000, help="Between-page pause")
    parser.add_argument(
        "--interaction-share", type=float, default=0.5, help="Sessions that interact"
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout (s)")
    parser.add_argument("--secret", help="Load-test secret (else env / Secret Manager)")
    parser.add_argument("--out", default="results.jsonl")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--no-progress", action="store_true", help="Hide the progress bar")
    parser.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Don't delete the run's data afterwards (cleanup runs by default); "
        "prints the cleanup command to run later instead.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan, send nothing")
    parser.add_argument("--force", action="store_true", help="Bypass the high-rate guardrail")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

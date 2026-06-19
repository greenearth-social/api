#!/usr/bin/env python3
"""Replay slow ES queries from Cloud Run logs with profile: true.

Reads NDJSON from stdin (output of scripts/pull_slow_es_queries.sh), replays
each query against an Elasticsearch cluster with profile: true injected, and
renders a per-query breakdown via rich.

Run from the api/ directory (same convention as scripts/feed_debug.py):

    ./scripts/pull_slow_es_queries.sh --environment prod | \\
        pipenv run python scripts/profile_es_queries.py

    # Or from a saved file:
    pipenv run python scripts/profile_es_queries.py < /tmp/slow_queries_prod.ndjson

    # Dry-run: parse and display queries without hitting ES
    pipenv run python scripts/profile_es_queries.py --dry-run

    # Show top N slowest only:
    pipenv run python scripts/profile_es_queries.py --top 10

Environment variables required (unless --dry-run):
    GE_ELASTICSEARCH_URL      e.g. https://localhost:9200
    GE_ELASTICSEARCH_API_KEY  your ES API key
    GE_ELASTICSEARCH_VERIFY_SSL  set to true if your cert is valid (default: false)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

console = Console()

# ──────────────────────────── log parsing ────────────────────────────────────

_LOG_RE = re.compile(
    r"slow_es_query\s+"
    r"rid=(\S+)\s+"
    r"elapsed_ms=([\d.]+)\s+"
    r"index=(\S+)\s+"
    r"body=(.+)$",
    re.DOTALL,
)


def parse_log_line(payload: str) -> dict | None:
    """Parse one ``slow_es_query`` log payload.

    Returns a dict with keys ``rid``, ``elapsed_ms``, ``index``, ``body``
    (parsed JSON) or ``None`` if the line isn't a slow-query entry.
    """
    m = _LOG_RE.search(payload)
    if not m:
        return None
    rid, elapsed_ms_str, index, body_str = m.groups()
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError:
        return None
    return {
        "rid": rid,
        "elapsed_ms": float(elapsed_ms_str),
        "index": index,
        "body": body,
    }


def inject_profile(body: dict) -> dict:
    """Return a copy of *body* with ``"profile": true`` added."""
    return {**body, "profile": True}


# ──────────────────────────── profile analysis ───────────────────────────────

def summarise_profile(profile: dict) -> dict:
    """Reduce the raw ES profile response to the numbers we care about.

    Returns:
        num_shards    — number of shards in the response
        max_query_ms  — highest per-shard query-phase time in ms
        max_fetch_ms  — highest per-shard fetch-phase time in ms
        slowest_shard — id of the shard with the highest query time
        shard_details — list of per-shard dicts for the table
    """
    shards = profile.get("shards", [])
    shard_details = []
    for shard in shards:
        sid = shard.get("id", "?")
        query_ns = sum(
            q.get("time_in_nanos", 0)
            for search in shard.get("searches", [])
            for q in search.get("query", [])
        )
        fetch_ns = shard.get("fetch", {}).get("time_in_nanos", 0)
        shard_details.append({
            "id": sid,
            "query_ms": query_ns / 1_000_000,
            "fetch_ms": fetch_ns / 1_000_000,
        })

    max_query_ms = max((s["query_ms"] for s in shard_details), default=0.0)
    max_fetch_ms = max((s["fetch_ms"] for s in shard_details), default=0.0)
    slowest_shard = next(
        (s["id"] for s in shard_details if s["query_ms"] == max_query_ms), "?"
    )

    return {
        "num_shards": len(shards),
        "max_query_ms": max_query_ms,
        "max_fetch_ms": max_fetch_ms,
        "slowest_shard": slowest_shard,
        "shard_details": shard_details,
    }


def _breakdown_table(profile: dict) -> Table | None:
    """Build a rich Table of per-shard query-phase breakdown for the slowest shard."""
    shards = profile.get("shards", [])
    if not shards:
        return None

    def _shard_query_ns(s):
        return sum(
            q.get("time_in_nanos", 0)
            for search in s.get("searches", [])
            for q in search.get("query", [])
        )

    slowest = max(shards, key=_shard_query_ns)
    shard_id = slowest.get("id", "?")

    table = Table(
        title=f"Slowest shard: {shard_id}",
        box=box.SIMPLE_HEAVY,
        title_justify="left",
    )
    table.add_column("query phase", style="cyan")
    table.add_column("description", style="white", max_width=60)
    table.add_column("time (ms)", justify="right", style="yellow")

    for search in slowest.get("searches", []):
        for q in search.get("query", []):
            desc = q.get("description", "")
            time_ms = q.get("time_in_nanos", 0) / 1_000_000
            table.add_row(q.get("type", "query"), desc[:60], f"{time_ms:.1f}")
            for bk, bv in q.get("breakdown", {}).items():
                if isinstance(bv, int) and bv > 0:
                    table.add_row(
                        f"  ↳ {bk}",
                        "",
                        f"{bv / 1_000_000:.2f}" if "count" not in bk else str(bv),
                    )

    fetch = slowest.get("fetch", {})
    if fetch:
        fetch_ms = fetch.get("time_in_nanos", 0) / 1_000_000
        table.add_row("fetch", "", f"{fetch_ms:.1f}", style="magenta")

    return table


# ──────────────────────────── ES replay ──────────────────────────────────────

async def replay_with_profile(entry: dict) -> dict | None:
    """Replay one slow-query entry against ES with profile: true, return response."""
    from elasticsearch import AsyncElasticsearch

    url = os.environ.get("GE_ELASTICSEARCH_URL", "https://localhost:9200")
    api_key = os.environ.get("GE_ELASTICSEARCH_API_KEY", "")
    verify = os.environ.get("GE_ELASTICSEARCH_VERIFY_SSL", "false").lower() in ("1", "true")

    es = AsyncElasticsearch(
        hosts=[url],
        api_key=api_key,
        verify_certs=verify,
        request_timeout=60,
    )
    try:
        profiled_body = inject_profile(entry["body"])
        resp = await es.search(index=entry["index"], **profiled_body)
        return dict(resp)
    except Exception as exc:
        console.print(f"[red]  Replay failed: {exc}[/red]")
        return None
    finally:
        await es.close()


# ──────────────────────────── rendering ──────────────────────────────────────

def _render_entry(entry: dict, profile_resp: dict | None, pos: int, total: int) -> None:
    """Print one slow-query entry with its profile summary."""
    rid = entry["rid"]
    elapsed = entry["elapsed_ms"]
    index = entry["index"]
    query_type = "knn" if "knn" in entry["body"] else "query"
    ts = entry.get("timestamp", "")
    title_ts = f"  ts={ts}" if ts else ""

    header = (
        f"[{pos}/{total}] rid={rid}  elapsed={elapsed:.0f} ms  "
        f"index={index}  type={query_type}{title_ts}"
    )

    if profile_resp is None:
        console.print(Panel("[red]No profile (replay failed or --dry-run)[/red]", title=header))
    else:
        raw_profile = profile_resp.get("profile", {})
        summary = summarise_profile(raw_profile)
        body_text = (
            f"shards={summary['num_shards']}  "
            f"max_query={summary['max_query_ms']:.0f} ms  "
            f"max_fetch={summary['max_fetch_ms']:.0f} ms  "
            f"slowest_shard={summary['slowest_shard']}"
        )
        console.print(Panel(body_text, title=header, border_style="blue"))

        table = _breakdown_table(raw_profile)
        if table:
            console.print(table)

    console.print(
        Panel(
            json.dumps(entry["body"], indent=2)[:2000],
            title="[dim]query body (curl -X POST $GE_ELASTICSEARCH_URL/<index>/_search)[/dim]",
            border_style="grey23",
        )
    )
    console.print()


# ──────────────────────────── main ───────────────────────────────────────────

def _load_entries(source) -> list[dict]:
    """Parse NDJSON log entries from *source*. Returns list of parsed entries."""
    entries = []
    for line in source:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = obj.get("payload", "")
        parsed = parse_log_line(payload)
        if parsed:
            parsed["timestamp"] = obj.get("timestamp", "")
            entries.append(parsed)
    return entries


async def _main_async(args) -> None:
    entries = _load_entries(sys.stdin)

    if not entries:
        console.print("[yellow]No slow_es_query entries found in input.[/yellow]")
        sys.exit(0)

    entries.sort(key=lambda e: e["elapsed_ms"], reverse=True)
    if args.top:
        entries = entries[: args.top]

    console.print(
        f"[bold]Profiling {len(entries)} slow queries[/bold]  "
        f"(sorted by elapsed_ms desc)"
    )
    console.print()

    for i, entry in enumerate(entries, 1):
        if args.dry_run:
            _render_entry(entry, None, i, len(entries))
            continue
        console.print(f"[dim]Replaying {i}/{len(entries)} ({entry['elapsed_ms']:.0f} ms)...[/dim]")
        resp = await replay_with_profile(entry)
        _render_entry(entry, resp, i, len(entries))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay slow ES queries from Cloud Run log NDJSON with profile: true"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and display queries without replaying against ES",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        metavar="N",
        help="Only profile the N slowest queries",
    )
    args = parser.parse_args()

    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()

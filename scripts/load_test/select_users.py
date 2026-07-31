#!/usr/bin/env python3
"""Select a diverse set of real Bluesky users for a load test (issue api#189).

Produces a JSON file of user DIDs split across three cohorts so a run exercises
both warm and cold serving paths:

  existing  our own feed users (from the Firestore ``users`` collection)
  active    active Bluesky users with recent like history in Elasticsearch but
            no Firestore document yet — warm ES, cold Firestore
  low       users with little or no like history — the fully cold path

Run from the api/ directory (Elasticsearch reached via a port-forward — see
below):

    kubectl port-forward service/greenearth-es-http 9200:9200 -n greenearth-prod
    pipenv run python scripts/load_test/select_users.py --environment prod \
        --es-password "$ELASTIC_PASSWORD" --count 100 --output load_test_users.json

Firestore connection comes from the same env vars as the API server
(GE_FIRESTORE_PROJECT, GE_FIRESTORE_DATABASE); ``--environment`` sets them for
stage/prod just like scripts/feed_debug.py. Elasticsearch auth is either
``--es-api-key`` or ``--es-user``/``--es-password`` (the ``elastic`` user's
password from the ``greenearth-es-elastic-user`` secret).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))  # src/

from app.lib.firestore import USERS_COLLECTION, init_firestore_client
from load_test.lib import COHORTS, GCP_PROJECT, split_counts

console = Console()

LIKES_INDEX = "likes"
POSTS_INDEX = "posts_recent"

_FIRESTORE_DATABASES = {"stage": "greenearth-stage", "prod": "greenearth-prod"}


def _configure_environment(env: str) -> None:
    """Point Firestore at the chosen environment in-process (see feed_debug.py)."""
    if env == "dev":
        return
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = _FIRESTORE_DATABASES[env]
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)
    console.print(f"[dim]→ {env} (database {_FIRESTORE_DATABASES[env]})[/dim]")


def _es_client(args: argparse.Namespace) -> Elasticsearch:
    if args.es_api_key:
        return Elasticsearch(args.es_url, api_key=args.es_api_key, verify_certs=args.es_verify)
    if args.es_password:
        return Elasticsearch(
            args.es_url,
            basic_auth=(args.es_user, args.es_password),
            verify_certs=args.es_verify,
        )
    raise SystemExit("Elasticsearch auth required: pass --es-api-key or --es-password")


async def _existing_users(db) -> list[dict]:
    """All our feed users, most-recently-active first, excluding load-test-created."""
    users: list[dict] = []
    async for doc in db.collection(USERS_COLLECTION).stream():
        data = doc.to_dict() or {}
        did = data.get("user_did")
        if not did or data.get("created_by_load_test"):
            continue
        users.append(
            {
                "did": did,
                "last_seen_at": data.get("last_seen_at"),
                "username": data.get("username"),
            }
        )

    def _sort_key(u: dict):
        ts = u.get("last_seen_at")
        return ts if isinstance(ts, datetime) else datetime.min.replace(tzinfo=timezone.utc)

    users.sort(key=_sort_key, reverse=True)
    return users


def _active_likers(es: Elasticsearch, days: int, size: int) -> list[dict]:
    """Users with the most likes in the recent window (network-wide activity signal)."""
    resp = es.search(
        index=LIKES_INDEX,
        size=0,
        query={"range": {"created_at": {"gte": f"now-{days}d"}}},
        aggs={
            "top_users": {
                "terms": {"field": "author_did", "size": size, "order": {"_count": "desc"}}
            }
        },
    )
    buckets = resp["aggregations"]["top_users"]["buckets"]
    return [{"did": b["key"], "like_count": b["doc_count"]} for b in buckets]


def _recent_post_authors(es: Elasticsearch, seed: int, size: int) -> list[str]:
    """Distinct authors of recent posts, randomly sampled (deterministic via seed)."""
    resp = es.search(
        index=POSTS_INDEX,
        size=size,
        _source=["author_did"],
        query={
            "function_score": {
                "query": {"match_all": {}},
                "random_score": {"seed": seed, "field": "_seq_no"},
            }
        },
    )
    seen: list[str] = []
    seen_set: set[str] = set()
    for hit in resp["hits"]["hits"]:
        did = (hit.get("_source") or {}).get("author_did")
        if did and did not in seen_set:
            seen_set.add(did)
            seen.append(did)
    return seen


def _like_counts_for(es: Elasticsearch, dids: list[str], days: int) -> dict[str, int]:
    """Per-DID like counts over the window for a fixed set of authors (0 if absent)."""
    if not dids:
        return {}
    resp = es.search(
        index=LIKES_INDEX,
        size=0,
        query={
            "bool": {
                "filter": [
                    {"terms": {"author_did": dids}},
                    {"range": {"created_at": {"gte": f"now-{days}d"}}},
                ]
            }
        },
        aggs={"by_user": {"terms": {"field": "author_did", "size": len(dids)}}},
    )
    counts = {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_user"]["buckets"]}
    return {did: counts.get(did, 0) for did in dids}


def _sample(rng: random.Random, pool: list, n: int) -> list:
    if n >= len(pool):
        return list(pool)
    return rng.sample(pool, n)


async def run(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    counts = split_counts(args.count, args.pct_existing, args.pct_active, args.pct_low)
    console.print(f"[bold]Target cohorts:[/bold] {counts}")

    db = init_firestore_client()
    existing_pool = await _existing_users(db)
    firestore_dids = {u["did"] for u in existing_pool}
    console.print(f"Firestore users available: {len(existing_pool)}")

    es = _es_client(args)

    # Cohort 1: existing users, drawn from the most-recently-active slice.
    existing_slice = existing_pool[: max(counts["existing"] * 3, counts["existing"])]
    existing_pick = _sample(rng, existing_slice, counts["existing"])

    # Cohort 2: active likers not already ours.
    likers = _active_likers(es, args.days, size=max(counts["active"] * 20, 200))
    active_candidates = [
        u for u in likers if u["did"].startswith("did:plc:") and u["did"] not in firestore_dids
    ]
    active_pick = _sample(rng, active_candidates, counts["active"])

    # Cohort 3: low/no-history — recent post authors filtered to few likes.
    authors = _recent_post_authors(es, args.seed, size=max(counts["low"] * 20, 500))
    author_dids = [d for d in authors if d.startswith("did:plc:") and d not in firestore_dids]
    like_counts = _like_counts_for(es, author_dids, args.days)
    low_candidates = [
        {"did": d, "like_count": like_counts.get(d, 0)}
        for d in author_dids
        if like_counts.get(d, 0) <= args.low_likes_max
    ]
    low_pick = _sample(rng, low_candidates, counts["low"])

    users: list[dict] = []
    for u in existing_pick:
        users.append(
            {"did": u["did"], "cohort": "existing", "like_count": None, "in_firestore": True}
        )
    for u in active_pick:
        users.append(
            {
                "did": u["did"],
                "cohort": "active",
                "like_count": u["like_count"],
                "in_firestore": False,
            }
        )
    for u in low_pick:
        users.append(
            {"did": u["did"], "cohort": "low", "like_count": u["like_count"], "in_firestore": False}
        )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": args.environment,
        "params": {
            "count": args.count,
            "pct_existing": args.pct_existing,
            "pct_active": args.pct_active,
            "pct_low": args.pct_low,
            "days": args.days,
            "low_likes_max": args.low_likes_max,
            "seed": args.seed,
        },
        "users": users,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    table = Table(title=f"Selected {len(users)} users", title_justify="left")
    table.add_column("cohort")
    table.add_column("requested", justify="right")
    table.add_column("selected", justify="right")
    for c in COHORTS:
        got = sum(1 for u in users if u["cohort"] == c)
        table.add_row(c, str(counts[c]), str(got))
    console.print(table)
    if len(users) < args.count:
        console.print(
            f"[yellow]Only {len(users)}/{args.count} users found — some cohort pools were "
            f"too small. Adjust percentages, --days, or --low-likes-max.[/yellow]"
        )
    console.print(f"[green]Wrote {args.output}[/green]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Select users for a Green Earth load test")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["dev", "stage", "prod"],
        default="prod",
        help="Firestore/ES target (default prod). dev uses the local emulator.",
    )
    parser.add_argument("--count", type=int, default=100, help="Total users (default 100)")
    parser.add_argument("--pct-existing", type=int, default=60)
    parser.add_argument("--pct-active", type=int, default=30)
    parser.add_argument("--pct-low", type=int, default=10)
    parser.add_argument("--days", type=int, default=30, help="Like-history window (default 30)")
    parser.add_argument(
        "--low-likes-max",
        type=int,
        default=2,
        help="Max likes in window for the low-history cohort (default 2)",
    )
    parser.add_argument("--es-url", default="https://localhost:9200")
    parser.add_argument("--es-user", default="elastic")
    parser.add_argument("--es-password", default=os.environ.get("ELASTIC_PASSWORD"))
    parser.add_argument("--es-api-key", default=os.environ.get("GE_ELASTICSEARCH_API_KEY"))
    parser.add_argument(
        "--es-verify",
        action="store_true",
        help="Verify ES TLS certs (off by default; port-forwarded ES uses a self-signed cert)",
    )
    parser.add_argument("--output", default="load_test_users.json")
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()

    _configure_environment(args.environment)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

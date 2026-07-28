#!/usr/bin/env python3
"""Delete data created by load-test traffic (issue api#189).

Removes users that were created *only* by a load test (so the cold-start path
can be re-run for the same DID), plus load-test interactions and feed-cache
entries. A user who has since made a real request has had their
``created_by_load_test`` flag cleared by the API (they "became ours") and is
left untouched — this script re-checks the flag immediately before deleting to
avoid a race.

Runs as a dry-run by default; pass ``--execute`` to actually delete.

Run from the api/ directory:

    pipenv run python scripts/load_test/cleanup.py --environment stage          # dry run
    pipenv run python scripts/load_test/cleanup.py --environment stage --execute

Firestore connection comes from the same env vars as the API server; the
``--environment`` flag sets them for stage/prod (see scripts/feed_debug.py).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from google.cloud.firestore_v1.base_query import FieldFilter
from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))  # src/

from app.lib.feed_cache import FEED_CACHE_COLLECTION
from app.lib.firestore import (
    INTERACTIONS_COLLECTION,
    USERS_COLLECTION,
    init_firestore_client,
    user_doc_id,
)
from load_test.lib import GCP_PROJECT

console = Console()

_FIRESTORE_DATABASES = {"stage": "greenearth-stage", "prod": "greenearth-prod"}


def _configure_environment(env: str) -> None:
    if env == "dev":
        return
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = _FIRESTORE_DATABASES[env]
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)
    console.print(f"[dim]→ {env} (database {_FIRESTORE_DATABASES[env]})[/dim]")


def _restrict_dids(path: str | None) -> set[str] | None:
    if not path:
        return None
    with open(path) as f:
        data = json.load(f)
    return {u["did"] for u in data.get("users", [])}


async def _delete_test_users(db, execute: bool, restrict: set[str] | None) -> tuple[int, int]:
    """Delete users flagged created_by_load_test. Returns (deleted, skipped)."""
    query = db.collection(USERS_COLLECTION).where(
        filter=FieldFilter("created_by_load_test", "==", True)
    )
    deleted = skipped = 0
    table = Table(title="Load-test-created users", title_justify="left")
    table.add_column("did")
    table.add_column("username")
    table.add_column("action")

    async for doc in query.stream():
        data = doc.to_dict() or {}
        did = data.get("user_did", doc.id)
        if restrict is not None and did not in restrict:
            continue
        if not execute:
            table.add_row(did, data.get("username") or "—", "[yellow]would delete[/yellow]")
            deleted += 1
            continue

        # Re-read immediately before deleting: a real request may have cleared
        # the flag between the query and now (the user became ours).
        ref = db.collection(USERS_COLLECTION).document(user_doc_id(did))
        fresh = await ref.get()
        fresh_data = fresh.to_dict() or {}
        if not fresh_data.get("created_by_load_test"):
            table.add_row(did, data.get("username") or "—", "[dim]skipped (now real)[/dim]")
            skipped += 1
            continue
        await db.recursive_delete(ref)
        table.add_row(did, data.get("username") or "—", "[red]deleted[/red]")
        deleted += 1

    console.print(table)
    return deleted, skipped


async def _delete_flagged(db, collection: str, execute: bool) -> int:
    """Delete every document in ``collection`` with load_test == True."""
    query = db.collection(collection).where(filter=FieldFilter("load_test", "==", True))
    count = 0
    async for doc in query.stream():
        count += 1
        if execute:
            await doc.reference.delete()
    return count


async def run(args: argparse.Namespace) -> None:
    restrict = _restrict_dids(args.users)
    db = init_firestore_client()

    if not args.execute:
        console.print(
            "[yellow]DRY RUN — nothing will be deleted. Pass --execute to delete.[/yellow]"
        )

    deleted, skipped = await _delete_test_users(db, args.execute, restrict)

    interactions = await _delete_flagged(db, INTERACTIONS_COLLECTION, args.execute)
    cache = 0
    if not args.skip_cache:
        cache = await _delete_flagged(db, FEED_CACHE_COLLECTION, args.execute)

    verb = "Deleted" if args.execute else "Would delete"
    console.print(
        f"[bold]{verb}:[/bold] {deleted} users (skipped {skipped} now-real), "
        f"{interactions} interactions, {cache} feed-cache entries"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete Green Earth load-test data")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["dev", "stage", "prod"],
        required=True,
        help="Firestore target (required; this deletes data)",
    )
    parser.add_argument("--execute", action="store_true", help="Actually delete (default: dry run)")
    parser.add_argument("--users", help="Restrict deletion to DIDs in this selection JSON")
    parser.add_argument("--skip-cache", action="store_true", help="Leave feed_cache entries alone")
    args = parser.parse_args()

    _configure_environment(args.environment)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

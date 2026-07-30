#!/usr/bin/env python3
"""Delete data created by load-test traffic (issue api#189).

Removes users that were created *only* by a load test (so the cold-start path
can be re-run for the same DID), plus load-test interactions, feed-cache
entries, and — for real users touched by a run — their load-test seen/discarded
activity buckets. A user who has since made a real request has had their
``created_by_load_test`` flag cleared by the API (they "became ours") and is
left untouched — this script re-checks the flag immediately before deleting to
avoid a race.

Runs as a dry-run by default; pass ``--execute`` to actually delete.
``--environment dev`` requires a Firestore emulator (``GE_FIRESTORE_EMULATOR_HOST``)
and refuses to run without one, so deletes can never hit real Firestore.

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
    DISCARDED_POSTS_COLLECTION,
    INTERACTIONS_COLLECTION,
    LOAD_TEST_BUCKET_SUFFIX,
    SEEN_POSTS_COLLECTION,
    USERS_COLLECTION,
    init_firestore_client,
    user_doc_id,
)
from load_test.lib import GCP_PROJECT

console = Console()

_FIRESTORE_DATABASES = {"stage": "greenearth-stage", "prod": "greenearth-prod"}


def _configure_environment(env: str) -> None:
    if env == "dev":
        # Fail closed: dev must target an emulator. Without this, inherited
        # credentials could send --execute deletes at real/default Firestore.
        if not (
            os.environ.get("GE_FIRESTORE_EMULATOR_HOST")
            or os.environ.get("FIRESTORE_EMULATOR_HOST")
        ):
            raise SystemExit(
                "dev requires a Firestore emulator: set GE_FIRESTORE_EMULATOR_HOST "
                "(e.g. 127.0.0.1:8080) before running cleanup with --environment dev."
            )
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


async def _delete_flagged(
    db,
    collection: str,
    execute: bool,
    *,
    restrict: set[str] | None = None,
    user_field: str | None = None,
) -> int:
    """Delete documents in ``collection`` with load_test == True.

    When ``restrict`` is given, only documents whose ``user_field`` value is in
    the set are deleted (used to honor ``--users`` for interactions, which carry
    ``user_did``). Collections with no user linkage pass ``user_field=None`` and
    should not be called with a restriction.
    """
    query = db.collection(collection).where(filter=FieldFilter("load_test", "==", True))
    count = 0
    async for doc in query.stream():
        if restrict is not None and user_field is not None:
            data = doc.to_dict() or {}
            if data.get(user_field) not in restrict:
                continue
        count += 1
        if execute:
            await doc.reference.delete()
    return count


async def _delete_suffixed_buckets(db, dids: set[str], execute: bool) -> int:
    """Delete load-test seen/discarded buckets for the given (real) users.

    Test-created users are removed whole by ``recursive_delete``; this handles
    the remaining case — real users whose docs we keep but who accumulated
    load-test activity buckets (doc IDs ending in the load-test suffix).
    """
    count = 0
    for did in dids:
        for collection in (SEEN_POSTS_COLLECTION, DISCARDED_POSTS_COLLECTION):
            coll = (
                db.collection(USERS_COLLECTION)
                .document(user_doc_id(did))
                .collection(collection)
            )
            async for doc in coll.stream():
                if doc.id.endswith(LOAD_TEST_BUCKET_SUFFIX):
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

    interactions = await _delete_flagged(
        db, INTERACTIONS_COLLECTION, args.execute, restrict=restrict, user_field="user_did"
    )

    # feed_cache docs are keyed by request_id with no user field, so --users
    # cannot scope them; skip cache deletion (rather than delete globally) when
    # a restriction is in effect.
    cache = 0
    cache_note = ""
    if args.skip_cache:
        cache_note = " (feed_cache skipped: --skip-cache)"
    elif restrict is not None:
        cache_note = " (feed_cache skipped: not scopable by --users)"
    else:
        cache = await _delete_flagged(db, FEED_CACHE_COLLECTION, args.execute)

    # Real users keep their doc, but their load-test activity buckets must be
    # removed explicitly. Only possible with a --users manifest of who to clean;
    # otherwise native TTL (14d/3d) is the backstop.
    buckets = 0
    if restrict is not None:
        buckets = await _delete_suffixed_buckets(db, restrict, args.execute)
    else:
        console.print(
            "[dim]No --users file: real users' load-test seen/discarded buckets "
            "are left to native TTL (14d/3d).[/dim]"
        )

    verb = "Deleted" if args.execute else "Would delete"
    console.print(
        f"[bold]{verb}:[/bold] {deleted} users (skipped {skipped} now-real), "
        f"{interactions} interactions, {cache} feed-cache entries{cache_note}, "
        f"{buckets} load-test activity buckets"
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
    parser.add_argument(
        "--users",
        help="Restrict deletion to DIDs in this selection JSON. Scopes user docs, "
        "interactions, and load-test activity buckets; feed_cache (no user linkage) "
        "is skipped when set.",
    )
    parser.add_argument("--skip-cache", action="store_true", help="Leave feed_cache entries alone")
    args = parser.parse_args()

    _configure_environment(args.environment)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Delete data created by load-test traffic (issue api#189).

The goal is always the same: remove *everything* a load test created. Most of
it is tagged and deleted globally by tag — users created only by a load test
(``created_by_load_test``, whole subtree, so the cold-start path can be re-run
for the same DID), plus load-test interactions and feed-cache entries
(``load_test``). A user who has since made a real request had that flag cleared
by the API (they "became ours"); this script re-checks it immediately before
deleting to avoid a race, and leaves such users alone.

The one thing tags can't find is a load-test seen/discarded **activity bucket**
that accumulated under a *real* user (its suffixed doc ID lives in the same
subcollection as the user's real buckets). To reach those, pass ``--users``
with the run's manifest from select_users.py — it's used only as a **hint** for
which real users to check; everything else is still cleaned globally. Without
``--users`` the cleanup can't be complete, so it refuses unless ``--force`` is
given (which cleans everything else and leaves those buckets to native TTL).

Runs as a dry-run by default; pass ``--execute`` to actually delete.
``--environment dev`` requires a Firestore emulator (``GE_FIRESTORE_EMULATOR_HOST``)
and refuses to run without one, so deletes can never hit real Firestore.

Run from the api/ directory:

    pipenv run python scripts/load_test/cleanup.py --environment stage --users run.json
    pipenv run python scripts/load_test/cleanup.py --environment stage --users run.json --execute

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


async def _delete_test_users(db, execute: bool) -> tuple[int, int]:
    """Delete all users flagged created_by_load_test. Returns (deleted, skipped)."""
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
    """Delete all documents in ``collection`` with load_test == True."""
    query = db.collection(collection).where(filter=FieldFilter("load_test", "==", True))
    count = 0
    async for doc in query.stream():
        count += 1
        if execute:
            await doc.reference.delete()
    return count


async def _delete_suffixed_buckets(db, dids: set[str], execute: bool) -> int:
    """Delete load-test seen/discarded buckets belonging to *real* users.

    ``dids`` is the run manifest — a hint of who to check. Test-created users
    are removed whole by ``_delete_test_users``/``recursive_delete``, so we skip
    any manifest DID whose user doc is missing (already deleted) or still flagged
    (would be). That leaves exactly the real users, whose suffixed activity
    buckets tags can't reach — and keeps the dry-run count honest, since it never
    counts buckets that ``recursive_delete`` already owns.
    """
    count = 0
    for did in dids:
        user_ref = db.collection(USERS_COLLECTION).document(user_doc_id(did))
        snap = await user_ref.get()
        if not snap.exists or (snap.to_dict() or {}).get("created_by_load_test"):
            continue
        for collection in (SEEN_POSTS_COLLECTION, DISCARDED_POSTS_COLLECTION):
            async for doc in user_ref.collection(collection).stream():
                if doc.id.endswith(LOAD_TEST_BUCKET_SUFFIX):
                    count += 1
                    if execute:
                        await doc.reference.delete()
    return count


async def run(args: argparse.Namespace) -> None:
    manifest_dids = _restrict_dids(args.users)

    # Without a manifest we can't find real users' load-test activity buckets,
    # so a run would be incomplete. Refuse unless the operator opts into that.
    if manifest_dids is None and not args.force:
        raise SystemExit(
            "Refusing to run without --users: real users' load-test seen/discarded "
            "activity buckets can only be found from a run manifest, so cleanup "
            "would be incomplete. Pass --users <manifest.json>, or --force to clean "
            "everything else and leave those buckets to native TTL (14d/3d)."
        )

    db = init_firestore_client()

    if not args.execute:
        console.print(
            "[yellow]DRY RUN — nothing will be deleted. Pass --execute to delete.[/yellow]"
        )

    # Everything below is deleted globally by tag — the manifest never narrows it.
    deleted, skipped = await _delete_test_users(db, args.execute)
    interactions = await _delete_flagged(db, INTERACTIONS_COLLECTION, args.execute)

    cache = 0
    cache_note = ""
    if args.skip_cache:
        cache_note = " (feed_cache skipped: --skip-cache)"
    else:
        cache = await _delete_flagged(db, FEED_CACHE_COLLECTION, args.execute)

    # Suffixed activity buckets on real users need the manifest hint to be found.
    buckets = 0
    if manifest_dids is not None:
        buckets = await _delete_suffixed_buckets(db, manifest_dids, args.execute)
    else:
        console.print(
            "[yellow]--force without --users: real users' load-test seen/discarded "
            "buckets are left to native TTL (14d/3d).[/yellow]"
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
        help="Run manifest (from select_users.py). A hint for which real users' "
        "load-test seen/discarded activity buckets to remove; everything else is "
        "cleaned globally by tag regardless. Required unless --force.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed without --users, leaving real users' load-test activity "
        "buckets to native TTL (cleanup is otherwise incomplete).",
    )
    parser.add_argument("--skip-cache", action="store_true", help="Leave feed_cache entries alone")
    args = parser.parse_args()

    _configure_environment(args.environment)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

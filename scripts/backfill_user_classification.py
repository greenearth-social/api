#!/usr/bin/env python3
"""Backfill PostSeen-derived user classification flags.

Dry-run is the default. Pass ``--execute`` to merge the calculated fields into
user documents. The script never clears a true classification flag.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import pathlib
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from google.cloud.firestore import FieldFilter  # type: ignore[import-untyped]

REPO_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from app.lib.firestore import (  # type: ignore  # noqa: E402
    INTERACTIONS_COLLECTION,
    backfill_user_post_seen,
    init_firestore_client,
)


@dataclass
class UserPostSeenHistory:
    days: set[date] = field(default_factory=set)
    last_seen_at: datetime | None = None


def add_interaction(
    histories: dict[str, UserPostSeenHistory],
    data: dict,
) -> bool:
    """Add one valid stored PostSeen interaction to the aggregate."""
    if data.get("event") != "interactionSeen" or data.get("load_test"):
        return False
    user_did = data.get("user_did")
    item_uri = data.get("item_uri")
    created_at = data.get("created_at")
    if not isinstance(user_did, str) or not user_did:
        return False
    if not isinstance(item_uri, str) or not item_uri:
        return False
    if not isinstance(created_at, datetime):
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    created_at = created_at.astimezone(UTC)

    history = histories.setdefault(user_did, UserPostSeenHistory())
    history.days.add(created_at.date())
    if history.last_seen_at is None or created_at > history.last_seen_at:
        history.last_seen_at = created_at
    return True


async def collect_histories(db) -> tuple[dict[str, UserPostSeenHistory], int, int]:
    """Stream stored PostSeen events and return per-user history plus counts."""
    histories: dict[str, UserPostSeenHistory] = {}
    scanned = 0
    accepted = 0
    query = db.collection(INTERACTIONS_COLLECTION).where(
        filter=FieldFilter("event", "==", "interactionSeen")
    )
    async for snapshot in query.stream():
        scanned += 1
        data = snapshot.to_dict()
        if data is not None and add_interaction(histories, data):
            accepted += 1
    return histories, scanned, accepted


async def close_firestore_client(db) -> None:
    """Close Firestore clients across library versions with sync or async close()."""
    result = db.close()
    if inspect.isawaitable(result):
        await result


async def run(*, execute: bool) -> None:
    db = init_firestore_client()
    try:
        histories, scanned, accepted = await collect_histories(db)
        mode = "EXECUTE" if execute else "DRY RUN"
        print(
            f"{mode}: scanned {scanned} PostSeen documents; "
            f"accepted {accepted} events for {len(histories)} users"
        )
        if not execute:
            print("No writes performed. Re-run with --execute to apply the backfill.")
            return

        updated = 0
        for user_did, history in histories.items():
            if history.last_seen_at is None:
                continue
            await backfill_user_post_seen(
                db,
                user_did,
                history.days,
                last_seen_at=history.last_seen_at,
            )
            updated += 1
        print(f"Updated {updated} user documents.")
    finally:
        await close_firestore_client(db)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill PostSeen-derived user classification flags."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write updates (default: dry run).",
    )
    args = parser.parse_args()
    asyncio.run(run(execute=args.execute))


if __name__ == "__main__":
    main()

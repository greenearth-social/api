#!/usr/bin/env python3
"""Migrate the former 24-hour freshness default to seven days in stage.

This script is deliberately stage-only and dry-runs unless ``--apply`` is
provided. It updates only user documents whose stored ``freshness`` value is
exactly ``2``, changing that single field to ``5``.

Usage:
    pipenv run python scripts/migrate_stage_freshness.py
    pipenv run python scripts/migrate_stage_freshness.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google.cloud.firestore import AsyncClient, FieldFilter, async_transactional

from app.lib.firestore import USERS_COLLECTION, init_firestore_client

GCP_PROJECT = "greenearth-471522"
STAGE_DATABASE = "greenearth-stage"
OLD_FRESHNESS = 2
NEW_FRESHNESS = 5


@dataclass(frozen=True)
class MigrationResult:
    matched: int
    updated: int
    skipped: int


def configure_stage() -> None:
    """Force the process onto the stage Firestore database."""
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = STAGE_DATABASE
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)


async def _update_if_unchanged(db: AsyncClient, document_ref) -> bool:
    """Update one document only if its value is still the former default."""
    transaction = db.transaction()

    @async_transactional
    async def update(transaction) -> bool:
        snapshot = await document_ref.get(transaction=transaction)
        data = snapshot.to_dict() if snapshot.exists else None
        if data is None or data.get("freshness") != OLD_FRESHNESS:
            return False
        transaction.update(document_ref, {"freshness": NEW_FRESHNESS})
        return True

    return await update(transaction)


async def migrate_stage_freshness(
    db: AsyncClient,
    *,
    apply: bool,
    update_document: Callable[[AsyncClient, object], Awaitable[bool]] = _update_if_unchanged,
) -> MigrationResult:
    """Find stage users at index 2 and optionally migrate them to index 5."""
    query = db.collection(USERS_COLLECTION).where(
        filter=FieldFilter("freshness", "==", OLD_FRESHNESS)
    )
    documents = [document async for document in query.stream()]

    if not apply:
        return MigrationResult(matched=len(documents), updated=0, skipped=0)

    updated = 0
    for document in documents:
        if await update_document(db, document.reference):
            updated += 1

    return MigrationResult(
        matched=len(documents),
        updated=updated,
        skipped=len(documents) - updated,
    )


async def run(*, apply: bool) -> MigrationResult:
    configure_stage()
    db = init_firestore_client()
    return await migrate_stage_freshness(db, apply=apply)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate stage users from freshness index 2 (24h) to 5 (7d)."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Without this flag, the command is a read-only dry run.",
    )
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"{mode}: project={GCP_PROJECT} database={STAGE_DATABASE}")
    result = asyncio.run(run(apply=args.apply))
    print(f"matched={result.matched} updated={result.updated} skipped={result.skipped}")
    if not args.apply:
        print("No documents were changed. Re-run with --apply to migrate stage.")


if __name__ == "__main__":
    main()

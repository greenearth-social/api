#!/usr/bin/env python3
"""Backfill PostHog with historical user and interaction data from Firestore.

Reads all users, per-user feed activity, and all interaction events from
Firestore and replays them to PostHog with the original timestamps.  Safe to
run multiple times — PostHog deduplicates on (distinct_id, event, timestamp).

Usage:
    cd api
    pipenv run python scripts/backfill_posthog.py
    pipenv run python scripts/backfill_posthog.py --dry-run       # log only, no PostHog writes
    pipenv run python scripts/backfill_posthog.py --limit 500     # cap at 500 users for testing

Requires in .env or environment:
    GE_POSTHOG_API_KEY   — PostHog project API key (pk_...)
    GE_POSTHOG_HOST      — optional, default https://us.i.posthog.com
    GE_FIRESTORE_PROJECT / PROJECT_ID, GE_FIRESTORE_DATABASE, etc. (same as API server)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
from google.cloud.firestore import AsyncClient

from app.lib.firestore import (
    FEED_ACTIVITY_COLLECTION,
    INTERACTIONS_COLLECTION,
    USERS_COLLECTION,
    init_firestore_client,
    user_doc_id,
)
from app.lib.posthog_client import init_posthog_client

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# ---------------------------------------------------------------------------
# Firestore streaming helpers (injected in tests)
# ---------------------------------------------------------------------------


async def _default_stream_users(db: AsyncClient):  # type: ignore[return]
    async for doc in db.collection(USERS_COLLECTION).stream():
        yield doc


async def _default_stream_feed_activity(db: AsyncClient, user_did: str):  # type: ignore[return]
    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_ACTIVITY_COLLECTION)
    )
    async for doc in ref.stream():
        yield doc


async def _default_stream_interactions(db: AsyncClient):  # type: ignore[return]
    async for doc in db.collection(INTERACTIONS_COLLECTION).stream():
        yield doc


# ---------------------------------------------------------------------------
# Backfill logic
# ---------------------------------------------------------------------------


async def backfill_users(
    db: AsyncClient,
    ph,
    *,
    stream_users=None,
    stream_feed_activity=None,
    dry_run: bool = False,
    limit: int | None = None,
) -> int:
    """Emit feed_loaded events and user identification for all Firestore users.

    Returns the total number of feed_loaded events that would be / were sent.
    """
    if stream_users is None:
        stream_users = lambda: _default_stream_users(db)  # noqa: E731
    if stream_feed_activity is None:
        stream_feed_activity = lambda did: _default_stream_feed_activity(db, did)  # noqa: E731

    user_count = 0
    event_count = 0

    async for user_doc in stream_users():
        data = user_doc.to_dict()
        if data is None:
            continue

        user_did = data["user_did"]
        username = data.get("username") or ""
        created_at = data.get("created_at")

        user_count += 1
        if limit is not None and user_count > limit:
            break

        async for activity_doc in stream_feed_activity(user_did):
            activity = activity_doc.to_dict()
            if activity is None:
                continue

            feed_name = activity["feed_name"]
            first_seen_at = activity["first_seen_at"]

            event_count += 1
            logger.info(
                "user=%s feed=%s first_seen_at=%s",
                user_did,
                feed_name,
                first_seen_at,
            )

            if not dry_run:
                set_props: dict = {"username": username}
                if created_at:
                    set_props["posthog_created_at"] = created_at.isoformat()
                ph.capture(
                    distinct_id=user_did,
                    event="feed_loaded",
                    properties={"feed_name": feed_name, "$set": set_props},
                    timestamp=first_seen_at,
                )

    logger.info("Users processed: %d | feed_loaded events queued: %d", user_count, event_count)
    return event_count


async def backfill_interactions(
    db: AsyncClient,
    ph,
    *,
    stream_interactions=None,
    dry_run: bool = False,
) -> int:
    """Emit one PostHog event per Firestore interaction document.

    Returns the total number of events that would be / were sent.
    """
    if stream_interactions is None:
        stream_interactions = lambda: _default_stream_interactions(db)  # noqa: E731

    count = 0
    async for doc in stream_interactions():
        data = doc.to_dict()
        if data is None:
            continue

        user_did = data.get("user_did")
        event = data.get("event")
        feed_name = data.get("feed_name", "")
        item_uri = data.get("item_uri")
        created_at = data.get("created_at")

        if not user_did or not event or not created_at:
            logger.warning("Skipping interaction doc with missing fields: %s", data)
            continue

        count += 1
        logger.info("interaction event=%s user=%s ts=%s", event, user_did, created_at)

        if not dry_run:
            properties: dict = {"feed_name": feed_name}
            if item_uri:
                properties["item_uri"] = item_uri
            ph.capture(
                distinct_id=user_did,
                event=event,
                properties=properties,
                timestamp=created_at,
            )

    logger.info("Interaction events queued: %d", count)
    return count


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


async def _run(dry_run: bool, limit: int | None) -> None:
    db = init_firestore_client()
    api_key = os.environ.get("GE_POSTHOG_API_KEY")
    host = os.environ.get("GE_POSTHOG_HOST", "https://us.i.posthog.com")

    if not api_key:
        raise SystemExit("GE_POSTHOG_API_KEY is required")

    ph = init_posthog_client(api_key, host)

    try:
        await backfill_users(db, ph, dry_run=dry_run, limit=limit)
        await backfill_interactions(db, ph, dry_run=dry_run)
    finally:
        if not dry_run:
            ph.shutdown()
        db.close()


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Backfill PostHog from Firestore")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log events without sending them to PostHog",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of users processed (useful for testing against prod)",
    )
    args = parser.parse_args()

    asyncio.run(_run(dry_run=args.dry_run, limit=args.limit))


if __name__ == "__main__":
    main()

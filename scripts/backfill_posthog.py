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

    # Deploying live PostHog logging: backfill only events strictly before the
    # deployment time, so the live code and this script never double-emit the
    # same event.
    pipenv run python scripts/backfill_posthog.py --before 2026-07-12T18:30:00Z

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
import uuid
from datetime import UTC, datetime

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
    before: datetime | None = None,
) -> int:
    """Emit feedLoaded events and user identification for all Firestore users.

    When ``before`` is set, feed-activity records with ``first_seen_at`` at or
    after that timestamp are skipped -- use this when backfilling up to a
    deployment cutoff so live traffic after that point isn't double-emitted.

    Firestore's ``feed_activity`` collection only stores an aggregate
    first/last-seen pair per (user, feed), not a per-request id, so each
    emitted event gets its own freshly generated ``$session_id`` rather than
    a historically accurate one -- there's no way to reconstruct the
    original per-load sessions from this aggregate data. This at least
    avoids every backfilled feedLoaded event collapsing into one PostHog
    session, which is what happens if ``$session_id`` is omitted entirely.

    Returns the total number of feedLoaded events that would be / were sent.
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

            if before is not None and first_seen_at >= before:
                continue

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
                    event="feedLoaded",
                    properties={
                        "feed_name": feed_name,
                        "$session_id": uuid.uuid4().hex,
                        "$set": set_props,
                    },
                    timestamp=first_seen_at,
                )

    logger.info("Users processed: %d | feedLoaded events queued: %d", user_count, event_count)
    return event_count


async def backfill_interactions(
    db: AsyncClient,
    ph,
    *,
    stream_interactions=None,
    dry_run: bool = False,
    before: datetime | None = None,
) -> int:
    """Emit one PostHog event per Firestore interaction document.

    When ``before`` is set, interactions with ``created_at`` at or after that
    timestamp are skipped -- use this when backfilling up to a deployment
    cutoff so live traffic after that point isn't double-emitted.

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
        request_id = data.get("request_id")
        created_at = data.get("created_at")

        if not user_did or not event or not created_at:
            logger.warning("Skipping interaction doc with missing fields: %s", data)
            continue

        if before is not None and created_at >= before:
            continue

        count += 1
        logger.info("interaction event=%s user=%s ts=%s", event, user_did, created_at)

        if not dry_run:
            properties: dict = {"feed_name": feed_name}
            if request_id:
                properties["$session_id"] = request_id
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


def _parse_before(value: str) -> datetime:
    """Parse a --before value into a UTC-aware datetime.

    Accepts ISO 8601 strings with or without a timezone offset (e.g.
    "2026-07-12T18:30:00Z" or "2026-07-12T18:30:00"). Naive values are
    assumed to already be UTC, matching how Firestore timestamps are stored.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def _run(dry_run: bool, limit: int | None, before: datetime | None) -> None:
    db = init_firestore_client()
    api_key = os.environ.get("GE_POSTHOG_API_KEY")
    host = os.environ.get("GE_POSTHOG_HOST", "https://us.i.posthog.com")

    if not api_key:
        raise SystemExit("GE_POSTHOG_API_KEY is required")

    ph = init_posthog_client(api_key, host)

    try:
        await backfill_users(db, ph, dry_run=dry_run, limit=limit, before=before)
        await backfill_interactions(db, ph, dry_run=dry_run, before=before)
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
    parser.add_argument(
        "--before",
        type=str,
        default=None,
        help="Only backfill events with a timestamp strictly before this ISO 8601 UTC "
        "datetime (e.g. 2026-07-12T18:30:00Z). Use the deployment time of the live "
        "PostHog logging changes to avoid duplicating events already captured live.",
    )
    args = parser.parse_args()

    before = _parse_before(args.before) if args.before else None
    asyncio.run(_run(dry_run=args.dry_run, limit=args.limit, before=before))


if __name__ == "__main__":
    main()

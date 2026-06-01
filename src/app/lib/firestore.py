"""Firestore helpers for typed document access.

Provides ``init_firestore_client`` for application startup and thin typed
wrappers around common Firestore operations.  Each wrapper accepts and
returns Pydantic document models so callers never deal with raw dicts.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ..documents import FeedActivityDocument, InteractionDocument, UserDocument

logger = logging.getLogger(__name__)

USERS_COLLECTION = "users"
FEED_ACTIVITY_COLLECTION = "feed_activity"
INTERACTIONS_COLLECTION = "interactions"


def init_firestore_client() -> AsyncClient:
    """Create an async Firestore client.

    When ``GE_FIRESTORE_EMULATOR_HOST`` is set, the client connects to the
    local emulator instead of production Firestore.  The Google SDK
    natively reads ``FIRESTORE_EMULATOR_HOST``, so we copy the GE-prefixed
    variable into that standard name before creating the client.
    """
    emulator_host = os.environ.get("GE_FIRESTORE_EMULATOR_HOST")
    if emulator_host:
        os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host
        logger.info("Firestore emulator configured at %s", emulator_host)

    project = os.environ.get("GE_FIRESTORE_PROJECT", os.environ.get("PROJECT_ID"))
    if emulator_host and not project:
        # firebase-tools defaults to this demo project when no project is configured.
        # Aligning the SDK avoids writing into a different project namespace.
        project = "demo-no-project"

    database = os.environ.get("GE_FIRESTORE_DATABASE", "(default)")
    logger.info(
        "Initializing Firestore client (project=%s, database=%s, emulator=%s)",
        project,
        database,
        bool(emulator_host),
    )
    return AsyncClient(project=project, database=database)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


async def get_user(db: AsyncClient, user_did: str) -> UserDocument | None:
    """Fetch a user document by DID, or return ``None`` if not found."""
    doc = await db.collection(USERS_COLLECTION).document(user_did).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return UserDocument.model_validate(data)


async def upsert_user(db: AsyncClient, user_did: str, username: str) -> UserDocument:
    """Create or update a user document.

    On first visit the document is created with all timestamps set to now.
    On subsequent visits ``last_seen_at`` is refreshed and ``username`` is
    updated if it changed.
    """
    ref = db.collection(USERS_COLLECTION).document(user_did)
    doc = await ref.get()

    now = datetime.now(timezone.utc)

    if doc.exists:
        data = doc.to_dict()
        if data is None:
            raise ValueError(f"Firestore document exists but to_dict() returned None for {user_did}")

        update_fields: dict[str, object] = {"last_seen_at": now}
        if data.get("username") != username:
            update_fields["username"] = username
            update_fields["updated_at"] = now

        await ref.update(update_fields)

        data.update(update_fields)
        return UserDocument.model_validate(data)

    user = UserDocument(
        user_did=user_did,
        username=username,
        created_at=now,
        updated_at=now,
        last_seen_at=now,
    )
    await ref.set(user.model_dump())
    return user


# ---------------------------------------------------------------------------
# Feed activity
# ---------------------------------------------------------------------------


async def get_feed_activity(db: AsyncClient, user_did: str, feed_name: str) -> FeedActivityDocument | None:
    """Fetch a feed activity document, or return ``None`` if not found."""
    doc = await (
        db.collection(USERS_COLLECTION)
        .document(user_did)
        .collection(FEED_ACTIVITY_COLLECTION)
        .document(feed_name)
        .get()
    )
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return FeedActivityDocument.model_validate(data)


async def upsert_feed_activity(db: AsyncClient, user_did: str, feed_name: str) -> FeedActivityDocument:
    """Record that a user loaded a feed.

    On first visit creates the document with both timestamps set to now.
    On subsequent visits updates only ``last_seen_at``; ``first_seen_at`` is
    never overwritten.
    """
    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_did)
        .collection(FEED_ACTIVITY_COLLECTION)
        .document(feed_name)
    )
    doc = await ref.get()

    now = datetime.now(timezone.utc)

    if doc.exists:
        data = doc.to_dict()
        if data is None:
            raise ValueError(
                f"Firestore feed_activity document exists but to_dict() returned None for {user_did}/{feed_name}"
            )
        await ref.update({"last_seen_at": now})
        data["last_seen_at"] = now
        return FeedActivityDocument.model_validate(data)

    activity = FeedActivityDocument(
        feed_name=feed_name,
        first_seen_at=now,
        last_seen_at=now,
    )
    await ref.set(activity.model_dump())
    return activity


# ---------------------------------------------------------------------------
# Interactions
# ---------------------------------------------------------------------------


async def record_interaction(db: AsyncClient, interaction: InteractionDocument) -> None:
    """Append an interaction event as a new auto-ID document.

    Each interaction is its own document in the top-level ``interactions``
    collection so the data is easy to query and export (e.g. to Elasticsearch).
    """
    await db.collection(INTERACTIONS_COLLECTION).add(interaction.model_dump())

"""Firestore helpers for typed document access.

Provides ``init_firestore_client`` for application startup and thin typed
wrappers around common Firestore operations.  Each wrapper accepts and
returns Pydantic document models so callers never deal with raw dicts.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import ArrayUnion, AsyncClient, FieldFilter, Query  # type: ignore[import-untyped]

from ..documents import (
    FeedActivityDocument,
    FeedDebugDocument,
    FeedSnapshotDocument,
    InteractionDocument,
    UserDocument,
)

logger = logging.getLogger(__name__)

USERS_COLLECTION = "users"
FEED_ACTIVITY_COLLECTION = "feed_activity"
INTERACTIONS_COLLECTION = "interactions"
SEEN_POSTS_COLLECTION = "seen_posts"
FEED_DEBUG_COLLECTION = "feed_debug"
FEED_SNAPSHOTS_COLLECTION = "feed_snapshots"

# How long a seen-posts bucket lives before native Firestore TTL deletes it.
SEEN_POSTS_RETENTION_DAYS = 5

# How long a feed-debug record lives before native Firestore TTL deletes it.
FEED_DEBUG_RETENTION_DAYS = 7

# Prefix stripped from a DID to form the user document ID. The full DID is
# still stored in the document's ``user_did`` field; only the document *key* is
# shortened. This keeps colons out of the key — colons in a document ID break
# subcollection navigation in the Firestore emulator UI. All users are
# currently did:plc; other DID methods are passed through unchanged.
_USER_DID_PREFIX = "did:plc:"


def user_doc_id(user_did: str) -> str:
    """Map a DID to its Firestore user-document ID (colon-free for did:plc)."""
    return user_did.removeprefix(_USER_DID_PREFIX)


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
    doc = await db.collection(USERS_COLLECTION).document(user_doc_id(user_did)).get()
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
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
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


async def get_user_by_username(db: AsyncClient, username: str) -> UserDocument | None:
    """Fetch a user document by handle, or return ``None`` if not found.

    Usernames are not guaranteed unique over time (handles can be reused), so
    this returns the first match.
    """
    query = (
        db.collection(USERS_COLLECTION)
        .where(filter=FieldFilter("username", "==", username))
        .limit(1)
    )
    async for doc in query.stream():
        data = doc.to_dict()
        if data is not None:
            return UserDocument.model_validate(data)
    return None


async def set_user_debug_flag(db: AsyncClient, user_did: str, enabled: bool) -> None:
    """Set the ``debug_feeds`` flag on a user document.

    The user document must already exist (users are created on their first feed
    request); raises ``ValueError`` otherwise so the CLI can report it clearly.
    """
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    doc = await ref.get()
    if not doc.exists:
        raise ValueError(f"No user document for {user_did}")
    await ref.update({"debug_feeds": enabled, "updated_at": datetime.now(timezone.utc)})


async def set_user_social_radius(db: AsyncClient, user_did: str, social_radius: int) -> None:
    """Set the ``social_radius`` preference on a user document.

    Uses ``merge=True`` so the preference is created alongside a minimal
    user document when the user has not yet loaded a feed in Bluesky.
    The full user document is filled in later by ``upsert_user``.
    """
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    await ref.set(
        {"user_did": user_did, "social_radius": social_radius},
        merge=True,
    )


# ---------------------------------------------------------------------------
# Feed activity
# ---------------------------------------------------------------------------


async def get_feed_activity(db: AsyncClient, user_did: str, feed_name: str) -> FeedActivityDocument | None:
    """Fetch a feed activity document, or return ``None`` if not found."""
    doc = await (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
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
        .document(user_doc_id(user_did))
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


# ---------------------------------------------------------------------------
# Seen posts
# ---------------------------------------------------------------------------


async def record_seen_posts(db: AsyncClient, user_did: str, post_uris: list[str]) -> None:
    """Append seen post URIs to the user's bucket for the current UTC day.

    Buckets are keyed by ``YYYY-MM-DD`` under the user's ``seen_posts``
    subcollection.  ``ArrayUnion`` appends without duplicating within the bucket,
    and ``expires_at`` (re-stamped on each write) drives the native Firestore TTL
    so the bucket self-deletes ~``SEEN_POSTS_RETENTION_DAYS`` days after its last
    update.  No-op when there is nothing to record.
    """
    if not post_uris:
        return

    now = datetime.now(timezone.utc)
    bucket_id = now.strftime("%Y-%m-%d")
    expires_at = now + timedelta(days=SEEN_POSTS_RETENTION_DAYS)

    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(SEEN_POSTS_COLLECTION)
        .document(bucket_id)
    )
    await ref.set(
        {"post_uris": ArrayUnion(post_uris), "expires_at": expires_at},
        merge=True,
    )


async def get_recent_seen_uris(
    db: AsyncClient, user_did: str, *, max_uris: int = 1000
) -> list[str]:
    """Return the user's most-recently-seen post URIs, de-duped and capped.

    Reads the non-expired daily buckets (filtering on ``expires_at`` so buckets
    not yet reaped by TTL are still excluded once stale) and walks them
    newest-first, collecting URIs until ``max_uris`` is reached.  Within a day
    ``ArrayUnion`` preserves append order, so the result is roughly the most
    recent URIs.
    """
    now = datetime.now(timezone.utc)
    query = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(SEEN_POSTS_COLLECTION)
        .where("expires_at", ">", now)
    )

    buckets = [doc async for doc in query.stream()]
    # Doc IDs are YYYY-MM-DD, so lexical sort == chronological; newest first.
    buckets.sort(key=lambda doc: doc.id, reverse=True)

    result: list[str] = []
    seen: set[str] = set()
    for doc in buckets:
        data = doc.to_dict() or {}
        for uri in data.get("post_uris", []):
            if uri in seen:
                continue
            seen.add(uri)
            result.append(uri)
            if len(result) >= max_uris:
                return result
    return result


# ---------------------------------------------------------------------------
# Feed debug
# ---------------------------------------------------------------------------


async def write_feed_debug(db: AsyncClient, doc: FeedDebugDocument) -> None:
    """Persist a feed-debug record under ``users/{user_did}/feed_debug/{request_id}``.

    ``expires_at`` on the document drives the native Firestore TTL so records
    self-delete ~``FEED_DEBUG_RETENTION_DAYS`` days after the feed was served.
    """
    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(doc.user_did))
        .collection(FEED_DEBUG_COLLECTION)
        .document(doc.request_id)
    )
    await ref.set(doc.model_dump())


async def get_recent_feed_debug(
    db: AsyncClient, user_did: str, *, limit: int = 20
) -> list[FeedDebugDocument]:
    """Return a user's most recent feed-debug records, newest first."""
    query = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_DEBUG_COLLECTION)
        .order_by("generated_at", direction=Query.DESCENDING)
        .limit(limit)
    )
    docs: list[FeedDebugDocument] = []
    async for doc in query.stream():
        data = doc.to_dict()
        if data is not None:
            docs.append(FeedDebugDocument.model_validate(data))
    return docs


async def get_feed_debug(
    db: AsyncClient, user_did: str, request_id: str
) -> FeedDebugDocument | None:
    """Fetch a single feed-debug record, or ``None`` if not found."""
    doc = await (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_DEBUG_COLLECTION)
        .document(request_id)
        .get()
    )
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return FeedDebugDocument.model_validate(data)


# ---------------------------------------------------------------------------
# Feed snapshots — lightweight pipeline metadata for every feed load
# ---------------------------------------------------------------------------


async def write_feed_snapshot(
    db: AsyncClient, user_did: str, request_id: str, doc: FeedSnapshotDocument
) -> None:
    """Persist a feed snapshot under ``users/{user_did}/feed_snapshots/{request_id}``."""
    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
        .document(request_id)
    )
    await ref.set(doc.model_dump())


async def get_feed_snapshot(
    db: AsyncClient, user_did: str, request_id: str
) -> FeedSnapshotDocument | None:
    """Fetch a single feed snapshot, or ``None`` if not found."""
    doc = await (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
        .document(request_id)
        .get()
    )
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return FeedSnapshotDocument.model_validate(data)


async def get_recent_feed_snapshots(
    db: AsyncClient,
    user_did: str,
    *,
    feed_name: str | None = None,
    cutoff: datetime | None = None,
    limit: int = 20,
) -> list[FeedSnapshotDocument]:
    """Return a user's most recent feed snapshots, newest first.

    Optional *feed_name* and *cutoff* filters are pushed into the Firestore
    query so callers don't get false-empty results from Python-side filtering
    of a fixed-size result set.

    Requires a collection-group composite index on ``feed_snapshots`` with
    fields ``(feed_name ASC, generated_at DESC)`` — see ``firestore.indexes.json``.
    """
    query = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
    )
    if feed_name is not None:
        query = query.where(filter=FieldFilter("feed_name", "==", feed_name))
    if cutoff is not None:
        query = query.where(filter=FieldFilter("generated_at", ">=", cutoff))
    query = query.order_by("generated_at", direction=Query.DESCENDING).limit(limit)
    docs: list[FeedSnapshotDocument] = []
    async for doc in query.stream():
        data = doc.to_dict()
        if data is not None:
            docs.append(FeedSnapshotDocument.model_validate(data))
    return docs


async def get_newer_feed_snapshot_uris(
    db: AsyncClient,
    user_did: str,
    *,
    feed_name: str,
    newer_than: datetime,
) -> set[str]:
    """Return all AT URIs from snapshots newer than *newer_than* for *feed_name*.

    Used for deduplication when viewing a feed snapshot: any post that already
    appears in a more-recent snapshot for the same feed is excluded so the user
    only sees fresh posts.
    """
    uris: set[str] = set()
    query = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
        .where(filter=FieldFilter("feed_name", "==", feed_name))
        .where(filter=FieldFilter("generated_at", ">", newer_than))
        .order_by("generated_at", direction=Query.DESCENDING)
    )
    async for doc in query.stream():
        data = doc.to_dict()
        if data is not None:
            items = data.get("items", [])
            if items:
                uris.update(items)
    return uris

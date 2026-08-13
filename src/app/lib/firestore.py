"""Firestore helpers for typed document access.

Provides ``init_firestore_client`` for application startup and thin typed
wrappers around common Firestore operations.  Each wrapper accepts and
returns Pydantic document models so callers never deal with raw dicts.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from google.cloud.firestore import (  # type: ignore[import-untyped]
    ArrayUnion,
    AsyncClient,
    FieldFilter,
    Query,
    async_transactional,
)

from ..documents import (
    FeedActivityDocument,
    FeedDebugDocument,
    FeedPreferencesDocument,
    FeedSnapshotDocument,
    InteractionDocument,
    RedirectDocument,
    UserDocument,
)
from .feed_preferences import preference_source, resolve_feed_preferences

logger = logging.getLogger(__name__)

USERS_COLLECTION = "users"
FEED_ACTIVITY_COLLECTION = "feed_activity"
INTERACTIONS_COLLECTION = "interactions"
SEEN_POSTS_COLLECTION = "seen_posts"
DISCARDED_POSTS_COLLECTION = "discarded_posts"
FEED_DEBUG_COLLECTION = "feed_debug"
FEED_SNAPSHOTS_COLLECTION = "feed_snapshots"
MAX_FEED_SNAPSHOT_ITEMS = 500
MAX_FEED_SNAPSHOT_DOCUMENTS = 100
FIRESTORE_WRITE_BATCH_LIMIT = 500

# Suffix appended to a daily-bucket document ID (``YYYY-MM-DD``) for load-test
# traffic. Test buckets live in the same subcollection as real ones so the
# exclusion read path is exercised identically (``_get_recent_bucket_uris``
# streams every non-expired bucket), but the distinct ID lets a cleanup script
# delete exactly the synthetic buckets without touching a real user's data.
# The suffix sorts after any time-of-day, so lexical ID order stays chronological.
LOAD_TEST_BUCKET_SUFFIX = "-load-test"

# How long a seen-posts bucket lives before native Firestore TTL deletes it.
SEEN_POSTS_RETENTION_DAYS = 14

# How long a discarded-posts bucket lives before native Firestore TTL deletes
# it. Shorter than seen posts: the candidate pool refreshes quickly and ranker
# scores are only stable short-term.
DISCARDED_POSTS_RETENTION_DAYS = 3

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
        # The canonical emulator config lives in the frontend repository and
        # uses this project namespace via its .firebaserc.
        project = "greenearth-471522"

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


async def upsert_user(
    db: AsyncClient, user_did: str, username: str | None, *, is_load_test: bool = False
) -> UserDocument:
    """Create or update a user document.

    On first visit the document is created with all timestamps set to now.
    On subsequent visits ``last_seen_at`` is refreshed and ``username`` is
    updated if it changed.

    ``username`` may be ``None`` when the caller's handle couldn't be resolved
    (the DID is the identity; the handle is enrichment). A ``None`` never
    overwrites a handle we already know — a transient resolution failure
    shouldn't erase good data.

    Load testing (``is_load_test=True``):

    - A newly created document is tagged ``created_by_load_test=True`` so a
      cleanup script can later delete users that only ever existed as test
      traffic (letting the cold-start path be re-run for the same DID).
    - When the document already exists it is left **untouched** — no
      ``last_seen_at`` bump. A load test samples real users, and bumping their
      activity on every run would corrupt the very activity data the user
      selection reads. The read already happened; skipping the write is free.

    Real traffic (``is_load_test=False``) on a document previously created by a
    load test clears the tag and resets ``created_at`` to now: a real person has
    now shown up, so the user "becomes ours", their data must be preserved, and
    their first-seen time should reflect the first *real* visit.
    """
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    doc = await ref.get()

    now = datetime.now(UTC)

    if doc.exists:
        data = doc.to_dict()
        if data is None:
            raise ValueError(
                f"Firestore document exists but to_dict() returned None for {user_did}"
            )

        if is_load_test:
            # Never mutate a real user's record from synthetic traffic.
            return UserDocument.model_validate(data)

        update_fields: dict[str, object] = {"last_seen_at": now}
        if username is not None and data.get("username") != username:
            update_fields["username"] = username
            update_fields["updated_at"] = now

        if data.get("created_by_load_test"):
            # A real request has arrived for a load-test-created user: they are
            # ours now. Clear the deletable flag and treat this as their true
            # first visit so growth analytics don't count the synthetic one.
            update_fields["created_by_load_test"] = False
            update_fields["created_at"] = now
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
        created_by_load_test=is_load_test,
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
    await ref.update({"debug_feeds": enabled, "updated_at": datetime.now(UTC)})


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


async def set_user_preferences(
    db: AsyncClient,
    user_did: str,
    social_radius: int,
    freshness: int,
    politics: float,
    purpose: float,
) -> None:
    """Set all preferences on a user document.

    Uses ``merge=True`` so the preference is created alongside a minimal
    user document when the user has not yet loaded a feed in Bluesky.
    The full user document is filled in later by ``upsert_user``.

    Setting preferences is a real person acting, so it also clears any
    ``created_by_load_test`` tag — a load-test-created user who adopts us via
    the web UI (before any real feed load) must not be deletable.
    """
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    await ref.set(
        {
            "user_did": user_did,
            "social_radius": social_radius,
            "freshness": freshness,
            "politics": politics,
            "purpose": purpose,
            "created_by_load_test": False,
        },
        merge=True,
    )


async def patch_user_feed_preferences(
    db: AsyncClient,
    user_did: str,
    feed_name: str,
    patch: FeedPreferencesDocument,
) -> FeedPreferencesDocument:
    """Atomically patch and materialize one feed without replacing siblings."""
    ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    transaction = db.transaction()

    @async_transactional
    async def _patch(transaction) -> FeedPreferencesDocument:
        snapshot = await ref.get(transaction=transaction)
        data = snapshot.to_dict() if snapshot.exists else None
        user = UserDocument.model_validate(data) if data is not None else None
        resolved = resolve_feed_preferences(user, feed_name)
        values = resolved.model_dump(exclude_none=True)
        values.update(patch.model_dump(exclude_none=True))
        updated = FeedPreferencesDocument.model_validate(values)
        transaction.set(
            ref,
            {
                "user_did": user_did,
                "feed_preferences": {
                    preference_source(feed_name): updated.model_dump(exclude_none=True),
                },
                "created_by_load_test": False,
                "updated_at": datetime.now(UTC),
            },
            merge=True,
        )
        return updated

    return await _patch(transaction)


# ---------------------------------------------------------------------------
# Feed activity
# ---------------------------------------------------------------------------


async def get_feed_activity(
    db: AsyncClient, user_did: str, feed_name: str
) -> FeedActivityDocument | None:
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


async def upsert_feed_activity(
    db: AsyncClient, user_did: str, feed_name: str
) -> FeedActivityDocument:
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

    now = datetime.now(UTC)

    if doc.exists:
        data = doc.to_dict()
        if data is None:
            raise ValueError(
                "Firestore feed_activity document exists but to_dict() returned "
                f"None for {user_did}/{feed_name}"
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
# Seen / discarded posts (per-user daily URI buckets)
# ---------------------------------------------------------------------------


async def _record_daily_bucket_uris(
    db: AsyncClient,
    user_did: str,
    collection: str,
    post_uris: list[str],
    retention_days: int,
    *,
    load_test: bool = False,
) -> None:
    """Append post URIs to the user's ``collection`` bucket for the current UTC day.

    Buckets are keyed by ``YYYY-MM-DD`` under the user's subcollection.
    ``ArrayUnion`` appends without duplicating within the bucket, and
    ``expires_at`` (re-stamped on each write) drives the native Firestore TTL
    so the bucket self-deletes ~``retention_days`` days after its last update.
    No-op when there is nothing to record.

    Load-test traffic (``load_test=True``) is routed to a separate bucket
    (``YYYY-MM-DD-load-test``) carrying a ``load_test`` marker, so it never
    mixes into a real user's exclusion data and a cleanup script can delete it
    precisely. The read path (``_get_recent_bucket_uris``) picks up both, so the
    exclusion behaviour under test is identical to production.
    """
    if not post_uris:
        return

    now = datetime.now(UTC)
    bucket_id = now.strftime("%Y-%m-%d")
    if load_test:
        bucket_id += LOAD_TEST_BUCKET_SUFFIX
    expires_at = now + timedelta(days=retention_days)

    doc: dict[str, object] = {"post_uris": ArrayUnion(post_uris), "expires_at": expires_at}
    if load_test:
        doc["load_test"] = True

    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(collection)
        .document(bucket_id)
    )
    await ref.set(doc, merge=True)


async def _get_recent_bucket_uris(
    db: AsyncClient, user_did: str, collection: str, max_uris: int
) -> list[str]:
    """Return the user's most-recent ``collection`` post URIs, de-duped and capped.

    Reads the non-expired daily buckets (filtering on ``expires_at`` so buckets
    not yet reaped by TTL are still excluded once stale) and walks them
    newest-first, collecting URIs until ``max_uris`` is reached.  Within a day
    ``ArrayUnion`` preserves append order, so the result is roughly the most
    recent URIs.
    """
    now = datetime.now(UTC)
    query = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(collection)
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


async def record_seen_posts(
    db: AsyncClient, user_did: str, post_uris: list[str], *, load_test: bool = False
) -> None:
    """Append seen post URIs to the user's bucket for the current UTC day."""
    await _record_daily_bucket_uris(
        db,
        user_did,
        SEEN_POSTS_COLLECTION,
        post_uris,
        SEEN_POSTS_RETENTION_DAYS,
        load_test=load_test,
    )


async def get_recent_seen_uris(
    db: AsyncClient, user_did: str, *, max_uris: int = 1000
) -> list[str]:
    """Return the user's most-recently-seen post URIs, de-duped and capped."""
    return await _get_recent_bucket_uris(db, user_did, SEEN_POSTS_COLLECTION, max_uris)


async def delete_most_recent_seen_bucket(db: AsyncClient, user_did: str) -> None:
    """Delete the current UTC day's seen-posts bucket for the user.

    Idempotent: no-op if the bucket does not exist. Called on settings changes so
    the user sees fresh candidates after adjusting their feed controls; repeated
    settings changes within the same day are safe because the document is already
    gone after the first call.
    """
    bucket_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(SEEN_POSTS_COLLECTION)
        .document(bucket_id)
    )
    await ref.delete()


async def record_discarded_posts(
    db: AsyncClient, user_did: str, post_uris: list[str], *, load_test: bool = False
) -> None:
    """Append low-ranker-score post URIs to the user's bucket for the current UTC day.

    Discarded posts scored below a feed's ``min_rank_score`` and will never be
    displayed, so future candidate generation excludes them.
    """
    await _record_daily_bucket_uris(
        db,
        user_did,
        DISCARDED_POSTS_COLLECTION,
        post_uris,
        DISCARDED_POSTS_RETENTION_DAYS,
        load_test=load_test,
    )


async def get_recent_discarded_uris(
    db: AsyncClient, user_did: str, *, max_uris: int = 1000
) -> list[str]:
    """Return the user's most-recently-discarded post URIs, de-duped and capped."""
    return await _get_recent_bucket_uris(db, user_did, DISCARDED_POSTS_COLLECTION, max_uris)


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


def _merge_feed_snapshots(
    existing: FeedSnapshotDocument,
    incoming: FeedSnapshotDocument,
) -> tuple[FeedSnapshotDocument, bool]:
    """Merge two batches for one feed session in chronological order."""
    incoming_is_earlier = incoming.generated_at < existing.generated_at
    earlier, later = (incoming, existing) if incoming_is_earlier else (existing, incoming)

    ordered_items = list(dict.fromkeys([*earlier.items, *later.items]))
    truncated = len(ordered_items) > MAX_FEED_SNAPSHOT_ITEMS
    ordered_items = ordered_items[:MAX_FEED_SNAPSHOT_ITEMS]
    # Later metadata wins for a URI without changing its first position.
    meta_by_uri = {meta.at_uri: meta for meta in earlier.items_meta}
    meta_by_uri.update({meta.at_uri: meta for meta in later.items_meta})
    items_meta = [meta_by_uri[uri] for uri in ordered_items if uri in meta_by_uri]
    existing_diag = {(diag.name, diag.mode): diag for diag in existing.generator_diagnostics}
    incoming_diag = {(diag.name, diag.mode): diag for diag in incoming.generator_diagnostics}
    incoming_new = set(incoming.items) - set(existing.items)
    diagnostics = []
    for key in dict.fromkeys([*existing_diag, *incoming_diag]):
        name, _mode = key
        old = existing_diag.get(key)
        new = incoming_diag.get(key)
        if old is None and new is not None:
            diagnostics.append(new)
            continue
        if new is None and old is not None:
            diagnostics.append(old)
            continue
        assert old is not None and new is not None
        is_new_generation = incoming.generated_at > existing.generated_at
        added = sum(
            1
            for meta in incoming.items_meta
            if meta.at_uri in incoming_new
            and any(generator.name == name for generator in meta.generators)
        )
        diagnostics.append(
            old.model_copy(
                update={
                    "returned_count": (
                        old.returned_count + new.returned_count
                        if is_new_generation
                        else max(old.returned_count, new.returned_count)
                    ),
                    "contributed_count": old.contributed_count + added,
                    "status": new.status if old.status != "success" else old.status,
                    "reason": new.reason if old.reason is None else old.reason,
                }
            )
        )

    return (
        earlier.model_copy(
            update={
                "items": ordered_items,
                "items_meta": items_meta,
                "generator_diagnostics": diagnostics,
                "expires_at": max(existing.expires_at, incoming.expires_at),
            }
        ),
        truncated,
    )


async def merge_feed_snapshot(
    db: AsyncClient,
    user_did: str,
    request_id: str,
    doc: FeedSnapshotDocument,
    *,
    enforce_document_cap: bool = True,
) -> bool:
    """Atomically create or extend a feed-session snapshot.

    Returns ``True`` when the merged session exceeded the item safety limit and
    was truncated. Empty batches are ignored.
    """
    if not doc.items:
        return False

    if len(doc.items) > MAX_FEED_SNAPSHOT_ITEMS:
        included_items = doc.items[:MAX_FEED_SNAPSHOT_ITEMS]
        included = set(included_items)
        doc = doc.model_copy(
            update={
                "items": included_items,
                "items_meta": [meta for meta in doc.items_meta if meta.at_uri in included],
            }
        )
        initial_truncated = True
    else:
        initial_truncated = False

    ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
        .document(request_id)
    )
    transaction = db.transaction()

    @async_transactional
    async def _merge(transaction) -> tuple[bool, bool]:
        snapshot = await ref.get(transaction=transaction)
        if not snapshot.exists:
            transaction.set(ref, doc.model_dump())
            return initial_truncated, True

        data = snapshot.to_dict()
        if data is None:
            transaction.set(ref, doc.model_dump())
            return initial_truncated, False

        merged, truncated = _merge_feed_snapshots(FeedSnapshotDocument.model_validate(data), doc)
        transaction.set(ref, merged.model_dump())
        return truncated, False

    truncated, created = await _merge(transaction)
    if created and enforce_document_cap:
        await prune_feed_snapshots(db, user_did)
    return truncated


async def prune_feed_snapshots(
    db: AsyncClient,
    user_did: str,
    *,
    max_documents: int = MAX_FEED_SNAPSHOT_DOCUMENTS,
) -> int:
    """Delete snapshots older than the newest *max_documents* for a user.

    The cap is enforced after a new snapshot is committed. Pruning is
    intentionally fail-soft: a later snapshot creation retries the same query,
    while feed serving remains unaffected by a cleanup failure.
    """
    try:
        collection = (
            db.collection(USERS_COLLECTION)
            .document(user_doc_id(user_did))
            .collection(FEED_SNAPSHOTS_COLLECTION)
        )
        query = collection.order_by("generated_at", direction=Query.DESCENDING).offset(
            max_documents
        )
        overflow = [snapshot async for snapshot in query.stream()]

        # The query returns newest-first. Reverse it so the oldest overflow
        # documents are enqueued for deletion first, including across batches.
        overflow.reverse()
        for start in range(0, len(overflow), FIRESTORE_WRITE_BATCH_LIMIT):
            batch = db.batch()
            for snapshot in overflow[start : start + FIRESTORE_WRITE_BATCH_LIMIT]:
                batch.delete(snapshot.reference)
            await batch.commit()
        return len(overflow)
    except Exception:
        logger.exception(
            "Failed to prune feed snapshots for user '%s'",
            user_did,
        )
        return 0


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


async def delete_feed_snapshot(db: AsyncClient, user_did: str, request_id: str) -> None:
    """Delete a single feed snapshot.

    Used to remove load-test snapshots right after they are written: the write
    exercises the real transactional path (part of what the load test measures),
    but the document must not linger in the user-facing transparency feed.
    """
    await (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(FEED_SNAPSHOTS_COLLECTION)
        .document(request_id)
        .delete()
    )


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
    fields ``(feed_name ASC, generated_at DESC)`` — managed by the frontend
    repository's ``firestore.indexes.json``.
    """
    try:
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
    except Exception:
        logger.exception(
            "Failed to query feed snapshots for user '%s' (feed_name=%s)",
            user_did,
            feed_name,
        )
        return []


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
    try:
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
    except Exception:
        logger.exception(
            "Failed to query newer feed snapshot URIs for user '%s' (feed_name=%s)",
            user_did,
            feed_name,
        )
        return set()


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------

REDIRECTS_COLLECTION = "redirects"


async def get_redirect(db: AsyncClient, slug: str) -> RedirectDocument | None:
    """Fetch a redirect document by slug, or return ``None`` if not found."""
    doc = await db.collection(REDIRECTS_COLLECTION).document(slug).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return RedirectDocument.model_validate(data)


async def create_redirect(
    db: AsyncClient, slug: str, url: str, description: str | None = None
) -> RedirectDocument:
    """Create a new redirect document. Raises ``ValueError`` if slug already exists."""
    ref = db.collection(REDIRECTS_COLLECTION).document(slug)
    existing = await ref.get()
    if existing.exists:
        raise ValueError(f"Slug '{slug}' already exists")
    record = RedirectDocument(slug=slug, url=url, description=description)
    await ref.set(record.model_dump())
    return record


async def update_redirect(
    db: AsyncClient, slug: str, url: str, description: str | None = None
) -> RedirectDocument | None:
    """Update an existing slug, returning ``None`` when it is not found."""
    ref = db.collection(REDIRECTS_COLLECTION).document(slug)
    existing = await ref.get()
    if not existing.exists:
        return None
    updates: dict = {"url": url}
    if description is not None:
        updates["description"] = description
    await ref.update(updates)
    data = existing.to_dict() or {}
    data.update(updates)
    return RedirectDocument.model_validate(data)


async def delete_redirect(db: AsyncClient, slug: str) -> bool:
    """Delete a redirect document. Returns ``True`` if deleted, ``False`` if not found."""
    ref = db.collection(REDIRECTS_COLLECTION).document(slug)
    existing = await ref.get()
    if not existing.exists:
        return False
    await ref.delete()
    return True

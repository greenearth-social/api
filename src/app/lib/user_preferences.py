"""Shared preference operations for Firebase users and scoped service callers."""

import logging

from fastapi import HTTPException, status
from google.cloud.firestore import AsyncClient

from ..documents import FeedPreferencesDocument
from ..feeds import FEEDS
from ..models_feed_transparency import FeedPreferences, PreferencesResponse
from .feed_preferences import resolve_feed_preferences
from .firestore import delete_most_recent_seen_bucket, get_user, patch_user_feed_preferences

logger = logging.getLogger(__name__)


async def get_preferences_for_user(db: AsyncClient, user_did: str) -> PreferencesResponse:
    """Return public feed controls, using defaults without creating a missing user."""
    user_doc = await get_user(db, user_did)
    return PreferencesResponse(
        feeds={
            feed_name: FeedPreferences.model_validate(
                resolve_feed_preferences(user_doc, feed_name).model_dump(exclude_none=True)
            )
            for feed_name, feed in FEEDS.items()
            if feed.public and feed.controls
        }
    )


async def patch_preferences_for_user(
    db: AsyncClient,
    user_did: str,
    feed_name: str,
    body: FeedPreferences,
) -> FeedPreferences:
    """Save one feed's controls with identical behavior for user and service calls.

    Every valid save, even when values are unchanged, can create a minimal user,
    clears created_by_load_test, updates updated_at, and atomically removes the
    feed's accepted-preview handoff. This intentionally treats service writes
    like user saves, including protecting load-test-created users from cleanup.
    After committing, clear today's UTC seen-post bucket on a best-effort basis.
    """
    feed = FEEDS.get(feed_name)
    if feed is None or not feed.public or not feed.controls:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown feed")

    supplied = body.model_fields_set
    if not supplied:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one control is required",
        )
    if any(getattr(body, control) is None for control in supplied):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Control values cannot be null",
        )
    unsupported = supplied.difference(feed.controls)
    if unsupported:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported controls for {feed_name}: {', '.join(sorted(unsupported))}",
        )

    patch = FeedPreferencesDocument.model_validate(body.model_dump(exclude_none=True))
    updated = await patch_user_feed_preferences(
        db,
        user_did,
        feed_name,
        patch,
    )
    try:
        # Seen buckets are account-wide: today's posts can become eligible again
        # across feeds. Older buckets and the raw interaction history are retained.
        await delete_most_recent_seen_bucket(db, user_did)
    except Exception:
        # Preference persistence has already committed. Seen-history cleanup is
        # best-effort and must not make the client roll back a successful save.
        logger.exception("Failed to clear seen-post history for user '%s'", user_did)
    return FeedPreferences.model_validate(updated.model_dump(exclude_none=True))

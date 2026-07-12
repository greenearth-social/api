"""Tests for scripts/backfill_posthog.py."""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

MODULE_PATH = Path(__file__).with_name("backfill_posthog.py")
spec = importlib.util.spec_from_file_location("backfill_posthog", MODULE_PATH)
assert spec and spec.loader
backfill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill)

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
EARLIER = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
USER_DID = "did:plc:abc123"


def _make_user_doc(user_did=USER_DID, username="alice.bsky.app", created_at=NOW):
    doc = MagicMock()
    doc.id = user_did.removeprefix("did:plc:")
    data = {
        "user_did": user_did,
        "username": username,
        "created_at": created_at,
        "updated_at": created_at,
        "last_seen_at": created_at,
        "debug_feeds": False,
    }
    doc.to_dict.return_value = data
    return doc


def _make_activity_doc(feed_name="your-feed", first_seen_at=EARLIER):
    doc = MagicMock()
    doc.id = feed_name
    doc.to_dict.return_value = {
        "feed_name": feed_name,
        "first_seen_at": first_seen_at,
        "last_seen_at": NOW,
    }
    return doc


def _make_interaction_doc(
    user_did=USER_DID,
    event="interactionLike",
    feed_name="your-feed",
    item_uri="at://did/post/1",
    created_at=NOW,
):
    doc = MagicMock()
    doc.to_dict.return_value = {
        "user_did": user_did,
        "item_uri": item_uri,
        "event": event,
        "feed_name": feed_name,
        "request_id": "req123",
        "feed_generated_at": created_at,
        "created_at": created_at,
    }
    return doc


@pytest.mark.asyncio
async def test_backfill_users_emits_feed_loaded_per_feed():
    ph = MagicMock()
    db = AsyncMock()

    user_doc = _make_user_doc()
    activity_doc = _make_activity_doc()

    async def _stream_users():
        yield user_doc

    async def _stream_activity(user_did):
        yield activity_doc

    await backfill.backfill_users(
        db,
        ph,
        stream_users=_stream_users,
        stream_feed_activity=_stream_activity,
        dry_run=False,
    )

    ph.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="feed_loaded",
        properties={
            "feed_name": "your-feed",
            "$set": {
                "username": "alice.bsky.app",
                "posthog_created_at": NOW.isoformat(),
            },
        },
        timestamp=EARLIER,
    )


@pytest.mark.asyncio
async def test_backfill_users_dry_run_skips_capture():
    ph = MagicMock()
    db = AsyncMock()

    user_doc = _make_user_doc()
    activity_doc = _make_activity_doc()

    async def _stream_users():
        yield user_doc

    async def _stream_activity(user_did):
        yield activity_doc

    await backfill.backfill_users(
        db,
        ph,
        stream_users=_stream_users,
        stream_feed_activity=_stream_activity,
        dry_run=True,
    )

    ph.capture.assert_not_called()


@pytest.mark.asyncio
async def test_backfill_interactions_emits_one_event_per_doc():
    ph = MagicMock()
    db = AsyncMock()

    ix_doc = _make_interaction_doc()

    async def _stream_interactions():
        yield ix_doc

    await backfill.backfill_interactions(
        db,
        ph,
        stream_interactions=_stream_interactions,
        dry_run=False,
    )

    ph.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="interactionLike",
        properties={"feed_name": "your-feed", "item_uri": "at://did/post/1"},
        timestamp=NOW,
    )


@pytest.mark.asyncio
async def test_backfill_interactions_dry_run_skips_capture():
    ph = MagicMock()
    db = AsyncMock()

    ix_doc = _make_interaction_doc()

    async def _stream_interactions():
        yield ix_doc

    await backfill.backfill_interactions(
        db,
        ph,
        stream_interactions=_stream_interactions,
        dry_run=True,
    )

    ph.capture.assert_not_called()

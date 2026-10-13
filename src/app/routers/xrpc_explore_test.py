"""Explore previews retain personalization without durable feed session state."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import Request

from ..documents import (
    FeedActivityDocument,
    FeedCacheDocument,
    FeedSnapshotDocument,
    PipelineItemMeta,
    UserDocument,
)
from ..lib.feed_cache import FeedCache
from ..lib.feed_context import decode_feed_context
from ..lib.request_context import bypass_user_caches
from ..main import app
from ..models import FeedCursor
from . import xrpc

USER_DID = "did:plc:exploreuser"
FEED_URI = "at://did:plc:publisher/app.bsky.feed.generator/your-feed"
PIN_URI = "at://ux/full-feed"
EXPLORE_PIN_URI = "at://ux/explore"
RETURNING_PIN_URI = "at://ux/returning"
SURVEY_URI = "at://ux/survey"
POST_URIS = [f"at://posts/{index}" for index in range(40)]


@pytest.fixture
def feed_session(monkeypatch):
    monkeypatch.setenv("GE_FEED_GENERATOR_DID", "did:web:test.example.com")
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "explore-test-secret")
    xrpc._clear_initial_request_cache()

    feed_cfg = xrpc.FEEDS["your-feed"].model_copy(
        update={
            "pinned_post_uri": PIN_URI,
            "explore_pinned_post_uri": EXPLORE_PIN_URI,
            "returning_pinned_post_uri": RETURNING_PIN_URI,
            "survey_post_uri": SURVEY_URI,
        }
    )
    monkeypatch.setitem(xrpc.FEEDS, "your-feed", feed_cfg)
    db = AsyncMock()
    cache = AsyncMock(spec=FeedCache)
    cache.retrieve_document.return_value = None
    monkeypatch.setattr(app.state, "firestore", db, raising=False)
    monkeypatch.setattr(app.state, "feed_cache", cache, raising=False)
    request = Request({"type": "http", "app": app, "headers": []})
    now = datetime.now(UTC)
    snapshot = FeedSnapshotDocument(
        request_id="generated-preview",
        items=POST_URIS,
        items_meta=[PipelineItemMeta(at_uri=uri) for uri in POST_URIS],
        feed_name="your-feed",
        generated_at=now,
        expires_at=now + timedelta(hours=24),
    )
    # Give this user saved preferences and a debug opt-in: Explore still honors both.
    user = UserDocument(user_did=USER_DID, freshness=0, debug_feeds=True)
    mocks = {
        "verify_auth_header": AsyncMock(return_value=USER_DID),
        "_resolve_username": AsyncMock(return_value="exploreuser.bsky.app"),
        "get_user": AsyncMock(return_value=user),
        "upsert_user": AsyncMock(),
        "upsert_feed_activity": AsyncMock(),
        "get_feed_activity": AsyncMock(
            return_value=FeedActivityDocument(feed_name="your-feed", load_count=10)
        ),
        "claim_accepted_feed_slate": AsyncMock(return_value=None),
        "_write_feed_snapshot_background": AsyncMock(),
        "_run_pipeline_capturing_with_timeout": AsyncMock(
            return_value=(snapshot, ["at://newly-discarded"])
        ),
        "get_recent_seen_uris": AsyncMock(return_value=["at://seen"]),
        "get_recent_discarded_uris": AsyncMock(return_value=["at://discarded"]),
        "record_discarded_posts": AsyncMock(),
        "get_posthog_client": MagicMock(return_value=None),
        "track_session": MagicMock(),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(xrpc, name, mock)

    async def load(limit, cursor=None):
        response = await xrpc.get_feed_skeleton(
            request=request, feed=FEED_URI, limit=limit, cursor=cursor
        )
        # Wait for the real session/discard background handlers before checking writes.
        await asyncio.gather(*xrpc._background_tasks)
        return response

    yield SimpleNamespace(
        db=db, cache=cache, user=user, request=request, snapshot=snapshot, load=load, **mocks
    )
    xrpc._clear_initial_request_cache()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [2, 8])
@pytest.mark.parametrize("empty", [False, True])
async def test_explore_skips_feed_state_but_keeps_personalization_and_analytics(
    feed_session, limit, empty
):
    state = feed_session
    if empty:
        state._run_pipeline_capturing_with_timeout.return_value = (
            state.snapshot.model_copy(update={"items": [], "items_meta": []}),
            ["at://newly-discarded"],
        )

    response = await state.load(limit)

    expected = [EXPLORE_PIN_URI, *(POST_URIS[: limit - 1] if not empty else [])]
    assert [item.post for item in response.feed] == expected
    assert response.cursor is None
    state.cache.store_document.assert_not_awaited()
    state.cache.retrieve_document.assert_not_awaited()
    state.claim_accepted_feed_slate.assert_not_awaited()
    state._write_feed_snapshot_background.assert_not_awaited()
    state.upsert_feed_activity.assert_not_awaited()
    state.get_feed_activity.assert_not_awaited()
    state.get_recent_seen_uris.assert_not_awaited()
    state.get_recent_discarded_uris.assert_not_awaited()
    state.record_discarded_posts.assert_not_awaited()

    state.get_user.assert_awaited_once_with(state.db, USER_DID)
    state.upsert_user.assert_awaited_once_with(
        state.db, USER_DID, "exploreuser.bsky.app", is_load_test=False
    )
    state.track_session.assert_called_once()
    assert state.track_session.call_args.kwargs == {
        "requested_limit": limit,
        "has_cursor": False,
    }
    pipeline_call = state._run_pipeline_capturing_with_timeout.await_args
    generation = pipeline_call.args[3]
    assert generation.num_candidates == limit * 5
    assert generation.max_age_hours == 6
    assert set(generation.exclude_uris) == {PIN_URI, EXPLORE_PIN_URI, RETURNING_PIN_URI}
    assert pipeline_call.kwargs["debug_enabled"] is True
    # Interaction attribution does not depend on a cache or snapshot being stored.
    context = decode_feed_context(response.feed[0].feed_context)
    assert context is not None
    assert context.did == USER_DID
    assert context.feed == "your-feed"
    assert context.rid == pipeline_call.kwargs["request_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [9, 30])
async def test_full_feed_load_still_records_feed_state_and_shows_eligible_survey(
    feed_session, limit
):
    state = feed_session
    response = await state.load(limit)

    assert len(response.feed) == limit
    assert response.feed[0].post == PIN_URI
    assert response.feed[5].post == SURVEY_URI
    state.claim_accepted_feed_slate.assert_awaited_once()
    state.upsert_feed_activity.assert_awaited_once_with(
        state.db, USER_DID, "your-feed", is_initial_load=True
    )
    state.get_feed_activity.assert_awaited_once_with(state.db, USER_DID, "your-feed")
    state._write_feed_snapshot_background.assert_awaited_once()
    served_snapshot = state._write_feed_snapshot_background.await_args.args[3]
    assert served_snapshot.items == POST_URIS[: limit - 2]
    state.cache.store_document.assert_awaited_once()
    stored_id, stored_document = state.cache.store_document.await_args.args
    assert stored_document.items == POST_URIS
    cursor = FeedCursor.decode(response.cursor)
    assert cursor.id == stored_id
    assert cursor.offset == limit - 2
    state.get_recent_seen_uris.assert_awaited_once_with(state.db, USER_DID)
    state.get_recent_discarded_uris.assert_awaited_once_with(state.db, USER_DID)
    generation = state._run_pipeline_capturing_with_timeout.await_args.args[3]
    assert {"at://seen", "at://discarded"}.issubset(generation.exclude_uris)
    state.record_discarded_posts.assert_awaited_once_with(
        state.db, USER_DID, ["at://newly-discarded"], load_test=False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [2, 8])
async def test_small_cursor_page_still_reads_cache_and_records_activity(feed_session, limit):
    state = feed_session
    state.cache.retrieve_document.return_value = FeedCacheDocument(
        items=POST_URIS,
        items_meta=state.snapshot.items_meta,
        user_did=USER_DID,
        feed_name="your-feed",
        generated_at=state.snapshot.generated_at,
        expires_at=state.snapshot.expires_at,
        preference_fingerprint=xrpc._configured_generation(
            "your-feed", state.user
        ).preference_fingerprint,
    )

    response = await state.load(limit, FeedCursor(id="existing-feed", offset=9).encode())

    assert [item.post for item in response.feed] == POST_URIS[9 : 9 + limit]
    assert FeedCursor.decode(response.cursor) == FeedCursor(id="existing-feed", offset=9 + limit)
    state.cache.retrieve_document.assert_awaited_once_with("existing-feed")
    state._run_pipeline_capturing_with_timeout.assert_not_awaited()
    state._write_feed_snapshot_background.assert_awaited_once()
    state.upsert_feed_activity.assert_awaited_once_with(
        state.db, USER_DID, "your-feed", is_initial_load=False
    )
    assert state.track_session.call_args.kwargs["has_cursor"] is True


@pytest.mark.asyncio
async def test_small_expired_cursor_regenerates_with_normal_persistence(feed_session):
    state = feed_session
    response = await state.load(8, FeedCursor(id="expired-feed", offset=9).encode())

    assert response.cursor is not None
    state.cache.retrieve_document.assert_awaited_once_with("expired-feed")
    state.cache.store_document.assert_awaited_once()
    state._write_feed_snapshot_background.assert_awaited_once()
    state.upsert_feed_activity.assert_awaited_once_with(
        state.db, USER_DID, "your-feed", is_initial_load=False
    )
    state.claim_accepted_feed_slate.assert_not_awaited()
    state.get_recent_seen_uris.assert_awaited_once_with(state.db, USER_DID)
    state.get_recent_discarded_uris.assert_awaited_once_with(state.db, USER_DID)
    generation = state._run_pipeline_capturing_with_timeout.await_args.args[3]
    assert {"at://seen", "at://discarded"}.issubset(generation.exclude_uris)
    state.record_discarded_posts.assert_awaited_once_with(
        state.db, USER_DID, ["at://newly-discarded"], load_test=False
    )


@pytest.mark.asyncio
async def test_full_feed_generation_after_explore_resumes_history_reads_and_writes(feed_session):
    state = feed_session
    cache_modes = []

    async def pipeline(*args, **kwargs):
        cache_modes.append(bypass_user_caches())
        return state.snapshot, ["at://newly-discarded"]

    state._run_pipeline_capturing_with_timeout.side_effect = pipeline
    await state.load(8)

    state.get_recent_seen_uris.assert_not_awaited()
    state.get_recent_discarded_uris.assert_not_awaited()
    state.record_discarded_posts.assert_not_awaited()
    assert bypass_user_caches() is False

    await state.load(30)

    assert cache_modes == [True, False]
    assert bypass_user_caches() is False
    assert state._run_pipeline_capturing_with_timeout.await_count == 2
    preview_generation, full_generation = [
        call.args[3] for call in state._run_pipeline_capturing_with_timeout.await_args_list
    ]
    assert set(preview_generation.exclude_uris) == {PIN_URI, EXPLORE_PIN_URI, RETURNING_PIN_URI}
    assert {"at://seen", "at://discarded"}.issubset(full_generation.exclude_uris)
    state.get_recent_seen_uris.assert_awaited_once_with(state.db, USER_DID)
    state.get_recent_discarded_uris.assert_awaited_once_with(state.db, USER_DID)
    state.record_discarded_posts.assert_awaited_once_with(
        state.db, USER_DID, ["at://newly-discarded"], load_test=False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "has_cursor", "expected_bypass"),
    [(2, False, True), (8, False, True), (9, False, False), (30, False, False), (8, True, False)],
)
async def test_user_cache_policy_reaches_concurrent_pipeline_tasks(
    feed_session, limit, has_cursor, expected_bypass
):
    state = feed_session
    cache_modes = []

    async def child_lookup():
        return bypass_user_caches()

    async def pipeline(*args, **kwargs):
        # Candidate generators and rankers launch concurrent lookup tasks.
        cache_modes.extend(await asyncio.gather(child_lookup(), child_lookup()))
        return state.snapshot, []

    state._run_pipeline_capturing_with_timeout.side_effect = pipeline
    cursor = FeedCursor(id="expired-feed", offset=9).encode() if has_cursor else None

    await state.load(limit, cursor)

    assert cache_modes == [expected_bypass, expected_bypass]
    assert bypass_user_caches() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_failed_explore_pipeline_restores_normal_user_cache_policy(feed_session, error_type):
    state = feed_session

    async def failing_pipeline(*args, **kwargs):
        assert bypass_user_caches() is True
        raise error_type("pipeline failed")

    state._run_pipeline_capturing_with_timeout.side_effect = failing_pipeline

    with pytest.raises(error_type, match="pipeline failed"):
        await state.load(8)
    await asyncio.gather(*xrpc._background_tasks)

    assert bypass_user_caches() is False


@pytest.mark.asyncio
async def test_reported_explore_impression_still_records_interaction_and_seen_history(
    feed_session, monkeypatch
):
    state = feed_session
    response = await state.load(8)
    post = response.feed[1]
    record_interaction = AsyncMock()
    record_seen_posts = AsyncMock()
    record_user_post_seen = AsyncMock()
    monkeypatch.setattr(xrpc, "record_interaction", record_interaction)
    monkeypatch.setattr(xrpc, "record_seen_posts", record_seen_posts)
    monkeypatch.setattr(xrpc, "record_user_post_seen", record_user_post_seen)
    monkeypatch.setattr(xrpc, "track_interaction", MagicMock())

    await xrpc.send_interactions(
        request=state.request,
        body=xrpc.SendInteractionsRequest(
            interactions=[
                xrpc.Interaction(
                    item=post.post,
                    event="app.bsky.feed.defs#interactionSeen",
                    feed_context=post.feed_context,
                )
            ]
        ),
    )
    await asyncio.gather(*xrpc._background_tasks)

    record_interaction.assert_awaited_once()
    assert record_interaction.await_args is not None
    interaction = record_interaction.await_args.args[1]
    assert interaction.item_uri == post.post
    assert interaction.user_did == USER_DID
    assert interaction.event == "interactionSeen"
    record_seen_posts.assert_awaited_once_with(state.db, USER_DID, [post.post], load_test=False)
    record_user_post_seen.assert_awaited_once_with(state.db, USER_DID)


@pytest.mark.asyncio
async def test_explore_preserves_accepted_settings_slate_for_next_full_feed_load(feed_session):
    state = feed_session
    accepted_id = "accepted-settings"
    accepted_uris = [f"at://accepted/{index}" for index in range(40)]
    state.claim_accepted_feed_slate.return_value = accepted_id
    state.cache.retrieve_document.return_value = FeedCacheDocument(
        items=accepted_uris,
        user_did=USER_DID,
        feed_name="your-feed",
        generated_at=state.snapshot.generated_at,
        expires_at=state.snapshot.expires_at,
        mode="accepted",
    )

    preview = await state.load(8)
    state.claim_accepted_feed_slate.assert_not_awaited()
    state.cache.retrieve_document.assert_not_awaited()
    assert preview.cursor is None
    assert [item.post for item in preview.feed] == [EXPLORE_PIN_URI, *POST_URIS[:7]]

    full_feed = await state.load(30)

    assert [item.post for item in full_feed.feed] == [PIN_URI, *accepted_uris[:29]]
    assert FeedCursor.decode(full_feed.cursor).id == accepted_id
    state.claim_accepted_feed_slate.assert_awaited_once()
    state.cache.retrieve_document.assert_awaited_once_with(accepted_id)
    state._write_feed_snapshot_background.assert_awaited_once()
    state.upsert_feed_activity.assert_awaited_once()
    state._run_pipeline_capturing_with_timeout.assert_awaited_once()

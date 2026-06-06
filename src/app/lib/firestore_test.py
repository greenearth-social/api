"""Tests for Firestore helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from google.cloud.firestore import ArrayUnion

from ..documents import FeedDebugDocument, InteractionDocument, UserDocument
from ..models import CandidateGenerateRequest, GeneratorSpec
from ..lib.firestore import (
    FEED_DEBUG_COLLECTION,
    INTERACTIONS_COLLECTION,
    SEEN_POSTS_COLLECTION,
    USERS_COLLECTION,
    get_feed_activity,
    get_feed_debug,
    get_recent_feed_debug,
    get_recent_seen_uris,
    get_user,
    get_user_by_username,
    init_firestore_client,
    record_interaction,
    record_seen_posts,
    set_user_debug_flag,
    upsert_feed_activity,
    upsert_user,
    write_feed_debug,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

USER_DID = "did:plc:testuser123"
USERNAME = "testuser.bsky.app"
FEED_NAME = "unranked-your-feed"


def _mock_feed_activity_client():
    db = MagicMock()
    doc_ref = AsyncMock()
    db.collection.return_value.document.return_value.collection.return_value.document.return_value = doc_ref
    return db, doc_ref


def _mock_seen_posts_write_client():
    """Mock the users/{did}/seen_posts/{bucket} document-write chain."""
    db = MagicMock()
    doc_ref = AsyncMock()
    db.collection.return_value.document.return_value.collection.return_value.document.return_value = doc_ref
    return db, doc_ref


def _async_iter(items):
    async def _gen():
        for item in items:
            yield item

    return _gen()


def _mock_seen_posts_query_client(buckets):
    """Mock the seen_posts subcollection query chain; ``buckets`` stream out."""
    db = MagicMock()
    query = MagicMock()
    query.stream.return_value = _async_iter(buckets)
    db.collection.return_value.document.return_value.collection.return_value.where.return_value = query
    return db, query


def _mock_bucket(doc_id: str, post_uris: list[str]) -> MagicMock:
    snap = MagicMock()
    snap.id = doc_id
    snap.to_dict.return_value = {"post_uris": post_uris}
    return snap


def _mock_doc_snapshot(exists: bool, data: dict | None = None) -> MagicMock:
    """Create a fake Firestore document snapshot."""
    snap = MagicMock()
    snap.exists = exists
    snap.to_dict.return_value = data
    return snap


def _mock_firestore_client() -> tuple[MagicMock, MagicMock, AsyncMock]:
    """Create a mock AsyncClient with a single collection/document chain."""
    db = MagicMock()
    doc_ref = AsyncMock()
    collection_ref = MagicMock()
    collection_ref.document.return_value = doc_ref
    db.collection.return_value = collection_ref
    return db, collection_ref, doc_ref


# ---------------------------------------------------------------------------
# init_firestore_client
# ---------------------------------------------------------------------------


class TestInitFirestoreClient:
    @patch("app.lib.firestore.AsyncClient")
    def test_creates_client(self, MockAsyncClient, monkeypatch):
        monkeypatch.delenv("GE_FIRESTORE_EMULATOR_HOST", raising=False)
        monkeypatch.delenv("GE_FIRESTORE_PROJECT", raising=False)
        monkeypatch.setenv("PROJECT_ID", "test-project")

        init_firestore_client()

        MockAsyncClient.assert_called_once_with(project="test-project", database="(default)")

    @patch("app.lib.firestore.AsyncClient")
    def test_sets_emulator_host(self, MockAsyncClient, monkeypatch):
        monkeypatch.setenv("GE_FIRESTORE_EMULATOR_HOST", "localhost:8081")
        monkeypatch.delenv("GE_FIRESTORE_PROJECT", raising=False)
        monkeypatch.setenv("PROJECT_ID", "test-project")

        init_firestore_client()

        # Verify the standard env var was set for the SDK
        assert "FIRESTORE_EMULATOR_HOST" in __import__("os").environ

    @patch("app.lib.firestore.AsyncClient")
    def test_ge_project_env_takes_precedence(self, MockAsyncClient, monkeypatch):
        monkeypatch.setenv("GE_FIRESTORE_PROJECT", "ge-project")
        monkeypatch.setenv("PROJECT_ID", "other-project")

        init_firestore_client()

        MockAsyncClient.assert_called_once_with(project="ge-project", database="(default)")

    @patch("app.lib.firestore.AsyncClient")
    def test_ge_database_env_takes_precedence(self, MockAsyncClient, monkeypatch):
        monkeypatch.setenv("GE_FIRESTORE_PROJECT", "ge-project")
        monkeypatch.setenv("GE_FIRESTORE_DATABASE", "greenearth-stage")

        init_firestore_client()

        MockAsyncClient.assert_called_once_with(project="ge-project", database="greenearth-stage")

    @patch("app.lib.firestore.AsyncClient")
    def test_emulator_defaults_project_when_unset(self, MockAsyncClient, monkeypatch):
        monkeypatch.setenv("GE_FIRESTORE_EMULATOR_HOST", "localhost:8080")
        monkeypatch.delenv("GE_FIRESTORE_PROJECT", raising=False)
        monkeypatch.delenv("PROJECT_ID", raising=False)

        init_firestore_client()

        MockAsyncClient.assert_called_once_with(project="demo-no-project", database="(default)")


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


class TestGetUser:
    @pytest.mark.asyncio
    async def test_returns_user_when_exists(self):
        db, _, doc_ref = _mock_firestore_client()
        now = datetime.now(timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "user_did": USER_DID,
            "username": USERNAME,
            "created_at": now,
            "updated_at": now,
            "last_seen_at": now,
        })

        user = await get_user(db, USER_DID)

        assert user is not None
        assert user.user_did == USER_DID
        assert user.username == USERNAME
        assert user.created_at == now
        db.collection.assert_called_with(USERS_COLLECTION)

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        db, _, doc_ref = _mock_firestore_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        user = await get_user(db, USER_DID)

        assert user is None


# ---------------------------------------------------------------------------
# upsert_user
# ---------------------------------------------------------------------------


class TestUpsertUser:
    @pytest.mark.asyncio
    async def test_creates_new_user(self):
        db, _, doc_ref = _mock_firestore_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        user = await upsert_user(db, USER_DID, USERNAME)

        assert user.user_did == USER_DID
        assert user.username == USERNAME
        assert isinstance(user.created_at, datetime)
        assert isinstance(user.updated_at, datetime)
        doc_ref.set.assert_called_once()

        # Verify the data written
        written = doc_ref.set.call_args[0][0]
        assert written["user_did"] == USER_DID
        assert written["username"] == USERNAME
        assert "created_at" in written
        assert "updated_at" in written
        assert "last_seen_at" in written

    @pytest.mark.asyncio
    async def test_updates_existing_user(self):
        db, _, doc_ref = _mock_firestore_client()
        original_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "user_did": USER_DID,
            "username": USERNAME,
            "created_at": original_time,
            "updated_at": original_time,
            "last_seen_at": original_time,
        })

        user = await upsert_user(db, USER_DID, USERNAME)

        assert user.user_did == USER_DID
        assert user.username == USERNAME
        # created_at and updated_at should be preserved from original
        assert user.created_at == original_time
        assert user.updated_at == original_time
        # last_seen_at should be refreshed
        assert user.last_seen_at > original_time
        doc_ref.update.assert_called_once()
        update_fields = doc_ref.update.call_args[0][0]
        assert "username" not in update_fields
        doc_ref.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_updates_username_when_changed(self):
        db, _, doc_ref = _mock_firestore_client()
        original_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "user_did": USER_DID,
            "username": "old-handle.bsky.app",
            "created_at": original_time,
            "updated_at": original_time,
            "last_seen_at": original_time,
        })

        user = await upsert_user(db, USER_DID, USERNAME)

        assert user.user_did == USER_DID
        assert user.username == USERNAME
        assert user.created_at == original_time
        assert user.updated_at > original_time
        assert user.last_seen_at > original_time
        doc_ref.update.assert_called_once()
        update_fields = doc_ref.update.call_args[0][0]
        assert update_fields["username"] == USERNAME
        assert "updated_at" in update_fields


# ---------------------------------------------------------------------------
# get_feed_activity
# ---------------------------------------------------------------------------


class TestGetFeedActivity:
    @pytest.mark.asyncio
    async def test_returns_doc_when_exists(self):
        db, doc_ref = _mock_feed_activity_client()
        now = datetime.now(timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "feed_name": FEED_NAME,
            "first_seen_at": now,
            "last_seen_at": now,
        })

        activity = await get_feed_activity(db, USER_DID, FEED_NAME)

        assert activity is not None
        assert activity.feed_name == FEED_NAME
        assert activity.first_seen_at == now
        assert activity.last_seen_at == now

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        db, doc_ref = _mock_feed_activity_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        activity = await get_feed_activity(db, USER_DID, FEED_NAME)

        assert activity is None


# ---------------------------------------------------------------------------
# upsert_feed_activity
# ---------------------------------------------------------------------------


class TestUpsertFeedActivity:
    @pytest.mark.asyncio
    async def test_creates_new_doc_on_first_visit(self):
        db, doc_ref = _mock_feed_activity_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        activity = await upsert_feed_activity(db, USER_DID, FEED_NAME)

        assert activity.feed_name == FEED_NAME
        assert isinstance(activity.first_seen_at, datetime)
        assert isinstance(activity.last_seen_at, datetime)
        doc_ref.set.assert_called_once()
        written = doc_ref.set.call_args[0][0]
        assert written["feed_name"] == FEED_NAME
        assert "first_seen_at" in written
        assert "last_seen_at" in written

    @pytest.mark.asyncio
    async def test_updates_only_last_seen_at_on_revisit(self):
        db, doc_ref = _mock_feed_activity_client()
        original_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "feed_name": FEED_NAME,
            "first_seen_at": original_time,
            "last_seen_at": original_time,
        })

        activity = await upsert_feed_activity(db, USER_DID, FEED_NAME)

        assert activity.last_seen_at > original_time
        doc_ref.update.assert_called_once()
        update_fields = doc_ref.update.call_args[0][0]
        assert "last_seen_at" in update_fields
        assert "first_seen_at" not in update_fields
        doc_ref.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_overwrite_first_seen_at(self):
        db, doc_ref = _mock_feed_activity_client()
        original_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "feed_name": FEED_NAME,
            "first_seen_at": original_time,
            "last_seen_at": original_time,
        })

        activity = await upsert_feed_activity(db, USER_DID, FEED_NAME)

        assert activity.first_seen_at == original_time


# ---------------------------------------------------------------------------
# record_interaction
# ---------------------------------------------------------------------------


class TestRecordInteraction:
    @pytest.mark.asyncio
    async def test_adds_auto_id_doc_to_interactions_collection(self):
        db = MagicMock()
        collection_ref = MagicMock()
        collection_ref.add = AsyncMock()
        db.collection.return_value = collection_ref

        doc = InteractionDocument(
            user_did=USER_DID,
            item_uri="at://post/1",
            event="interactionLike",
            feed_name=FEED_NAME,
            request_id="req-1",
        )

        await record_interaction(db, doc)

        db.collection.assert_called_once_with(INTERACTIONS_COLLECTION)
        collection_ref.add.assert_called_once()
        written = collection_ref.add.call_args[0][0]
        assert written["user_did"] == USER_DID
        assert written["item_uri"] == "at://post/1"
        assert written["event"] == "interactionLike"
        assert written["feed_name"] == FEED_NAME
        assert written["request_id"] == "req-1"
        assert "created_at" in written


# ---------------------------------------------------------------------------
# record_seen_posts
# ---------------------------------------------------------------------------


class TestRecordSeenPosts:
    @pytest.mark.asyncio
    async def test_writes_today_bucket_with_array_union_and_expiry(self):
        db, doc_ref = _mock_seen_posts_write_client()

        await record_seen_posts(db, USER_DID, ["at://post/1", "at://post/2"])

        # Subcollection is keyed under the user document.
        db.collection.assert_called_with(USERS_COLLECTION)
        db.collection.return_value.document.return_value.collection.assert_called_with(
            SEEN_POSTS_COLLECTION
        )
        # Bucket id is the current UTC date.
        bucket_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        db.collection.return_value.document.return_value.collection.return_value.document.assert_called_with(
            bucket_id
        )

        doc_ref.set.assert_called_once()
        written, kwargs = doc_ref.set.call_args
        payload = written[0]
        assert kwargs == {"merge": True}
        assert isinstance(payload["post_uris"], ArrayUnion)
        assert payload["post_uris"].values == ["at://post/1", "at://post/2"]
        assert payload["expires_at"] > datetime.now(timezone.utc)

    @pytest.mark.asyncio
    async def test_noop_on_empty(self):
        db, doc_ref = _mock_seen_posts_write_client()

        await record_seen_posts(db, USER_DID, [])

        doc_ref.set.assert_not_called()


# ---------------------------------------------------------------------------
# get_recent_seen_uris
# ---------------------------------------------------------------------------


class TestGetRecentSeenUris:
    @pytest.mark.asyncio
    async def test_unions_buckets_newest_first_and_dedups(self):
        buckets = [
            _mock_bucket("2026-06-01", ["at://a", "at://b"]),
            _mock_bucket("2026-06-02", ["at://b", "at://c"]),
        ]
        db, query = _mock_seen_posts_query_client(buckets)

        uris = await get_recent_seen_uris(db, USER_DID)

        # Newest bucket first, duplicate "at://b" collapsed.
        assert uris == ["at://b", "at://c", "at://a"]
        # Query filters on a future expiry.
        where_args = db.collection.return_value.document.return_value.collection.return_value.where.call_args[0]
        assert where_args[0] == "expires_at"
        assert where_args[1] == ">"

    @pytest.mark.asyncio
    async def test_caps_at_max_uris(self):
        many = [f"at://post/{i}" for i in range(1500)]
        buckets = [_mock_bucket("2026-06-02", many)]
        db, _ = _mock_seen_posts_query_client(buckets)

        uris = await get_recent_seen_uris(db, USER_DID, max_uris=1000)

        assert len(uris) == 1000
        assert uris[0] == "at://post/0"
        assert uris[-1] == "at://post/999"


# ---------------------------------------------------------------------------
# Feed debug flag + records
# ---------------------------------------------------------------------------

REQUEST_ID = "req-abc123"


def _feed_debug_doc() -> FeedDebugDocument:
    now = datetime.now(timezone.utc)
    return FeedDebugDocument(
        request_id=REQUEST_ID,
        user_did=USER_DID,
        username=USERNAME,
        feed_name=FEED_NAME,
        generate_request=CandidateGenerateRequest(
            generators=[GeneratorSpec(name="post_similarity", weight=1.0)],
            user_did=USER_DID,
            num_candidates=10,
            video_only=False,
            infill=None,
        ),
        final_order=["at://p/1", "at://p/2"],
        generated_at=now,
        expires_at=now + timedelta(days=7),
    )


class TestGetUserByUsername:
    @pytest.mark.asyncio
    async def test_returns_first_match(self):
        now = datetime.now(timezone.utc)
        snap = _mock_doc_snapshot(True, {
            "user_did": USER_DID,
            "username": USERNAME,
            "created_at": now,
            "updated_at": now,
            "last_seen_at": now,
        })
        db = MagicMock()
        query = MagicMock()
        query.stream.return_value = _async_iter([snap])
        db.collection.return_value.where.return_value.limit.return_value = query

        user = await get_user_by_username(db, USERNAME)

        assert user is not None
        assert user.user_did == USER_DID
        db.collection.assert_called_with(USERS_COLLECTION)

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self):
        db = MagicMock()
        query = MagicMock()
        query.stream.return_value = _async_iter([])
        db.collection.return_value.where.return_value.limit.return_value = query

        assert await get_user_by_username(db, USERNAME) is None


class TestSetUserDebugFlag:
    @pytest.mark.asyncio
    async def test_updates_flag(self):
        db, _, doc_ref = _mock_firestore_client()
        doc_ref.get.return_value = _mock_doc_snapshot(True, {"user_did": USER_DID})

        await set_user_debug_flag(db, USER_DID, True)

        doc_ref.update.assert_called_once()
        written = doc_ref.update.call_args[0][0]
        assert written["debug_feeds"] is True
        assert "updated_at" in written

    @pytest.mark.asyncio
    async def test_raises_when_user_missing(self):
        db, _, doc_ref = _mock_firestore_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        with pytest.raises(ValueError):
            await set_user_debug_flag(db, USER_DID, True)


class TestWriteFeedDebug:
    @pytest.mark.asyncio
    async def test_writes_to_subcollection(self):
        db, doc_ref = _mock_feed_activity_client()

        await write_feed_debug(db, _feed_debug_doc())

        doc_ref.set.assert_called_once()
        written = doc_ref.set.call_args[0][0]
        assert written["request_id"] == REQUEST_ID
        assert written["final_order"] == ["at://p/1", "at://p/2"]


class TestGetRecentFeedDebug:
    @pytest.mark.asyncio
    async def test_returns_records(self):
        doc = _feed_debug_doc()
        snap = _mock_doc_snapshot(True, doc.model_dump())
        db = MagicMock()
        query = MagicMock()
        query.stream.return_value = _async_iter([snap])
        (
            db.collection.return_value.document.return_value.collection.return_value.order_by.return_value.limit.return_value
        ) = query

        docs = await get_recent_feed_debug(db, USER_DID, limit=5)

        assert len(docs) == 1
        assert docs[0].request_id == REQUEST_ID


class TestGetFeedDebug:
    @pytest.mark.asyncio
    async def test_returns_record(self):
        doc = _feed_debug_doc()
        db, doc_ref = _mock_feed_activity_client()
        doc_ref.get.return_value = _mock_doc_snapshot(True, doc.model_dump())

        result = await get_feed_debug(db, USER_DID, REQUEST_ID)

        assert result is not None
        assert result.request_id == REQUEST_ID

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self):
        db, doc_ref = _mock_feed_activity_client()
        doc_ref.get.return_value = _mock_doc_snapshot(False)

        assert await get_feed_debug(db, USER_DID, REQUEST_ID) is None

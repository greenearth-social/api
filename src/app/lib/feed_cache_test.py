"""Tests for the feed cache abstraction and Firestore implementation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from ..lib.feed_cache import (
    DEFAULT_TTL_SECONDS,
    FEED_CACHE_COLLECTION,
    FirestoreFeedCache,
)
from ..models import FeedCursor


# ---------------------------------------------------------------------------
# FeedCursor model
# ---------------------------------------------------------------------------

class TestFeedCursor:
    def test_roundtrip_encode_decode(self):
        cursor = FeedCursor(id="abc123", offset=30)
        raw = cursor.encode()
        decoded = FeedCursor.decode(raw)
        assert decoded.id == "abc123"
        assert decoded.offset == 30
        assert decoded.v == 1

    def test_decode_invalid_string_raises(self):
        with pytest.raises(ValueError, match="Invalid cursor"):
            FeedCursor.decode("not-valid!")

    def test_decode_bad_json_raises(self):
        import base64
        raw = base64.urlsafe_b64encode(b"not json").decode()
        with pytest.raises(ValueError, match="Invalid cursor"):
            FeedCursor.decode(raw)

    def test_decode_missing_fields_raises(self):
        import base64
        raw = base64.urlsafe_b64encode(b'{"v": 1}').decode()
        with pytest.raises(ValueError, match="Invalid cursor"):
            FeedCursor.decode(raw)

    def test_offset_must_be_non_negative(self):
        with pytest.raises(Exception):
            FeedCursor(id="x", offset=-1)

    def test_default_version(self):
        cursor = FeedCursor(id="x", offset=0)
        assert cursor.v == 1


# ---------------------------------------------------------------------------
# Firestore helpers
# ---------------------------------------------------------------------------

def _mock_firestore_client() -> tuple[MagicMock, MagicMock, AsyncMock]:
    db = MagicMock()
    doc_ref = AsyncMock()
    collection_ref = MagicMock()
    collection_ref.document.return_value = doc_ref
    db.collection.return_value = collection_ref
    return db, collection_ref, doc_ref


# ---------------------------------------------------------------------------
# FirestoreFeedCache.store
# ---------------------------------------------------------------------------

class TestFirestoreFeedCacheStore:
    @pytest.mark.asyncio
    async def test_stores_items_and_expiry(self):
        db, col_ref, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        await cache.store("key1", ["at://a/1", "at://a/2"], ttl_seconds=600)

        db.collection.assert_called_once_with(FEED_CACHE_COLLECTION)
        col_ref.document.assert_called_once_with("key1")
        doc_ref.set.assert_awaited_once()

        stored = doc_ref.set.call_args[0][0]
        assert stored["items"] == ["at://a/1", "at://a/2"]
        assert "expires_at" in stored
        # expires_at should be roughly 10 minutes from now.
        delta = stored["expires_at"] - datetime.now(timezone.utc)
        assert timedelta(seconds=590) < delta < timedelta(seconds=610)


# ---------------------------------------------------------------------------
# FirestoreFeedCache.retrieve
# ---------------------------------------------------------------------------

class TestFirestoreFeedCacheRetrieve:
    @pytest.mark.asyncio
    async def test_returns_items_when_not_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result == ["at://a/1"]

    @pytest.mark.asyncio
    async def test_returns_none_when_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) - timedelta(minutes=1),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_document_missing(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = False
        doc_ref.get.return_value = snap

        result = await cache.retrieve("missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_to_dict_is_none(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = None
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_handles_naive_datetime_from_firestore(self):
        """Firestore sometimes returns naive datetimes; they should be treated as UTC."""
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        # A naive datetime in the future.
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.retrieve("key1")
        assert result == ["at://a/1"]


# ---------------------------------------------------------------------------
# FirestoreFeedCache.append
# ---------------------------------------------------------------------------

class TestFirestoreFeedCacheAppend:
    @pytest.mark.asyncio
    async def test_appends_items_to_existing_doc(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2", "at://a/3"])
        assert result == ["at://a/1", "at://a/2", "at://a/3"]
        doc_ref.update.assert_awaited_once_with({"items": ["at://a/1", "at://a/2", "at://a/3"]})

    @pytest.mark.asyncio
    async def test_returns_none_when_document_missing(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = False
        doc_ref.get.return_value = snap

        result = await cache.append("missing", ["at://a/1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_expired(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = {
            "items": ["at://a/1"],
            "expires_at": datetime.now(timezone.utc) - timedelta(minutes=1),
        }
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"])
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_to_dict_is_none(self):
        db, _col, doc_ref = _mock_firestore_client()
        cache = FirestoreFeedCache(db)

        snap = MagicMock()
        snap.exists = True
        snap.to_dict.return_value = None
        doc_ref.get.return_value = snap

        result = await cache.append("key1", ["at://a/2"])
        assert result is None

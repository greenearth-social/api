"""Tests for Firestore helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ..documents import UserDocument
from ..lib.firestore import (
    USERS_COLLECTION,
    get_user,
    init_firestore_client,
    upsert_user,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

USER_DID = "did:plc:testuser123"


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
            "created_at": now,
            "updated_at": now,
            "last_seen_at": now,
        })

        user = await get_user(db, USER_DID)

        assert user is not None
        assert user.user_did == USER_DID
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

        user = await upsert_user(db, USER_DID)

        assert user.user_did == USER_DID
        assert isinstance(user.created_at, datetime)
        assert isinstance(user.updated_at, datetime)
        doc_ref.set.assert_called_once()

        # Verify the data written
        written = doc_ref.set.call_args[0][0]
        assert written["user_did"] == USER_DID
        assert "created_at" in written
        assert "updated_at" in written
        assert "last_seen_at" in written

    @pytest.mark.asyncio
    async def test_updates_existing_user(self):
        db, _, doc_ref = _mock_firestore_client()
        original_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc_ref.get.return_value = _mock_doc_snapshot(True, {
            "user_did": USER_DID,
            "created_at": original_time,
            "updated_at": original_time,
            "last_seen_at": original_time,
        })

        user = await upsert_user(db, USER_DID)

        assert user.user_did == USER_DID
        # created_at and updated_at should be preserved from original
        assert user.created_at == original_time
        assert user.updated_at == original_time
        # last_seen_at should be refreshed
        assert user.last_seen_at > original_time
        doc_ref.update.assert_called_once()
        doc_ref.set.assert_not_called()

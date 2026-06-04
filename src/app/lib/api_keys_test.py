"""Tests for api_keys module."""
from __future__ import annotations

import hmac

import pytest

from ..lib.api_keys import (
    FULL_KEY_LEN,
    KEY_PREFIX,
    _hash_key,
    generate_key,
    parse_key_id,
)


class TestGenerateKey:
    def test_returns_three_values(self):
        result = generate_key()
        assert len(result) == 3

    def test_full_key_format(self):
        key_id, full_key, key_hash = generate_key()
        assert full_key.startswith(KEY_PREFIX)
        assert len(full_key) == FULL_KEY_LEN

    def test_key_id_is_8_hex_chars(self):
        key_id, full_key, key_hash = generate_key()
        assert len(key_id) == 8
        int(key_id, 16)

    def test_key_hash_is_hex(self):
        key_id, full_key, key_hash = generate_key()
        assert len(key_hash) == 64
        int(key_hash, 16)

    def test_hash_verifies(self):
        key_id, full_key, key_hash = generate_key()
        assert hmac.compare_digest(_hash_key(full_key), key_hash)

    def test_keys_are_unique(self):
        keys = [generate_key()[1] for _ in range(10)]
        assert len(set(keys)) == 10


class TestHashKey:
    def test_deterministic(self):
        assert _hash_key("gea_key") == _hash_key("gea_key")

    def test_different_keys_produce_different_hashes(self):
        assert _hash_key("gea_key1") != _hash_key("gea_key2")


class TestParseKeyId:
    def test_returns_key_id_from_valid_key(self):
        key_id, full_key, _ = generate_key()
        assert parse_key_id(full_key) == key_id

    def test_returns_none_for_wrong_prefix(self):
        assert parse_key_id("bad_" + "a" * 56) is None

    def test_returns_none_for_wrong_length(self):
        assert parse_key_id("gea_tooshort") is None

    def test_returns_none_for_none(self):
        assert parse_key_id(None) is None

    def test_returns_none_for_empty_string(self):
        assert parse_key_id("") is None


from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from ..documents import ApiKeyDocument
from ..lib.api_keys import (
    API_KEYS_COLLECTION,
    authenticate_api_key,
    create_api_key,
    get_api_key_doc,
    list_api_keys,
    revoke_api_key,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_doc_data(
    key_id: str = "a1b2c3d4",
    is_active: bool = True,
    monthly_period: str = "2026-05",
    monthly_call_count: int = 0,
) -> dict:
    now = datetime.now(timezone.utc)
    _, _, key_hash = generate_key()
    return {
        "key_id": key_id,
        "key_hash": key_hash,
        "email": "test@example.com",
        "is_active": is_active,
        "created_at": now,
        "last_used_at": now,
        "monthly_call_count": monthly_call_count,
        "monthly_period": monthly_period,
    }


def _mock_firestore(doc_data: dict | None = None, exists: bool = True):
    db = MagicMock()
    doc_ref = AsyncMock()
    snap = MagicMock()
    snap.exists = exists
    snap.to_dict.return_value = doc_data
    doc_ref.get.return_value = snap
    db.collection.return_value.document.return_value = doc_ref
    return db, doc_ref


# ---------------------------------------------------------------------------
# get_api_key_doc
# ---------------------------------------------------------------------------


class TestGetApiKeyDoc:
    @pytest.mark.asyncio
    async def test_returns_document_when_exists(self):
        data = _make_doc_data()
        db, _ = _mock_firestore(data, exists=True)

        result = await get_api_key_doc(db, "a1b2c3d4")

        assert result is not None
        assert result.key_id == "a1b2c3d4"
        assert result.email == "test@example.com"
        db.collection.assert_called_with(API_KEYS_COLLECTION)

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        db, _ = _mock_firestore(exists=False)

        result = await get_api_key_doc(db, "a1b2c3d4")

        assert result is None


# ---------------------------------------------------------------------------
# create_api_key
# ---------------------------------------------------------------------------


class TestCreateApiKey:
    @pytest.mark.asyncio
    async def test_returns_document_and_plaintext_key(self):
        db, doc_ref = _mock_firestore(exists=False)

        doc, full_key = await create_api_key(db, "alice@example.com")

        assert isinstance(doc, ApiKeyDocument)
        assert doc.email == "alice@example.com"
        assert doc.is_active is True
        assert full_key.startswith("gea_")
        assert len(full_key) == FULL_KEY_LEN
        doc_ref.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_written_data_contains_hash_not_plaintext(self):
        db, doc_ref = _mock_firestore(exists=False)

        doc, full_key = await create_api_key(db, "alice@example.com")

        written = doc_ref.set.call_args[0][0]
        assert "key_hash" in written
        assert full_key not in str(written)

    @pytest.mark.asyncio
    async def test_monthly_period_set_to_current_month(self):
        db, doc_ref = _mock_firestore(exists=False)

        doc, _ = await create_api_key(db, "alice@example.com")

        now = datetime.now(timezone.utc)
        assert doc.monthly_period == now.strftime("%Y-%m")


# ---------------------------------------------------------------------------
# authenticate_api_key
# ---------------------------------------------------------------------------


class TestAuthenticateApiKey:
    @pytest.mark.asyncio
    async def test_returns_none_for_none_key(self):
        db, _ = _mock_firestore(exists=False)
        assert await authenticate_api_key(db, None) is None

    @pytest.mark.asyncio
    async def test_returns_none_for_invalid_format(self):
        db, _ = _mock_firestore(exists=False)
        assert await authenticate_api_key(db, "invalid") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_key_not_found(self):
        db, _ = _mock_firestore(exists=False)
        fake_key = "gea_" + "a" * 8 + "b" * 48
        assert await authenticate_api_key(db, fake_key) is None

    @pytest.mark.asyncio
    async def test_returns_none_when_key_inactive(self):
        key_id, full_key, key_hash = generate_key()
        data = _make_doc_data(key_id=key_id, is_active=False)
        data["key_hash"] = key_hash
        db, _ = _mock_firestore(data, exists=True)

        assert await authenticate_api_key(db, full_key) is None

    @pytest.mark.asyncio
    async def test_returns_none_for_wrong_secret(self):
        key_id, full_key, key_hash = generate_key()
        data = _make_doc_data(key_id=key_id, is_active=True)
        data["key_hash"] = key_hash
        db, _ = _mock_firestore(data, exists=True)

        tampered_key = full_key[:-1] + ("x" if full_key[-1] != "x" else "y")
        assert await authenticate_api_key(db, tampered_key) is None

    @pytest.mark.asyncio
    async def test_returns_document_for_valid_key(self):
        key_id, full_key, key_hash = generate_key()
        data = _make_doc_data(key_id=key_id, is_active=True, monthly_period="2026-05")
        data["key_hash"] = key_hash
        db, doc_ref = _mock_firestore(data, exists=True)

        result = await authenticate_api_key(db, full_key)

        assert result is not None
        assert result.key_id == key_id
        doc_ref.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_increments_call_count_same_month(self):
        key_id, full_key, key_hash = generate_key()
        now = datetime.now(timezone.utc)
        data = _make_doc_data(key_id=key_id, monthly_period=now.strftime("%Y-%m"))
        data["key_hash"] = key_hash
        db, doc_ref = _mock_firestore(data, exists=True)

        await authenticate_api_key(db, full_key)

        update_args = doc_ref.update.call_args[0][0]
        assert "monthly_call_count" in update_args

    @pytest.mark.asyncio
    async def test_resets_counters_on_new_month(self):
        key_id, full_key, key_hash = generate_key()
        data = _make_doc_data(key_id=key_id, monthly_period="2025-01", monthly_call_count=100)
        data["key_hash"] = key_hash
        db, doc_ref = _mock_firestore(data, exists=True)

        await authenticate_api_key(db, full_key)

        update_args = doc_ref.update.call_args[0][0]
        assert update_args["monthly_call_count"] == 1
        assert "monthly_period" in update_args


# ---------------------------------------------------------------------------
# list_api_keys
# ---------------------------------------------------------------------------


class TestListApiKeys:
    @pytest.mark.asyncio
    async def test_returns_all_documents(self):
        db = MagicMock()
        docs_data = [_make_doc_data("key00001"), _make_doc_data("key00002")]

        async def mock_stream():
            for data in docs_data:
                snap = MagicMock()
                snap.to_dict.return_value = data
                yield snap

        db.collection.return_value.stream.return_value = mock_stream()

        result = await list_api_keys(db)

        assert len(result) == 2
        assert result[0].key_id == "key00001"
        assert result[1].key_id == "key00002"

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_keys(self):
        db = MagicMock()

        async def mock_stream():
            return
            yield

        db.collection.return_value.stream.return_value = mock_stream()

        result = await list_api_keys(db)

        assert result == []


# ---------------------------------------------------------------------------
# revoke_api_key
# ---------------------------------------------------------------------------


class TestRevokeApiKey:
    @pytest.mark.asyncio
    async def test_returns_true_and_sets_inactive(self):
        data = _make_doc_data(is_active=True)
        db, doc_ref = _mock_firestore(data, exists=True)

        result = await revoke_api_key(db, "a1b2c3d4")

        assert result is True
        doc_ref.update.assert_called_once_with({"is_active": False})

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self):
        db, _ = _mock_firestore(exists=False)

        result = await revoke_api_key(db, "a1b2c3d4")

        assert result is False

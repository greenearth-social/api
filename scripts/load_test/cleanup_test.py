"""Tests for scripts/load_test/cleanup.py (Firestore mocked)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from load_test import cleanup


class _AsyncIter:
    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


def _user_snap(did: str, flagged: bool = True, username: str | None = None):
    snap = MagicMock()
    snap.id = did.removeprefix("did:plc:")
    snap.to_dict.return_value = {
        "user_did": did,
        "username": username,
        "created_by_load_test": flagged,
    }
    return snap


def _make_db(*, user_stream, reread=None, flagged_stream=None):
    """Build a mock AsyncClient.

    ``user_stream``   snapshots returned by the created_by_load_test query.
    ``reread``        maps doc_id -> snapshot returned by document(id).get().
    ``flagged_stream``snapshots returned by any load_test==True collection query.
    """
    db = MagicMock()
    reread = reread or {}

    query = MagicMock()
    query.stream.side_effect = lambda: _AsyncIter(list(user_stream))

    flagged_query = MagicMock()
    flagged_query.stream.side_effect = lambda: _AsyncIter(list(flagged_stream or []))

    def _where(*, filter):  # noqa: A002 - mirrors the Firestore kwarg name
        field = getattr(filter, "field_path", "")
        # created_by_load_test query (users) vs load_test query (interactions/cache).
        return query if field == "created_by_load_test" else flagged_query

    def _document(doc_id):
        ref = MagicMock()
        ref.get = AsyncMock(return_value=reread.get(doc_id))
        return ref

    collection = MagicMock()
    collection.where.side_effect = _where
    collection.document.side_effect = _document
    db.collection.return_value = collection
    db.recursive_delete = AsyncMock()
    return db


@pytest.mark.asyncio
async def test_dry_run_deletes_nothing():
    db = _make_db(user_stream=[_user_snap("did:plc:a"), _user_snap("did:plc:b")])
    deleted, skipped = await cleanup._delete_test_users(db, execute=False, restrict=None)
    assert deleted == 2
    assert skipped == 0
    db.recursive_delete.assert_not_called()


@pytest.mark.asyncio
async def test_execute_deletes_flagged_user():
    reread = {"a": _user_snap("did:plc:a", flagged=True)}
    db = _make_db(user_stream=[_user_snap("did:plc:a")], reread=reread)
    deleted, skipped = await cleanup._delete_test_users(db, execute=True, restrict=None)
    assert deleted == 1
    assert skipped == 0
    db.recursive_delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_skips_user_that_became_real():
    # The re-read shows the flag cleared between query and delete.
    reread = {"a": _user_snap("did:plc:a", flagged=False)}
    db = _make_db(user_stream=[_user_snap("did:plc:a")], reread=reread)
    deleted, skipped = await cleanup._delete_test_users(db, execute=True, restrict=None)
    assert deleted == 0
    assert skipped == 1
    db.recursive_delete.assert_not_called()


@pytest.mark.asyncio
async def test_restrict_limits_to_named_dids():
    db = _make_db(
        user_stream=[_user_snap("did:plc:a"), _user_snap("did:plc:b")],
        reread={"a": _user_snap("did:plc:a", flagged=True)},
    )
    deleted, skipped = await cleanup._delete_test_users(db, execute=True, restrict={"did:plc:a"})
    assert deleted == 1
    db.recursive_delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_flagged_counts_and_deletes():
    doc1, doc2 = MagicMock(), MagicMock()
    doc1.reference.delete = AsyncMock()
    doc2.reference.delete = AsyncMock()
    db = _make_db(user_stream=[], flagged_stream=[doc1, doc2])
    count = await cleanup._delete_flagged(db, "interactions", execute=True)
    assert count == 2
    doc1.reference.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_flagged_dry_run_does_not_delete():
    doc1 = MagicMock()
    doc1.reference.delete = AsyncMock()
    db = _make_db(user_stream=[], flagged_stream=[doc1])
    count = await cleanup._delete_flagged(db, "feed_cache", execute=False)
    assert count == 1
    doc1.reference.delete.assert_not_called()

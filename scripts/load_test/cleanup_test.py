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
    deleted, skipped = await cleanup._delete_test_users(db, execute=False)
    assert deleted == 2
    assert skipped == 0
    db.recursive_delete.assert_not_called()


@pytest.mark.asyncio
async def test_execute_deletes_flagged_user():
    reread = {"a": _user_snap("did:plc:a", flagged=True)}
    db = _make_db(user_stream=[_user_snap("did:plc:a")], reread=reread)
    deleted, skipped = await cleanup._delete_test_users(db, execute=True)
    assert deleted == 1
    assert skipped == 0
    db.recursive_delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_skips_user_that_became_real():
    # The re-read shows the flag cleared between query and delete.
    reread = {"a": _user_snap("did:plc:a", flagged=False)}
    db = _make_db(user_stream=[_user_snap("did:plc:a")], reread=reread)
    deleted, skipped = await cleanup._delete_test_users(db, execute=True)
    assert deleted == 0
    assert skipped == 1
    db.recursive_delete.assert_not_called()


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


# --- dev fail-closed ---------------------------------------------------------


def test_dev_requires_emulator(monkeypatch):
    monkeypatch.delenv("GE_FIRESTORE_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("FIRESTORE_EMULATOR_HOST", raising=False)
    with pytest.raises(SystemExit):
        cleanup._configure_environment("dev")


def test_dev_with_emulator_ok(monkeypatch):
    monkeypatch.setenv("GE_FIRESTORE_EMULATOR_HOST", "127.0.0.1:8080")
    cleanup._configure_environment("dev")  # no raise


# --- suffixed activity-bucket cleanup ---------------------------------------


def _bucket_doc(doc_id: str):
    doc = MagicMock()
    doc.id = doc_id
    doc.reference.delete = AsyncMock()
    return doc


def _make_bucket_db(
    buckets_by_collection: dict[str, list], *, user_exists: bool = True, user_flag: bool = False
):
    """Mock users/{did} doc get() + its {collection}/ subcollection streams.

    ``user_exists``/``user_flag`` drive the doc.get() the bucket cleaner uses to
    decide whether a manifest DID is a real user (clean its suffixed buckets) or
    a test-created/deleted one (skip — recursive_delete owns it).
    """
    db = MagicMock()

    def _collection(name):
        if name == cleanup.USERS_COLLECTION:
            user_coll = MagicMock()

            def _document(_doc_id):
                user_doc = MagicMock()
                snap = MagicMock()
                snap.exists = user_exists
                snap.to_dict.return_value = {"created_by_load_test": user_flag}
                user_doc.get = AsyncMock(return_value=snap)

                def _subcollection(coll_name):
                    sub = MagicMock()
                    sub.stream.side_effect = lambda: _AsyncIter(
                        list(buckets_by_collection.get(coll_name, []))
                    )
                    return sub

                user_doc.collection.side_effect = _subcollection
                return user_doc

            user_coll.document.side_effect = _document
            return user_coll
        return MagicMock()

    db.collection.side_effect = _collection
    return db


@pytest.mark.asyncio
async def test_delete_suffixed_buckets_deletes_only_suffixed():
    from app.lib.firestore import (
        DISCARDED_POSTS_COLLECTION,
        LOAD_TEST_BUCKET_SUFFIX,
        SEEN_POSTS_COLLECTION,
    )

    lt = _bucket_doc("2026-06-02" + LOAD_TEST_BUCKET_SUFFIX)
    real = _bucket_doc("2026-06-02")
    db = _make_bucket_db(
        {SEEN_POSTS_COLLECTION: [lt, real], DISCARDED_POSTS_COLLECTION: []}
    )

    count = await cleanup._delete_suffixed_buckets(db, {"did:plc:a"}, execute=True)

    assert count == 1
    lt.reference.delete.assert_awaited_once()
    real.reference.delete.assert_not_called()


@pytest.mark.asyncio
async def test_delete_suffixed_buckets_skips_flagged_user():
    # A test-created user in the manifest is left to recursive_delete, so the
    # bucket cleaner must not touch (or count) its suffixed buckets.
    from app.lib.firestore import LOAD_TEST_BUCKET_SUFFIX, SEEN_POSTS_COLLECTION

    lt = _bucket_doc("2026-06-02" + LOAD_TEST_BUCKET_SUFFIX)
    db = _make_bucket_db({SEEN_POSTS_COLLECTION: [lt]}, user_flag=True)

    count = await cleanup._delete_suffixed_buckets(db, {"did:plc:a"}, execute=True)

    assert count == 0
    lt.reference.delete.assert_not_called()


@pytest.mark.asyncio
async def test_delete_suffixed_buckets_skips_absent_user():
    # Already recursive-deleted (or never created): nothing to do.
    from app.lib.firestore import LOAD_TEST_BUCKET_SUFFIX, SEEN_POSTS_COLLECTION

    lt = _bucket_doc("2026-06-02" + LOAD_TEST_BUCKET_SUFFIX)
    db = _make_bucket_db({SEEN_POSTS_COLLECTION: [lt]}, user_exists=False)

    count = await cleanup._delete_suffixed_buckets(db, {"did:plc:a"}, execute=True)

    assert count == 0
    lt.reference.delete.assert_not_called()


@pytest.mark.asyncio
async def test_run_refuses_without_users_or_force():
    import argparse

    args = argparse.Namespace(
        users=None, force=False, execute=False, skip_cache=False, environment="dev"
    )
    with pytest.raises(SystemExit):
        await cleanup.run(args)

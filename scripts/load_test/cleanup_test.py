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

    # load_test==True query. _delete_flagged pages it (limit().stream()) and
    # deletes each page via a batch, so model deletion: `remaining` shrinks as
    # batch.commit() applies the pending deletes, and the paged stream drains.
    remaining = list(flagged_stream or [])
    pending: list = []

    flagged_query = MagicMock()
    flagged_query.stream.side_effect = lambda: _AsyncIter(list(remaining))

    def _limit(n):
        limited = MagicMock()
        limited.stream.side_effect = lambda: _AsyncIter(remaining[:n])
        return limited

    flagged_query.limit.side_effect = _limit

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

    batch = MagicMock()
    batch.delete.side_effect = pending.append

    def _commit():
        dropped = {id(r) for r in pending}
        remaining[:] = [d for d in remaining if id(d.reference) not in dropped]
        pending.clear()

    batch.commit = AsyncMock(side_effect=_commit)
    db.batch.return_value = batch
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
async def test_delete_flagged_counts_and_batch_deletes():
    doc1, doc2 = MagicMock(), MagicMock()
    db = _make_db(user_stream=[], flagged_stream=[doc1, doc2])
    count = await cleanup._delete_flagged(db, "interactions", execute=True)
    assert count == 2
    # Deleted via a batched commit, not per-doc, and both refs enqueued.
    batch = db.batch.return_value
    assert batch.delete.call_count == 2
    batch.delete.assert_any_call(doc1.reference)
    batch.commit.assert_awaited()


@pytest.mark.asyncio
async def test_delete_flagged_paginates_until_empty():
    # More docs than a single batch: must loop, deleting every one, and terminate.
    docs = [MagicMock() for _ in range(cleanup.DELETE_BATCH + 7)]
    db = _make_db(user_stream=[], flagged_stream=docs)
    count = await cleanup._delete_flagged(db, "interactions", execute=True)
    assert count == cleanup.DELETE_BATCH + 7
    assert db.batch.return_value.delete.call_count == cleanup.DELETE_BATCH + 7


@pytest.mark.asyncio
async def test_delete_flagged_dry_run_does_not_delete():
    doc1 = MagicMock()
    db = _make_db(user_stream=[], flagged_stream=[doc1])
    count = await cleanup._delete_flagged(db, "feed_cache", execute=False)
    assert count == 1
    db.batch.assert_not_called()


@pytest.mark.asyncio
async def test_batch_delete_splits_on_transaction_too_big():
    # A backend that rejects any commit of more than 2 writes as "too big" — the
    # heavy-feed_cache case. _batch_delete must split down until each commit fits
    # and still delete every ref.
    from google.api_core.exceptions import InvalidArgument

    committed: list = []
    max_ok = 2

    def _make_batch():
        pending: list = []
        b = MagicMock()
        b.delete.side_effect = pending.append

        async def _commit():
            if len(pending) > max_ok:
                raise InvalidArgument("Transaction too big. Decrease transaction size.")
            committed.extend(pending)

        b.commit = AsyncMock(side_effect=_commit)
        return b

    db = MagicMock()
    db.batch.side_effect = _make_batch

    refs = [MagicMock() for _ in range(5)]
    advanced: list = []
    await cleanup._batch_delete(db, refs, advanced.append)

    assert {id(r) for r in committed} == {id(r) for r in refs}
    assert sum(advanced) == 5


@pytest.mark.asyncio
async def test_batch_delete_reraises_single_doc_failure():
    # If even a single-doc commit is rejected, it's a real error, not a size issue.
    from google.api_core.exceptions import InvalidArgument

    db = MagicMock()
    batch = MagicMock()
    batch.commit = AsyncMock(side_effect=InvalidArgument("nope"))
    db.batch.return_value = batch

    with pytest.raises(InvalidArgument):
        await cleanup._batch_delete(db, [MagicMock()], lambda n: None)


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

"""Tests for the followed-users cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest

from ..documents import FollowedUsersCacheDocument
from . import followed_users_cache as cache_module
from .bsky import FollowedUsersLookupError, FollowsFetch
from .followed_users_cache import (
    FOLLOWED_USERS_CACHE_COLLECTION,
    FollowedUsersCache,
    get_followed_dids_cached,
    set_followed_users_cache,
)
from .request_cache import request_cache_scope


# ---------------------------------------------------------------------------
# A small stateful Firestore fake.  The cache reads, writes and runs a
# transaction against the same document, so an in-memory store reads more
# clearly here than a stack of mocks.
# ---------------------------------------------------------------------------

class FakeSnapshot:
    def __init__(self, data: dict | None):
        self._data = data

    @property
    def exists(self) -> bool:
        return self._data is not None

    def to_dict(self) -> dict | None:
        return dict(self._data) if self._data is not None else None


class FakeDocRef:
    def __init__(self, store: dict, doc_id: str):
        self._store = store
        self._id = doc_id
        self.read_count = 0

    async def get(self, transaction=None) -> FakeSnapshot:
        self.read_count += 1
        if self._store.get("read_error"):
            raise RuntimeError("firestore unavailable")
        return FakeSnapshot(self._store["docs"].get(self._id))

    async def set(self, data: dict, merge: bool = False) -> None:
        if self._store.get("write_error"):
            raise RuntimeError("firestore unavailable")
        if merge and self._id in self._store["docs"]:
            self._store["docs"][self._id].update(data)
        else:
            self._store["docs"][self._id] = dict(data)


class FakeCollection:
    def __init__(self, store: dict):
        self._store = store

    def document(self, doc_id: str) -> FakeDocRef:
        return self._store["refs"].setdefault(doc_id, FakeDocRef(self._store, doc_id))


class FakeDb:
    def __init__(self):
        self.store: dict = {"docs": {}, "refs": {}}
        self.collections: list[str] = []

    def collection(self, name: str) -> FakeCollection:
        self.collections.append(name)
        return FakeCollection(self.store)

    def transaction(self):
        return object()


class FakeTransaction:
    """Applies writes straight through; the seam under test is the claim logic."""

    def __init__(self, store: dict):
        self._store = store

    def set(self, ref: FakeDocRef, data: dict, merge: bool = False) -> None:
        doc_id = ref._id
        if merge and doc_id in self._store["docs"]:
            self._store["docs"][doc_id].update(data)
        else:
            self._store["docs"][doc_id] = dict(data)


def make_cache(db: FakeDb) -> FollowedUsersCache:
    """A cache whose transaction seam runs the claim body inline."""
    cache = FollowedUsersCache(cast(Any, db))

    async def run_transaction(body):
        return await body(FakeTransaction(db.store))

    cache._run_transaction = run_transaction  # type: ignore[method-assign]
    return cache


def now() -> datetime:
    return datetime.now(timezone.utc)


def put(db: FakeDb, user_did: str, **fields) -> None:
    from .firestore import user_doc_id

    doc = FollowedUsersCacheDocument(**fields)
    db.store["docs"][user_doc_id(user_did)] = doc.model_dump()


def stored(db: FakeDb, user_did: str) -> FollowedUsersCacheDocument:
    from .firestore import user_doc_id

    return FollowedUsersCacheDocument.model_validate(db.store["docs"][user_doc_id(user_did)])


class RecordingOrigin:
    """Stands in for the Bluesky walk."""

    def __init__(self, *results):
        self.results = list(results)
        self.calls: list[tuple[str, float | None]] = []

    async def __call__(self, user_did, limit, timeout_seconds=None):
        self.calls.append((user_did, timeout_seconds))
        result = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def origin(monkeypatch):
    fetcher = RecordingOrigin(FollowsFetch(dids=["did:plc:a"], complete=True))
    monkeypatch.setattr(cache_module, "fetch_followed_user_dids", fetcher)
    return fetcher


@pytest.fixture(autouse=True)
def _clear_process_cache():
    set_followed_users_cache(None)
    yield
    set_followed_users_cache(None)


USER = "did:plc:user1"


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

class TestReadPath:
    @pytest.mark.asyncio
    async def test_fresh_complete_entry_is_served_without_touching_bluesky(self, origin):
        db = FakeDb()
        put(db, USER, follows=["did:plc:a", "did:plc:b"], complete=True, generated_at=now())
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:a", "did:plc:b"]
        assert origin.calls == []
        assert db.collections == [FOLLOWED_USERS_CACHE_COLLECTION]

    @pytest.mark.asyncio
    async def test_pending_adds_are_merged_in_read_order_without_duplicates(self, origin):
        db = FakeDb()
        put(
            db,
            USER,
            follows=["did:plc:a", "did:plc:b"],
            pending_adds=["did:plc:c", "did:plc:a"],
            complete=True,
            generated_at=now(),
        )
        cache = make_cache(db)

        # A jetstream follow shows up immediately, before any refresh runs.
        assert await cache.get_followed_dids(USER) == [
            "did:plc:a",
            "did:plc:b",
            "did:plc:c",
        ]

    @pytest.mark.asyncio
    async def test_stale_entry_is_served_immediately_and_refreshed_in_background(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=["did:plc:stale"], complete=True, generated_at=old)
        cache = make_cache(db)

        # The caller is never made to wait on the refresh.
        assert await cache.get_followed_dids(USER) == ["did:plc:stale"]
        await cache.drain()

        assert origin.calls == [(USER, cache_module.refresh_timeout_seconds())]
        assert stored(db, USER).follows == ["did:plc:a"]

    @pytest.mark.asyncio
    async def test_incomplete_entry_refreshes_however_young_it_is(self, origin):
        db = FakeDb()
        put(db, USER, follows=["did:plc:partial"], complete=False, generated_at=now())
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:partial"]
        await cache.drain()

        assert len(origin.calls) == 1
        assert stored(db, USER).complete is True

    @pytest.mark.asyncio
    async def test_invalidated_entry_refreshes(self, origin):
        db = FakeDb()
        put(
            db,
            USER,
            follows=["did:plc:x"],
            complete=True,
            generated_at=now(),
            invalidated_at=now(),
        )
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:x"]
        await cache.drain()

        assert len(origin.calls) == 1
        assert stored(db, USER).invalidated_at is None

    @pytest.mark.asyncio
    async def test_overfull_pending_adds_forces_a_refresh(self, origin):
        db = FakeDb()
        pending = [f"did:plc:p{i}" for i in range(cache_module.MAX_PENDING_ADDS + 1)]
        put(db, USER, follows=[], complete=True, generated_at=now(), pending_adds=pending)
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert len(origin.calls) == 1
        assert stored(db, USER).pending_adds == []


class TestColdStart:
    @pytest.mark.asyncio
    async def test_missing_document_fetches_on_the_request_path_and_stores(self, origin):
        db = FakeDb()
        cache = make_cache(db)

        # No entry: behave exactly as the pre-cache code did, under the tight
        # request-path budget rather than the refresh budget.
        assert await cache.get_followed_dids(USER) == ["did:plc:a"]
        assert origin.calls == [(USER, None)]

        await cache.drain()
        entry = stored(db, USER)
        assert entry.follows == ["did:plc:a"]
        assert entry.complete is True
        assert entry.expires_at is not None

    @pytest.mark.asyncio
    async def test_partial_cold_fetch_is_stored_as_incomplete(self, origin):
        origin.results = [FollowsFetch(dids=["did:plc:a"], complete=False)]
        db = FakeDb()
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:a"]
        await cache.drain()

        # Serving it is fine; trusting it is not.
        assert stored(db, USER).complete is False

    @pytest.mark.asyncio
    async def test_lease_only_document_is_treated_as_a_miss(self, origin):
        # A refresh can stamp the lease onto a document that holds no follows
        # yet (TTL reaped it, or a refresh died before writing). Validating
        # that as an entry would serve an empty follow list — indistinguishable
        # from "follows nobody" — and silently drop the generator's allocation.
        db = FakeDb()
        db.store["docs"][cache_module.user_doc_id(USER)] = {
            "refresh_started_at": now(),
        }
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:a"]
        assert origin.calls == [(USER, None)]

    @pytest.mark.asyncio
    async def test_raises_when_nothing_cached_and_bluesky_fails(self, origin):
        origin.results = [FollowedUsersLookupError("boom")]
        db = FakeDb()
        cache = make_cache(db)

        with pytest.raises(FollowedUsersLookupError):
            await cache.get_followed_dids(USER)

    @pytest.mark.asyncio
    async def test_firestore_read_failure_degrades_to_a_live_fetch(self, origin):
        db = FakeDb()
        db.store["read_error"] = True
        cache = make_cache(db)

        # A cache that is down must not break feed serving.
        assert await cache.get_followed_dids(USER) == ["did:plc:a"]


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

class TestRefresh:
    @pytest.mark.asyncio
    async def test_partial_refresh_never_shrinks_a_complete_entry(self, origin):
        origin.results = [FollowsFetch(dids=["did:plc:a"], complete=False)]
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        full = [f"did:plc:f{i}" for i in range(50)]
        put(db, USER, follows=full, complete=True, generated_at=old)
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        entry = stored(db, USER)
        assert entry.follows == full
        assert entry.complete is True

    @pytest.mark.asyncio
    async def test_partial_refresh_replaces_an_incomplete_entry(self, origin):
        origin.results = [FollowsFetch(dids=["did:plc:a", "did:plc:b"], complete=False)]
        db = FakeDb()
        put(db, USER, follows=["did:plc:old"], complete=False, generated_at=now())
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert stored(db, USER).follows == ["did:plc:a", "did:plc:b"]

    @pytest.mark.asyncio
    async def test_failed_refresh_clears_the_lease_and_stamps_the_cooldown(self, origin):
        origin.results = [FollowedUsersLookupError("boom")]
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=["did:plc:x"], complete=True, generated_at=old)
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        entry = stored(db, USER)
        # The lease is for excluding concurrent refreshes, not a penalty box:
        # the next request must be free to retry at once.
        assert entry.refresh_started_at is None
        assert entry.refresh_failed_at is not None
        assert entry.follows == ["did:plc:x"]

    @pytest.mark.asyncio
    async def test_recent_failure_suppresses_another_refresh(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(
            db,
            USER,
            follows=["did:plc:x"],
            complete=True,
            generated_at=old,
            refresh_failed_at=now(),
        )
        cache = make_cache(db)

        assert await cache.get_followed_dids(USER) == ["did:plc:x"]
        await cache.drain()

        assert origin.calls == []

    @pytest.mark.asyncio
    async def test_expired_cooldown_allows_a_retry(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(
            db,
            USER,
            follows=["did:plc:x"],
            complete=True,
            generated_at=old,
            refresh_failed_at=now()
            - timedelta(seconds=cache_module.retry_cooldown_seconds() + 5),
        )
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert len(origin.calls) == 1


class TestLease:
    @pytest.mark.asyncio
    async def test_unexpired_lease_from_another_instance_blocks_the_claim(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(
            db,
            USER,
            follows=["did:plc:x"],
            complete=True,
            generated_at=old,
            refresh_started_at=now(),
        )
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert origin.calls == []

    @pytest.mark.asyncio
    async def test_expired_lease_is_reclaimed(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(
            db,
            USER,
            follows=["did:plc:x"],
            complete=True,
            generated_at=old,
            refresh_started_at=now() - timedelta(seconds=cache_module.lease_seconds() + 5),
        )
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert len(origin.calls) == 1

    @pytest.mark.asyncio
    async def test_entry_refreshed_by_another_instance_is_not_refreshed_again(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=["did:plc:x"], complete=True, generated_at=old)
        cache = make_cache(db)

        async def refreshed_meanwhile(body):
            put(db, USER, follows=["did:plc:new"], complete=True, generated_at=now())
            return await body(FakeTransaction(db.store))

        cache._run_transaction = refreshed_meanwhile  # type: ignore[method-assign]

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert origin.calls == []


# ---------------------------------------------------------------------------
# Module-level accessor used by the candidate generators
# ---------------------------------------------------------------------------

class TestGetFollowedDidsCached:
    @pytest.mark.asyncio
    async def test_falls_back_to_a_direct_fetch_when_no_cache_is_installed(self, origin):
        # Unit tests and scripts run without a Firestore client.
        assert await get_followed_dids_cached(USER) == ["did:plc:a"]
        assert origin.calls == [(USER, None)]

    @pytest.mark.asyncio
    async def test_uses_the_installed_cache(self, origin):
        db = FakeDb()
        put(db, USER, follows=["did:plc:cached"], complete=True, generated_at=now())
        set_followed_users_cache(make_cache(db))

        assert await get_followed_dids_cached(USER) == ["did:plc:cached"]
        assert origin.calls == []

    @pytest.mark.asyncio
    async def test_both_generators_in_one_request_share_a_single_lookup(self, origin):
        db = FakeDb()
        put(db, USER, follows=["did:plc:cached"], complete=True, generated_at=now())
        set_followed_users_cache(make_cache(db))

        async with request_cache_scope():
            first = await get_followed_dids_cached(USER)
            second = await get_followed_dids_cached(USER)

        assert first == second == ["did:plc:cached"]
        # followed_users and network_likes must not each pay for a read.
        from .firestore import user_doc_id

        assert db.store["refs"][user_doc_id(USER)].read_count == 1

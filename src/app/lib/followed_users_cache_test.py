"""Tests for the followed-users cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
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


class FakeAggregation:
    """Stands in for a Firestore count() aggregation query."""

    def __init__(self, store: dict, predicate=None):
        self._store = store
        self._predicate = predicate

    async def get(self):
        docs = self._store["docs"].values()
        if self._predicate is not None:
            docs = [d for d in docs if self._predicate(d)]
        return [[SimpleNamespace(value=len(list(docs)))]]


class FakeQuery:
    def __init__(self, store: dict, predicate):
        self._store = store
        self._predicate = predicate

    def count(self) -> FakeAggregation:
        return FakeAggregation(self._store, self._predicate)


def _predicate_for(field_filter):
    """Evaluate a real FieldFilter against a stored document dict."""
    field, op, value = field_filter.field_path, field_filter.op_string, field_filter.value

    def check(doc: dict) -> bool:
        actual = doc.get(field)
        if op == "==":
            return actual == value
        if op == "<":
            return actual is not None and actual < value
        raise AssertionError(f"unsupported op in fake: {op}")

    return check


class FakeCollection:
    def __init__(self, store: dict):
        self._store = store

    def document(self, doc_id: str) -> FakeDocRef:
        return self._store["refs"].setdefault(doc_id, FakeDocRef(self._store, doc_id))

    def count(self) -> FakeAggregation:
        return FakeAggregation(self._store)

    def where(self, filter=None) -> FakeQuery:  # noqa: A002 - matches the SDK's kwarg
        return FakeQuery(self._store, _predicate_for(filter))


class FakeDb:
    def __init__(self):
        self.store: dict = {"docs": {}, "refs": {}}
        self.collections: list[str] = []

    def collection(self, name: str) -> FakeCollection:
        self.collections.append(name)
        return FakeCollection(self.store)

    def transaction(self):
        return object()


def make_cache(db: FakeDb) -> FollowedUsersCache:
    return FollowedUsersCache(cast(Any, db))


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


class RecordingCollector:
    def __init__(self):
        self.records: list[tuple[str, float, dict]] = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))

    def values(self, name: str) -> list[float]:
        return [v for n, v, _ in self.records if n == name]

    def labelled(self, name: str) -> dict[str, float]:
        """Values for *name* keyed by their ``state`` label."""
        return {a["state"]: v for n, v, a in self.records if n == name}


@pytest.fixture
def metrics(monkeypatch):
    collector = RecordingCollector()
    monkeypatch.setattr(cache_module, "get_metric_collector", lambda: collector)
    return collector


@pytest.fixture(autouse=True)
def _reset_sweep_clock():
    # The sweep is rate-limited per process; tests must not inherit each
    # other's last-sweep timestamp.
    cache_module.reset_sweep_clock()
    yield
    cache_module.reset_sweep_clock()


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
    async def test_stale_entry_is_served_immediately_without_touching_bluesky(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=["did:plc:stale"], complete=True, generated_at=old)
        cache = make_cache(db)

        # Refreshing stale entries is now owned by the ingex backfill job, not
        # the request path (api#453) — the caller gets the stale value as-is.
        assert await cache.get_followed_dids(USER) == ["did:plc:stale"]
        await cache.drain()

        assert origin.calls == []
        assert stored(db, USER).follows == ["did:plc:stale"]


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
# Stale entries serve as-is; refreshing them moved off the request path
# (api#453) to a recurring ingex backfill job.
# ---------------------------------------------------------------------------

class TestStaleEntriesServeWithoutRefreshing:
    @pytest.mark.asyncio
    async def test_stale_entry_is_served_and_no_task_is_spawned(self, origin):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=["did:plc:a"], complete=True, generated_at=old)
        cache = make_cache(db)

        result = await cache.get_followed_dids(USER)
        assert result == ["did:plc:a"]
        await cache.drain()

        # The stale entry must be served as-is — nothing re-wrote the document.
        assert origin.calls == []
        assert stored(db, USER).generated_at == old

    @pytest.mark.asyncio
    async def test_incomplete_entry_is_served_and_no_task_is_spawned(self, origin):
        db = FakeDb()
        put(db, USER, follows=["did:plc:a"], complete=False, generated_at=now())
        cache = make_cache(db)

        result = await cache.get_followed_dids(USER)
        assert result == ["did:plc:a"]
        await cache.drain()

        assert origin.calls == []
        assert stored(db, USER).complete is False  # untouched, not silently "fixed"

    @pytest.mark.asyncio
    async def test_invalidated_entry_is_served_and_no_task_is_spawned(self, origin):
        db = FakeDb()
        put(
            db,
            USER,
            follows=["did:plc:a"],
            complete=True,
            generated_at=now(),
            invalidated_at=now(),
        )
        cache = make_cache(db)

        result = await cache.get_followed_dids(USER)
        assert result == ["did:plc:a"]
        await cache.drain()

        assert origin.calls == []
        assert stored(db, USER).invalidated_at is not None  # left for the ingex job to clear

    @pytest.mark.asyncio
    async def test_pending_overflow_entry_is_served_and_no_task_is_spawned(self, origin):
        db = FakeDb()
        pending = [f"did:plc:p{i}" for i in range(cache_module.MAX_PENDING_ADDS + 1)]
        put(db, USER, follows=[], complete=True, generated_at=now(), pending_adds=pending)
        cache = make_cache(db)

        await cache.get_followed_dids(USER)
        await cache.drain()

        assert origin.calls == []
        assert stored(db, USER).pending_adds == pending

    @pytest.mark.asyncio
    async def test_stale_lookup_records_the_staleness_reason_for_observability(
        self, origin, metrics
    ):
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, USER, follows=[], complete=True, generated_at=old)
        cache = make_cache(db)

        await cache.get_followed_dids(USER)

        lookups = [(n, a["outcome"]) for n, v, a in metrics.records if n == "follows_cache.lookup_count"]
        assert lookups == [("follows_cache.lookup_count", "stale")]


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


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------

class TestTelemetry:
    @pytest.mark.asyncio
    async def test_age_is_recorded_on_a_fresh_hit_too(self, origin, metrics):
        # Recording age only when an entry is already stale makes the series
        # structurally alarming: it would contain nothing but overdue entries,
        # so the freshness of a healthy population is invisible.
        db = FakeDb()
        put(
            db,
            USER,
            follows=["did:plc:a"],
            complete=True,
            generated_at=now() - timedelta(seconds=60),
        )
        cache = make_cache(db)

        await cache.get_followed_dids(USER)

        ages = metrics.values("follows_cache.age_seconds")
        assert len(ages) == 1
        assert 55 <= ages[0] <= 120

    @pytest.mark.asyncio
    async def test_population_sweep_counts_users_by_health(self, origin, metrics):
        # Lookup metrics are request-weighted, so one heavy user stuck
        # incomplete looks like many users briefly incomplete. This is the
        # per-user view.
        db = FakeDb()
        old = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
        put(db, "did:plc:healthy1", follows=["a"], complete=True, generated_at=now())
        put(db, "did:plc:healthy2", follows=["a"], complete=True, generated_at=now())
        put(db, "did:plc:broken", follows=["a"], complete=False, generated_at=now())
        put(db, "did:plc:overdue", follows=["a"], complete=True, generated_at=old)
        cache = make_cache(db)

        await cache.get_followed_dids("did:plc:healthy1")
        await cache.drain()

        assert metrics.labelled("follows_cache.users_rate") == {
            "total": 4,
            "incomplete": 1,
            "stale": 1,
        }

    @pytest.mark.asyncio
    async def test_sweep_is_deferred_not_awaited_by_the_caller(self, origin, metrics):
        db = FakeDb()
        put(db, USER, follows=["did:plc:a"], complete=True, generated_at=now())
        cache = make_cache(db)

        await cache.get_followed_dids(USER)

        # Nothing counted yet: the sweep must run after the response, not
        # inside the request that triggered it.
        assert metrics.labelled("follows_cache.users_rate") == {}
        await cache.drain()
        assert metrics.labelled("follows_cache.users_rate") != {}

    @pytest.mark.asyncio
    async def test_sweep_is_rate_limited_across_lookups(self, origin, metrics):
        db = FakeDb()
        put(db, USER, follows=["did:plc:a"], complete=True, generated_at=now())
        cache = make_cache(db)

        for _ in range(5):
            await cache.get_followed_dids(USER)
            await cache.drain()

        # One aggregation pass per interval, not one per feed request.
        assert metrics.values("follows_cache.users_rate").count(1.0) <= 3
        assert len([v for n, v, a in metrics.records
                    if n == "follows_cache.users_rate" and a["state"] == "total"]) == 1

    @pytest.mark.asyncio
    async def test_sweep_failure_never_breaks_a_lookup(self, origin, metrics):
        db = FakeDb()
        put(db, USER, follows=["did:plc:a"], complete=True, generated_at=now())
        cache = make_cache(db)

        async def boom():
            raise RuntimeError("aggregation unavailable")

        monkey = FakeCollection(db.store)
        monkey.count = lambda: SimpleNamespace(get=boom)  # type: ignore[assignment]
        db.collection = lambda name: monkey  # type: ignore[assignment]

        assert await cache.get_followed_dids(USER) == ["did:plc:a"]
        await cache.drain()

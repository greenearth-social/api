"""Tests for the followed-users refresh job."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from ..documents import FollowedUsersCacheDocument
from ..lib import followed_users_cache as cache_module
from ..lib.bsky import FollowsFetch
from ..lib.firestore import USERS_COLLECTION, user_doc_id
from ..lib.followed_users_cache import FOLLOWED_USERS_CACHE_COLLECTION, FollowedUsersCache
from . import refresh_followed_users as job


class FakeSnapshot:
    def __init__(self, doc_id: str, data: dict | None):
        self.id = doc_id
        self._data = data

    @property
    def exists(self) -> bool:
        return self._data is not None

    def to_dict(self) -> dict | None:
        return dict(self._data) if self._data is not None else None


class FakeDocRef:
    def __init__(self, db: FakeDb, collection: str, doc_id: str):
        self._db = db
        self._collection = collection
        self.id = doc_id

    async def get(self, transaction=None) -> FakeSnapshot:
        return FakeSnapshot(self.id, self._db.data[self._collection].get(self.id))

    async def set(self, data: dict, merge: bool = False) -> None:
        docs = self._db.data[self._collection]
        if merge and self.id in docs:
            docs[self.id].update(data)
        else:
            docs[self.id] = dict(data)


class FakeQuery:
    def __init__(self, db: FakeDb, collection: str, field_filter=None, fields=None):
        self._db = db
        self._collection = collection
        self._filter = field_filter
        self._fields = fields

    def where(self, filter=None) -> FakeQuery:  # noqa: A002 - matches the SDK's kwarg
        return FakeQuery(self._db, self._collection, filter, self._fields)

    def select(self, fields) -> FakeQuery:
        return FakeQuery(self._db, self._collection, self._filter, fields)

    async def stream(self):
        f = self._filter
        for doc_id, data in list(self._db.data[self._collection].items()):
            if f is not None:
                assert f.op_string == "=="
                if data.get(f.field_path) != f.value:
                    continue
            projected = {k: data[k] for k in (self._fields or data) if k in data}
            yield FakeSnapshot(doc_id, projected)


class FakeCollection(FakeQuery):
    def document(self, doc_id: str) -> FakeDocRef:
        return FakeDocRef(self._db, self._collection, doc_id)


class FakeDb:
    def __init__(self):
        self.data: dict[str, dict[str, dict]] = {
            USERS_COLLECTION: {},
            FOLLOWED_USERS_CACHE_COLLECTION: {},
        }

    def collection(self, name: str) -> FakeCollection:
        return FakeCollection(self, name)

    async def get_all(self, refs):
        for ref in refs:
            yield await ref.get()

    def transaction(self):
        return object()


def make_cache(db: FakeDb) -> FollowedUsersCache:
    cache = FollowedUsersCache(cast(Any, db))

    async def run_transaction(body):
        class Transaction:
            def set(self, ref, data, merge=False):
                docs = db.data[FOLLOWED_USERS_CACHE_COLLECTION]
                if merge and ref.id in docs:
                    docs[ref.id].update(data)
                else:
                    docs[ref.id] = dict(data)

        return await body(Transaction())

    cache._run_transaction = run_transaction  # type: ignore[method-assign]
    return cache


def now() -> datetime:
    return datetime.now(UTC)


def add_user(db: FakeDb, did: str, *, flagged: bool) -> None:
    db.data[USERS_COLLECTION][user_doc_id(did)] = {
        "user_did": did,
        "precompute_artifacts": flagged,
        "username": "irrelevant",
    }


def add_entry(db: FakeDb, did: str, **fields) -> None:
    doc = FollowedUsersCacheDocument(**fields)
    db.data[FOLLOWED_USERS_CACHE_COLLECTION][user_doc_id(did)] = doc.model_dump()


def entry(db: FakeDb, did: str) -> FollowedUsersCacheDocument:
    return FollowedUsersCacheDocument.model_validate(
        db.data[FOLLOWED_USERS_CACHE_COLLECTION][user_doc_id(did)]
    )


class RecordingWalk:
    def __init__(self, result: FollowsFetch | Exception):
        self.result = result
        self.calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def __call__(self, user_did, limit, timeout_seconds=None):
        self.calls.append(user_did)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.in_flight -= 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.fixture
def walk(monkeypatch):
    records = RecordingWalk(FollowsFetch(dids=["did:plc:new"], complete=True, rkeys=["rn"]))
    monkeypatch.setattr(cache_module, "fetch_visible_follow_records", records)

    async def no_getfollows(*args, **kwargs):
        raise AssertionError("the job walks follow records, never getFollows")

    monkeypatch.setattr(cache_module, "fetch_followed_user_dids", no_getfollows)
    return records


FRESH = {"follows": ["did:plc:old"], "follow_rkeys": ["ro"], "complete": True}


@pytest.mark.asyncio
async def test_refreshes_only_flagged_users_that_need_it(walk):
    db = FakeDb()
    stale = now() - timedelta(seconds=cache_module.ttl_seconds() + 60)
    add_user(db, "did:plc:fresh", flagged=True)
    add_entry(db, "did:plc:fresh", generated_at=now(), **FRESH)
    add_user(db, "did:plc:stale", flagged=True)
    add_entry(db, "did:plc:stale", generated_at=stale, **FRESH)
    add_user(db, "did:plc:missing", flagged=True)
    add_user(db, "did:plc:nokeys", flagged=True)
    add_entry(db, "did:plc:nokeys", follows=["did:plc:old"], complete=True, generated_at=now())
    add_user(db, "did:plc:explore", flagged=False)
    add_entry(db, "did:plc:explore", generated_at=stale, **FRESH)

    counts = await job.run(cast(Any, db), make_cache(db))

    assert sorted(walk.calls) == ["did:plc:missing", "did:plc:nokeys", "did:plc:stale"]
    assert counts["flagged"] == 4
    assert counts["reason:stale"] == counts["reason:missing"] == counts["reason:no_rkeys"] == 1
    assert counts["outcome:success"] == 3
    for did in ("did:plc:missing", "did:plc:nokeys", "did:plc:stale"):
        assert entry(db, did).follows == ["did:plc:new"]
        assert entry(db, did).follow_rkeys == ["rn"]
    assert entry(db, "did:plc:fresh").follows == ["did:plc:old"]
    # Explore users are left to their own request-path refresh.
    assert entry(db, "did:plc:explore").follows == ["did:plc:old"]


@pytest.mark.asyncio
async def test_dry_run_counts_without_walking_or_writing(walk):
    db = FakeDb()
    add_user(db, "did:plc:missing", flagged=True)

    counts = await job.run(cast(Any, db), make_cache(db), dry_run=True)

    assert counts["reason:missing"] == 1
    assert walk.calls == []
    assert db.data[FOLLOWED_USERS_CACHE_COLLECTION] == {}


@pytest.mark.asyncio
async def test_concurrency_is_bounded(walk):
    db = FakeDb()
    for i in range(20):
        add_user(db, f"did:plc:u{i}", flagged=True)

    await job.run(cast(Any, db), make_cache(db), concurrency=3)

    assert len(walk.calls) == 20
    assert walk.max_in_flight <= 3


@pytest.mark.asyncio
async def test_a_failed_walk_is_counted_not_fatal(walk):
    from ..lib.bsky import FollowedUsersLookupError

    walk.result = FollowedUsersLookupError("pds down")
    db = FakeDb()
    add_user(db, "did:plc:u", flagged=True)

    counts = await job.run(cast(Any, db), make_cache(db))

    assert counts["outcome:failed"] == 1


@pytest.mark.asyncio
async def test_flagged_user_without_user_did_is_skipped(walk):
    db = FakeDb()
    db.data[USERS_COLLECTION]["legacy"] = {"precompute_artifacts": True}

    counts = await job.run(cast(Any, db), make_cache(db))

    assert counts["flagged"] == 0
    assert walk.calls == []


@pytest.mark.asyncio
async def test_reports_population_gauges(walk, monkeypatch):
    records: list[tuple[str, float, dict]] = []
    collector = SimpleNamespace(record=lambda n, v, **a: records.append((n, v, a)))
    monkeypatch.setattr(job, "get_metric_collector", lambda: collector)
    db = FakeDb()
    add_user(db, "did:plc:u", flagged=True)

    await job.run(cast(Any, db), make_cache(db))

    gauges = {a["state"]: v for n, v, a in records if n == "follows_refresh_job.users_rate"}
    assert gauges == {"flagged": 1, "needed_refresh": 1, "refreshed": 1, "failed": 0}


def test_requires_firestore_config_outside_the_emulator(monkeypatch):
    for name in ("GE_FIRESTORE_EMULATOR_HOST", "GE_FIRESTORE_PROJECT", "GE_FIRESTORE_DATABASE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GE_FIRESTORE_PROJECT", "p")

    with pytest.raises(SystemExit, match="GE_FIRESTORE_DATABASE"):
        job._require_config()


def test_main_keeps_httpx_request_logs_out_of_job_logs(monkeypatch):
    import logging
    import sys

    httpx_logger = logging.getLogger("httpx")
    monkeypatch.setattr(httpx_logger, "level", logging.NOTSET)

    def run(coro):
        coro.close()
        return 0

    monkeypatch.setattr(job.asyncio, "run", run)
    monkeypatch.setattr(sys, "argv", ["refresh_followed_users", "--dry-run"])

    with pytest.raises(SystemExit) as exit_info:
        job.main()

    assert exit_info.value.code == 0
    assert httpx_logger.level == logging.WARNING

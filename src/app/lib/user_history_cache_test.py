"""Tests for the shared Firestore-backed user-history cache."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from . import user_history_cache as user_history_module
from .embeddings import (
    MINILM_L12_EMBEDDING_DIM,
    MINILM_L12_EMBEDDING_KEY,
    encode_float32_b64,
)
from .request_cache import request_cache_scope
from .user_history_cache import (
    USER_HISTORY_CACHE_COLLECTION,
    USER_HISTORY_CACHE_LEASE_SEC,
    USER_HISTORY_CACHE_MAX_AGE_SEC,
    USER_HISTORY_CACHE_RETRY_COOLDOWN_SEC,
    USER_HISTORY_CACHE_TTL_SEC,
    USER_HISTORY_CACHE_VERSION,
    USER_HISTORY_LIMIT,
    USER_HISTORY_REFRESH_TIMEOUT_SEC,
    FirestoreUserHistoryCache,
    UserHistory,
    UserHistoryCache,
    UserHistoryCacheEntry,
    UserHistoryItem,
    _user_history_cache_key,
    fetch_user_history_features,
    max_age_seconds,
    set_user_history_cache,
)

VALID_EMBEDDING = [1.0, 2.0, *([0.0] * (MINILM_L12_EMBEDDING_DIM - 2))]


@pytest.fixture(autouse=True)
def clear_shared_cache():
    set_user_history_cache(None)
    yield
    set_user_history_cache(None)


def test_cache_policy_constants():
    assert USER_HISTORY_CACHE_TTL_SEC == 600
    assert USER_HISTORY_CACHE_MAX_AGE_SEC == 1_800
    assert USER_HISTORY_CACHE_LEASE_SEC == 30
    assert USER_HISTORY_REFRESH_TIMEOUT_SEC == 10
    assert USER_HISTORY_CACHE_RETRY_COOLDOWN_SEC == 60
    assert max_age_seconds() == USER_HISTORY_CACHE_MAX_AGE_SEC


def test_max_age_is_never_shorter_than_fresh_ttl(monkeypatch):
    monkeypatch.setattr(user_history_module, "USER_HISTORY_CACHE_TTL_SEC", 900)
    monkeypatch.setattr(user_history_module, "USER_HISTORY_CACHE_MAX_AGE_SEC", 600)

    assert max_age_seconds() == 900


def _mock_firestore_client() -> tuple[MagicMock, MagicMock, AsyncMock]:
    db = MagicMock()
    doc_ref = AsyncMock()
    collection_ref = MagicMock()
    collection_ref.document.return_value = doc_ref
    db.collection.return_value = collection_ref
    return db, collection_ref, doc_ref


def _cached_document(
    *,
    user_did: str = "did:plc:user1",
    fetched_at: datetime | None = None,
    expires_at: datetime | None = None,
    refresh_started_at: datetime | None = None,
    refresh_failed_at: datetime | None = None,
) -> dict:
    fetched_at = fetched_at or datetime.now(UTC)
    return {
        "schema_version": USER_HISTORY_CACHE_VERSION,
        "user_did": user_did,
        "history_limit": USER_HISTORY_LIMIT,
        "embedding_key": MINILM_L12_EMBEDDING_KEY,
        "fetched_at": fetched_at,
        "expires_at": expires_at or fetched_at + timedelta(seconds=USER_HISTORY_CACHE_MAX_AGE_SEC),
        "refresh_started_at": refresh_started_at,
        "refresh_failed_at": refresh_failed_at,
        "items": [
            {
                "at_uri": "at://liked/a",
                "liked_at": "2026-01-01T00:00:00+00:00",
                "embedding_b64": encode_float32_b64(VALID_EMBEDDING),
                "author_did": "did:plc:author",
                "like_count": 7,
            },
            {
                "at_uri": "at://liked/missing",
                "liked_at": "2026-01-02T00:00:00+00:00",
                "embedding_b64": None,
                "author_did": "",
                "like_count": 0,
            },
        ],
    }


@pytest.mark.asyncio
async def test_firestore_cache_hit_decodes_history():
    db, collection_ref, doc_ref = _mock_firestore_client()
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = _cached_document()
    doc_ref.get.return_value = snapshot

    entry = await FirestoreUserHistoryCache(db).retrieve("did:plc:user1")

    db.collection.assert_called_once_with(USER_HISTORY_CACHE_COLLECTION)
    collection_ref.document.assert_called_once_with(_user_history_cache_key("did:plc:user1"))
    assert entry is not None
    assert entry.history == UserHistory(
        items=[
            UserHistoryItem(
                at_uri="at://liked/a",
                liked_at="2026-01-01T00:00:00+00:00",
                embedding=VALID_EMBEDDING,
                author_did="did:plc:author",
                like_count=7,
            ),
            UserHistoryItem(
                at_uri="at://liked/missing",
                liked_at="2026-01-02T00:00:00+00:00",
                embedding=None,
            ),
        ]
    )
    assert entry.age_seconds() < 1


@pytest.mark.asyncio
async def test_firestore_cache_returns_stale_entry_until_hard_expiry():
    db, _collection_ref, doc_ref = _mock_firestore_client()
    fetched_at = datetime.now(UTC) - timedelta(seconds=USER_HISTORY_CACHE_TTL_SEC + 1)
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = _cached_document(fetched_at=fetched_at)
    doc_ref.get.return_value = snapshot

    entry = await FirestoreUserHistoryCache(db).retrieve("did:plc:user1")

    assert entry is not None
    assert USER_HISTORY_CACHE_TTL_SEC < entry.age_seconds()
    assert entry.age_seconds() < USER_HISTORY_CACHE_MAX_AGE_SEC


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda data: data.update(schema_version=USER_HISTORY_CACHE_VERSION + 1),
        lambda data: data.update(user_did="did:plc:someone-else"),
        lambda data: data.update(history_limit=USER_HISTORY_LIMIT + 1),
        lambda data: data.update(embedding_key="different-embedding"),
        lambda data: data.update(expires_at=datetime.now(UTC) - timedelta(seconds=1)),
        lambda data: data.pop("items"),
        lambda data: data["items"][0].update(embedding_b64="not-base64!"),
        lambda data: data["items"][0].update(embedding_b64="!!!!"),
        lambda data: data["items"][0].update(
            embedding_b64=(
                data["items"][0]["embedding_b64"][:8] + "!" + data["items"][0]["embedding_b64"][8:]
            )
        ),
        lambda data: data["items"][0].update(embedding_b64=encode_float32_b64([1.0, 2.0])),
        lambda data: data["items"][0].update(
            embedding_b64=encode_float32_b64(
                [float("nan"), *([0.0] * (MINILM_L12_EMBEDDING_DIM - 1))]
            )
        ),
        lambda data: data["items"][0].update(liked_at="2026-01-01T00:00:00"),
        lambda data: data.update(items=[data["items"][0]] * (USER_HISTORY_LIMIT + 1)),
    ],
)
async def test_firestore_cache_rejects_incompatible_expired_or_malformed_documents(
    mutate,
):
    db, _collection_ref, doc_ref = _mock_firestore_client()
    data = _cached_document()
    mutate(data)
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = data
    doc_ref.get.return_value = snapshot

    assert await FirestoreUserHistoryCache(db).retrieve("did:plc:user1") is None


@pytest.mark.asyncio
async def test_firestore_cache_miss_for_missing_document():
    db, _collection_ref, doc_ref = _mock_firestore_client()
    doc_ref.get.return_value = MagicMock(exists=False)

    assert await FirestoreUserHistoryCache(db).retrieve("did:plc:user1") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("release_sha", [None, "abc1234"])
async def test_firestore_cache_stores_float32_embeddings_with_exact_max_age(
    monkeypatch,
    release_sha,
):
    db, _collection_ref, doc_ref = _mock_firestore_client()
    before = datetime.now(UTC)
    monkeypatch.setattr(user_history_module, "api_release_sha", lambda: release_sha)

    await FirestoreUserHistoryCache(db).store(
        "did:plc:user1",
        UserHistory(
            items=[
                UserHistoryItem(
                    at_uri="at://liked/a",
                    liked_at="2026-01-01T00:00:00+00:00",
                    embedding=VALID_EMBEDDING,
                    author_did="did:plc:author",
                    like_count=7,
                )
            ]
        ),
    )
    after = datetime.now(UTC)

    stored = doc_ref.set.await_args.args[0]
    assert stored["schema_version"] == USER_HISTORY_CACHE_VERSION
    assert stored["user_did"] == "did:plc:user1"
    assert stored["items"][0]["embedding_b64"] == encode_float32_b64(VALID_EMBEDDING)
    assert before <= stored["fetched_at"] <= after
    assert stored["expires_at"] - stored["fetched_at"] == timedelta(
        seconds=USER_HISTORY_CACHE_MAX_AGE_SEC
    )
    assert stored["refresh_started_at"] is None
    assert stored["refresh_failed_at"] is None
    assert stored["api_release_sha"] == release_sha


def _install_direct_transaction(cache, transaction: MagicMock, monkeypatch) -> None:
    async def run(body):
        return await body(transaction)

    monkeypatch.setattr(cache, "_run_transaction", run)


@pytest.mark.asyncio
async def test_firestore_cache_claims_stale_refresh_lease_transactionally(monkeypatch):
    db, _collection_ref, doc_ref = _mock_firestore_client()
    now = datetime.now(UTC)
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = _cached_document(
        fetched_at=now - timedelta(seconds=USER_HISTORY_CACHE_TTL_SEC + 1)
    )
    doc_ref.get.return_value = snapshot
    transaction = MagicMock()
    cache = FirestoreUserHistoryCache(db)
    _install_direct_transaction(cache, transaction, monkeypatch)

    assert await cache.claim_refresh("did:plc:user1") is True

    doc_ref.get.assert_awaited_once_with(transaction=transaction)
    update = transaction.set.call_args.args[1]
    assert update.keys() == {"refresh_started_at"}
    assert datetime.now(UTC) - update["refresh_started_at"] < timedelta(seconds=1)
    assert transaction.set.call_args.kwargs == {"merge": True}


@pytest.mark.asyncio
async def test_firestore_cache_reclaims_expired_lease_after_cooldown(monkeypatch):
    db, _collection_ref, doc_ref = _mock_firestore_client()
    now = datetime.now(UTC)
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = _cached_document(
        fetched_at=now - timedelta(seconds=USER_HISTORY_CACHE_TTL_SEC + 1),
        refresh_started_at=now - timedelta(seconds=USER_HISTORY_CACHE_LEASE_SEC + 1),
        refresh_failed_at=now - timedelta(seconds=USER_HISTORY_CACHE_RETRY_COOLDOWN_SEC + 1),
    )
    doc_ref.get.return_value = snapshot
    transaction = MagicMock()
    cache = FirestoreUserHistoryCache(db)
    _install_direct_transaction(cache, transaction, monkeypatch)

    assert await cache.claim_refresh("did:plc:user1") is True
    transaction.set.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    ["already refreshed", "live lease", "retry cooldown"],
)
async def test_firestore_cache_skips_refresh_claim(
    monkeypatch,
    reason,
):
    now = datetime.now(UTC)
    if reason == "already refreshed":
        document = _cached_document(fetched_at=now)
    elif reason == "live lease":
        document = _cached_document(
            fetched_at=now - timedelta(seconds=USER_HISTORY_CACHE_TTL_SEC + 1),
            refresh_started_at=now - timedelta(seconds=1),
        )
    else:
        document = _cached_document(
            fetched_at=now - timedelta(seconds=USER_HISTORY_CACHE_TTL_SEC + 1),
            refresh_failed_at=now - timedelta(seconds=1),
        )
    db, _collection_ref, doc_ref = _mock_firestore_client()
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.return_value = document
    doc_ref.get.return_value = snapshot
    transaction = MagicMock()
    cache = FirestoreUserHistoryCache(db)
    _install_direct_transaction(cache, transaction, monkeypatch)

    assert await cache.claim_refresh("did:plc:user1") is False
    transaction.set.assert_not_called()


@pytest.mark.asyncio
async def test_firestore_cache_releases_failed_refresh_lease():
    db, _collection_ref, doc_ref = _mock_firestore_client()
    before = datetime.now(UTC)

    await FirestoreUserHistoryCache(db).release_refresh("did:plc:user1", failed=True)

    update = doc_ref.set.await_args.args[0]
    assert update["refresh_started_at"] is None
    assert before <= update["refresh_failed_at"] <= datetime.now(UTC)
    assert doc_ref.set.await_args.kwargs == {"merge": True}


def _entry(history: UserHistory, *, age_seconds: float = 0) -> UserHistoryCacheEntry:
    return UserHistoryCacheEntry(
        history=history,
        fetched_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
    )


class _FakeCache(UserHistoryCache):
    def __init__(
        self,
        value: UserHistoryCacheEntry | UserHistory | None = None,
        *,
        retrieve_error: Exception | None = None,
        store_error: Exception | None = None,
        claim_result: bool = True,
        claim_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.value = _entry(value) if isinstance(value, UserHistory) else value
        self.retrieve_error = retrieve_error
        self.store_error = store_error
        self.claim_result = claim_result
        self.claim_error = claim_error
        self.retrieve_calls = 0
        self.store_calls: list[tuple[str, UserHistory]] = []
        self.claim_calls: list[str] = []
        self.release_calls: list[tuple[str, bool]] = []

    async def retrieve(self, user_did: str) -> UserHistoryCacheEntry | None:
        self.retrieve_calls += 1
        if self.retrieve_error is not None:
            raise self.retrieve_error
        return self.value

    async def store(self, user_did: str, history: UserHistory) -> None:
        self.store_calls.append((user_did, history))
        if self.store_error is not None:
            raise self.store_error
        self.value = _entry(history)

    async def claim_refresh(self, user_did: str) -> bool:
        self.claim_calls.append(user_did)
        if self.claim_error is not None:
            raise self.claim_error
        return self.claim_result

    async def release_refresh(self, user_did: str, *, failed: bool) -> None:
        self.release_calls.append((user_did, failed))


class _HangingCache(_FakeCache):
    def __init__(self, *, hang_retrieve: bool = False, hang_store: bool = False) -> None:
        super().__init__()
        self.hang_retrieve = hang_retrieve
        self.hang_store = hang_store

    async def retrieve(self, user_did: str) -> UserHistoryCacheEntry | None:
        self.retrieve_calls += 1
        if self.hang_retrieve:
            await asyncio.Event().wait()
        return self.value

    async def store(self, user_did: str, history: UserHistory) -> None:
        self.store_calls.append((user_did, history))
        if self.hang_store:
            await asyncio.Event().wait()
        self.value = _entry(history)


class _RecordingCollector:
    def __init__(self) -> None:
        self.records: list[tuple[str, float, dict[str, str]]] = []

    def record(self, name: str, value: float, **attributes: str) -> None:
        self.records.append((name, value, attributes))


def _install_index_hydration(
    monkeypatch, *, failures=None, empty_indexes=(), stalled_indexes=(), cancelled_indexes=None
):
    failures = failures if failures is not None else {}
    recent = AsyncMock(
        return_value=(
            ["at://liked/reply", "at://liked/missing", "at://liked/post"],
            ["time-reply", "time-missing", "time-post"],
        )
    )
    results = {
        "posts": [("at://liked/post", [1.0], "did:plc:post", 10)],
        "replies": [("at://liked/reply", [2.0], "did:plc:reply", 20)],
    }

    async def hydrate_index(_es, _uris, *, index):
        if index in stalled_indexes:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if cancelled_indexes is not None:
                    cancelled_indexes.add(index)
                raise
        if index in failures:
            raise failures[index]
        return [] if index in empty_indexes else results[index]

    hydrate = AsyncMock(side_effect=hydrate_index)
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)
    monkeypatch.setattr(user_history_module, "fetch_post_embeddings_and_metadata", hydrate)
    return recent, hydrate


@pytest.mark.asyncio
async def test_cache_hit_skips_elasticsearch(monkeypatch):
    cached = UserHistory(
        items=[
            UserHistoryItem(
                at_uri="at://liked/a",
                liked_at="2026-01-01T00:00:00+00:00",
                embedding=[1.0],
            )
        ]
    )
    cache = _FakeCache(cached)
    set_user_history_cache(cache)
    recent = AsyncMock()
    hydrate = AsyncMock()
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)
    monkeypatch.setattr(user_history_module, "fetch_post_embeddings_and_metadata", hydrate)

    assert await fetch_user_history_features(object(), "did:plc:user1") == cached
    recent.assert_not_awaited()
    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_cache_returns_immediately_and_single_flights_refresh(monkeypatch):
    stale = UserHistory(items=[UserHistoryItem("at://liked/stale", "2026-01-01T00:00:00Z", [1.0])])
    cache = _FakeCache(_entry(stale, age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1))
    set_user_history_cache(cache)
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()

    async def fetch_recent(*_args, **_kwargs):
        refresh_started.set()
        await release_refresh.wait()
        return [], []

    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", fetch_recent)

    assert await fetch_user_history_features(object(), "did:plc:user1") == stale
    assert await fetch_user_history_features(object(), "did:plc:user1") == stale
    await asyncio.wait_for(refresh_started.wait(), timeout=0.1)

    drain_task = asyncio.create_task(cache.drain())
    await asyncio.sleep(0)
    assert not drain_task.done()
    assert cache.claim_calls == ["did:plc:user1"]

    release_refresh.set()
    await asyncio.wait_for(drain_task, timeout=0.1)
    assert cache.store_calls == [("did:plc:user1", UserHistory(items=[]))]
    assert cache.release_calls == []


@pytest.mark.asyncio
async def test_stale_cache_skips_refresh_when_another_instance_holds_lease(monkeypatch):
    stale = UserHistory(items=[])
    cache = _FakeCache(
        _entry(stale, age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1),
        claim_result=False,
    )
    set_user_history_cache(cache)
    recent = AsyncMock()
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)

    assert await fetch_user_history_features(object(), "did:plc:user1") == stale
    await cache.drain()

    assert cache.claim_calls == ["did:plc:user1"]
    recent.assert_not_awaited()
    assert cache.store_calls == []
    assert cache.release_calls == []


@pytest.mark.asyncio
async def test_stale_refresh_failure_releases_lease_and_keeps_stale_value(monkeypatch):
    stale = UserHistory(items=[])
    cache = _FakeCache(_entry(stale, age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1))
    set_user_history_cache(cache)
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        AsyncMock(side_effect=RuntimeError("ES unavailable")),
    )

    assert await fetch_user_history_features(object(), "did:plc:user1") == stale
    await cache.drain()

    assert cache.store_calls == []
    assert cache.release_calls == [("did:plc:user1", True)]


@pytest.mark.asyncio
async def test_stale_refresh_timeout_releases_lease(monkeypatch):
    stale = UserHistory(items=[])
    cache = _FakeCache(_entry(stale, age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1))
    set_user_history_cache(cache)

    async def hang(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(user_history_module, "USER_HISTORY_REFRESH_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", hang)

    assert await fetch_user_history_features(object(), "did:plc:user1") == stale
    await asyncio.wait_for(cache.drain(), timeout=0.1)

    assert cache.store_calls == []
    assert cache.release_calls == [("did:plc:user1", True)]


@pytest.mark.asyncio
async def test_hard_expired_cache_synchronously_fetches_instead_of_serving_stale(monkeypatch):
    expired = UserHistory(
        items=[UserHistoryItem("at://liked/expired", "2026-01-01T00:00:00Z", [1.0])]
    )
    cache = _FakeCache(_entry(expired, age_seconds=USER_HISTORY_CACHE_MAX_AGE_SEC + 1))
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)

    result = await fetch_user_history_features(object(), "did:plc:user1")
    await cache.drain()

    assert result == UserHistory(items=[])
    recent.assert_awaited_once()
    assert cache.claim_calls == []
    assert cache.store_calls == [("did:plc:user1", result)]


@pytest.mark.asyncio
async def test_cache_records_hit_miss_and_write_outcomes(monkeypatch):
    collector = _RecordingCollector()
    monkeypatch.setattr(
        user_history_module,
        "get_metric_collector",
        lambda: collector,
    )
    cache = _FakeCache(UserHistory(items=[]))
    set_user_history_cache(cache)

    await fetch_user_history_features(object(), "did:plc:hit")

    cache.value = None
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        AsyncMock(return_value=([], [])),
    )
    await fetch_user_history_features(object(), "did:plc:miss")
    await cache.drain()

    assert collector.records[0][0] == "user_history.cache.age_seconds"
    assert 0 <= collector.records[0][1] < 1
    assert collector.records[1:] == [
        ("user_history.cache.lookup_count", 1, {"outcome": "hit"}),
        ("user_history.cache.lookup_count", 1, {"outcome": "miss"}),
        ("user_history.cache.write_count", 1, {"outcome": "success"}),
    ]


@pytest.mark.asyncio
async def test_cache_miss_fetches_aligns_and_stores_history(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        AsyncMock(
            return_value=(
                ["at://liked/reply", "at://liked/a", "at://liked/missing", "at://liked/c"],
                ["time-reply", "time-a", "time-missing", "time-c"],
            )
        ),
    )
    started_indexes: set[str] = set()
    both_started = asyncio.Event()

    async def hydrate_index(_es, _uris, *, index):
        started_indexes.add(index)
        if started_indexes == {"posts", "replies"}:
            both_started.set()
        # Neither search can complete until both have started.
        await asyncio.wait_for(both_started.wait(), timeout=1)
        if index == "posts":
            return [
                ("at://liked/c", [3.0], "did:plc:c", 30),
                ("at://liked/a", [1.0], "did:plc:a", 10),
            ]
        return [("at://liked/reply", [2.0], "did:plc:reply", 20)]

    hydrate = AsyncMock(side_effect=hydrate_index)
    monkeypatch.setattr(user_history_module, "fetch_post_embeddings_and_metadata", hydrate)
    es = object()

    history = await fetch_user_history_features(es, "did:plc:user1")
    await cache.drain()

    assert history.items == [
        UserHistoryItem("at://liked/reply", "time-reply", [2.0], "did:plc:reply", 20),
        UserHistoryItem("at://liked/a", "time-a", [1.0], "did:plc:a", 10),
        UserHistoryItem("at://liked/missing", "time-missing", None),
        UserHistoryItem("at://liked/c", "time-c", [3.0], "did:plc:c", 30),
    ]
    liked_uris = [item.at_uri for item in history.items]
    assert hydrate.await_args_list == [
        call(es, liked_uris, index="posts"),
        call(es, liked_uris, index="replies"),
    ]
    assert cache.store_calls == [("did:plc:user1", history)]


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_index", ["posts", "replies"])
@pytest.mark.parametrize("cache_state", ["disabled", "miss", "expired", "read_error"])
async def test_partial_hydration_returns_available_history_without_caching(
    monkeypatch, caplog, failed_index, cache_state
):
    expired = _entry(
        UserHistory(items=[UserHistoryItem("at://old", "time-old", [9.0])]),
        age_seconds=USER_HISTORY_CACHE_MAX_AGE_SEC + 1,
    )
    cache = None
    if cache_state != "disabled":
        cache = _FakeCache(
            value=expired if cache_state == "expired" else None,
            retrieve_error=RuntimeError("cache unavailable")
            if cache_state == "read_error"
            else None,
        )
        set_user_history_cache(cache)
    failure = RuntimeError(f"{failed_index} unavailable")
    recent, hydrate = _install_index_hydration(monkeypatch, failures={failed_index: failure})
    es = object()

    history = await fetch_user_history_features(es, "did:plc:user1")

    assert history.items == [
        (
            UserHistoryItem("at://liked/reply", "time-reply", None)
            if failed_index == "replies"
            else UserHistoryItem("at://liked/reply", "time-reply", [2.0], "did:plc:reply", 20)
        ),
        UserHistoryItem("at://liked/missing", "time-missing", None),
        (
            UserHistoryItem("at://liked/post", "time-post", None)
            if failed_index == "posts"
            else UserHistoryItem("at://liked/post", "time-post", [1.0], "did:plc:post", 10)
        ),
    ]
    recent.assert_awaited_once_with(es, "did:plc:user1", limit=USER_HISTORY_LIMIT)
    assert hydrate.await_args_list == [
        call(es, recent.return_value[0], index="posts"),
        call(es, recent.return_value[0], index="replies"),
    ]
    warnings = [record for record in caplog.records if "hydration" in record.getMessage()]
    assert len(warnings) == 1
    assert failed_index in warnings[0].getMessage()
    assert warnings[0].exc_info[1] is failure
    if cache is not None:
        # A partial value must not become a process-local pending cache hit either.
        assert cache._pending_histories == {}
        assert cache._tasks == {}
        await cache.drain()
        assert cache.store_calls == []
        assert cache.claim_calls == []
        assert cache.value is (expired if cache_state == "expired" else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_index", ["posts", "replies"])
async def test_partial_history_is_shared_within_request_and_retried_on_next_request(
    monkeypatch, failed_index
):
    cache = _FakeCache()
    set_user_history_cache(cache)
    failures = {failed_index: RuntimeError("index unavailable")}
    recent, hydrate = _install_index_hydration(monkeypatch, failures=failures)
    es = object()

    async with request_cache_scope():
        first, second = await asyncio.gather(
            fetch_user_history_features(es, "did:plc:user1"),
            fetch_user_history_features(es, "did:plc:user1"),
        )
        assert await fetch_user_history_features(es, "did:plc:user1") is first

    assert first is second
    assert sum(item.embedding is not None for item in first.items) == 1
    assert recent.await_count == 1
    assert hydrate.await_count == 2
    assert cache._pending_histories == {}
    assert cache.store_calls == []

    failures.clear()
    async with request_cache_scope():
        recovered = await fetch_user_history_features(es, "did:plc:user1")
    await cache.drain()

    assert recovered.items == [
        UserHistoryItem("at://liked/reply", "time-reply", [2.0], "did:plc:reply", 20),
        UserHistoryItem("at://liked/missing", "time-missing", None),
        UserHistoryItem("at://liked/post", "time-post", [1.0], "did:plc:post", 10),
    ]
    assert cache.retrieve_calls == 2
    assert recent.await_count == 2
    assert hydrate.await_count == 4
    assert cache.store_calls == [("did:plc:user1", recovered)]
    async with request_cache_scope():
        assert await fetch_user_history_features(es, "did:plc:user1") is recovered
    assert recent.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stalled_index", ["posts", "replies"])
@pytest.mark.parametrize("empty_sibling", [False, True])
async def test_slow_index_returns_partial_history_before_outer_deadline_and_recovers(
    monkeypatch, caplog, stalled_index, empty_sibling
):
    cache = _FakeCache()
    set_user_history_cache(cache)
    stalled_indexes = {stalled_index}
    empty_indexes = {"posts", "replies"} - stalled_indexes if empty_sibling else set()
    cancelled_indexes: set[str] = set()
    recent, hydrate = _install_index_hydration(
        monkeypatch,
        empty_indexes=empty_indexes,
        stalled_indexes=stalled_indexes,
        cancelled_indexes=cancelled_indexes,
    )
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0.01)
    es = object()

    async with request_cache_scope():
        first, second = await asyncio.wait_for(
            asyncio.gather(
                fetch_user_history_features(es, "did:plc:user1"),
                fetch_user_history_features(es, "did:plc:user1"),
            ),
            timeout=1,
        )
        assert await fetch_user_history_features(es, "did:plc:user1") is first

    assert first is second
    assert [item.at_uri for item in first.items] == recent.return_value[0]
    assert [item.liked_at for item in first.items] == recent.return_value[1]
    assert [item.embedding for item in first.items] == [
        None if stalled_index == "replies" or empty_sibling else [2.0],
        None,
        None if stalled_index == "posts" or empty_sibling else [1.0],
    ]
    assert cancelled_indexes == {stalled_index}
    recent.assert_awaited_once()
    assert hydrate.await_count == 2
    assert cache.value is None
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache._tasks == {}
    warnings = [record for record in caplog.records if "hydration" in record.getMessage()]
    assert len(warnings) == 1
    assert stalled_index in warnings[0].getMessage()
    assert isinstance(warnings[0].exc_info[1], TimeoutError)

    stalled_indexes.clear()
    empty_indexes.clear()
    async with request_cache_scope():
        recovered = await fetch_user_history_features(es, "did:plc:user1")
    await cache.drain()

    assert sum(item.embedding is not None for item in recovered.items) == 2
    assert recent.await_count == 2
    assert hydrate.await_count == 4
    assert cache.store_calls == [("did:plc:user1", recovered)]
    assert await fetch_user_history_features(es, "did:plc:user1") is recovered
    assert recent.await_count == 2


@pytest.mark.asyncio
async def test_both_slow_indexes_raise_timeout_and_cancel_queries_without_caching(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    cancelled_indexes: set[str] = set()
    _recent, hydrate = _install_index_hydration(
        monkeypatch,
        stalled_indexes={"posts", "replies"},
        cancelled_indexes=cancelled_indexes,
    )
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0.01)

    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await fetch_user_history_features(object(), "did:plc:user1")

    assert hydrate.await_count == 2
    assert cancelled_indexes == {"posts", "replies"}
    assert cache.value is None
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache._tasks == {}


@pytest.mark.asyncio
async def test_slow_recent_likes_times_out_before_hydration_and_can_be_retried(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent, hydrate = _install_index_hydration(monkeypatch)
    cancelled = asyncio.Event()

    async def hang(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    recent.side_effect = hang
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0.01)

    async with request_cache_scope():
        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError):
                await fetch_user_history_features(object(), "did:plc:user1")

    assert cancelled.is_set()
    hydrate.assert_not_awaited()
    assert cache.value is None
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache._tasks == {}

    recent.side_effect = None
    async with request_cache_scope():
        recovered = await fetch_user_history_features(object(), "did:plc:user1")
    await cache.drain()
    assert recent.await_count == 2
    assert hydrate.await_count == 2
    assert cache.store_calls == [("did:plc:user1", recovered)]


@pytest.mark.asyncio
async def test_likes_and_hydration_share_one_deadline_including_likes_latency(monkeypatch):
    recent, hydrate = _install_index_hydration(monkeypatch)
    original_timeout_at = asyncio.timeout_at
    deadlines: list[float] = []
    remaining_budgets: list[float] = []

    def record_timeout_at(deadline):
        deadlines.append(deadline)
        remaining_budgets.append(deadline - asyncio.get_running_loop().time())
        return original_timeout_at(deadline)

    async def fetch_recent(*_args, **_kwargs):
        # Let time advance before hydration without relying on a sleep duration
        # or asserting a narrow real-time scheduling window.
        await asyncio.sleep(0)
        return recent.return_value

    recent.side_effect = fetch_recent
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(user_history_module.asyncio, "timeout_at", record_timeout_at)

    history = await fetch_user_history_features(object(), "did:plc:user1")

    assert sum(item.embedding is not None for item in history.items) == 2
    assert hydrate.await_count == 2
    assert len(deadlines) == 3
    assert deadlines[0] == deadlines[1] == deadlines[2]
    assert 0 < remaining_budgets[0] <= 0.5
    assert all(0 < budget < remaining_budgets[0] for budget in remaining_budgets[1:])


@pytest.mark.asyncio
async def test_expired_history_budget_does_not_start_hydration(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent, hydrate = _install_index_hydration(monkeypatch)
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0)

    # The recent-likes mock completes without yielding, so the timeout callback
    # cannot fire before the explicit deadline check ahead of hydration.
    with pytest.raises(TimeoutError):
        await fetch_user_history_features(object(), "did:plc:user1")

    recent.assert_awaited_once()
    hydrate.assert_not_awaited()
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache._tasks == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_indexes", [("posts",), ("replies",), ("posts", "replies")])
async def test_successful_indexes_with_no_matches_are_cached(monkeypatch, empty_indexes):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent, hydrate = _install_index_hydration(monkeypatch, empty_indexes=empty_indexes)

    history = await fetch_user_history_features(object(), "did:plc:user1")
    await cache.drain()

    assert [item.at_uri for item in history.items] == recent.return_value[0]
    assert [item.liked_at for item in history.items] == recent.return_value[1]
    assert [item.embedding for item in history.items] == [
        None if "replies" in empty_indexes else [2.0],
        None,
        None if "posts" in empty_indexes else [1.0],
    ]
    assert cache.store_calls == [("did:plc:user1", history)]
    assert await fetch_user_history_features(object(), "did:plc:user1") is history
    recent.assert_awaited_once()
    assert hydrate.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_index", ["posts", "replies"])
async def test_empty_successful_index_still_returns_partial_history(monkeypatch, failed_index):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent, _hydrate = _install_index_hydration(
        monkeypatch,
        failures={failed_index: RuntimeError("index unavailable")},
        empty_indexes=("posts", "replies"),
    )

    history = await fetch_user_history_features(object(), "did:plc:user1")
    await cache.drain()

    assert history.items == [
        UserHistoryItem("at://liked/reply", "time-reply", None),
        UserHistoryItem("at://liked/missing", "time-missing", None),
        UserHistoryItem("at://liked/post", "time-post", None),
    ]
    recent.assert_awaited_once()
    assert cache.store_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_state", ["disabled", "miss", "expired"])
async def test_both_hydration_failures_raise_and_are_not_cached(monkeypatch, caplog, cache_state):
    cache = None
    if cache_state != "disabled":
        cache = _FakeCache(
            _entry(UserHistory(items=[]), age_seconds=USER_HISTORY_CACHE_MAX_AGE_SEC + 1)
            if cache_state == "expired"
            else None
        )
        set_user_history_cache(cache)
    failures = {"posts": RuntimeError("posts unavailable"), "replies": ValueError("replies failed")}
    _recent, hydrate = _install_index_hydration(monkeypatch, failures=failures)

    with pytest.raises(RuntimeError) as error:
        await fetch_user_history_features(object(), "did:plc:user1")

    assert error.value is failures["posts"]
    assert hydrate.await_count == 2
    warnings = [record for record in caplog.records if "hydration" in record.getMessage()]
    assert {record.exc_info[1] for record in warnings} == set(failures.values())
    if cache is not None:
        await cache.drain()
        assert cache.store_calls == []
        assert cache._pending_histories == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_indexes", [("posts",), ("replies",), ("posts", "replies")])
async def test_stale_hydration_failure_keeps_cached_history_and_releases_failed_lease(
    monkeypatch, failed_indexes
):
    stale = _entry(
        UserHistory(items=[UserHistoryItem("at://old", "time-old", [9.0])]),
        age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1,
    )
    cache = _FakeCache(stale)
    set_user_history_cache(cache)
    collector = _RecordingCollector()
    monkeypatch.setattr(user_history_module, "get_metric_collector", lambda: collector)
    failures = {index: RuntimeError(f"{index} unavailable") for index in failed_indexes}
    _install_index_hydration(monkeypatch, failures=failures)

    assert await fetch_user_history_features(object(), "did:plc:user1") is stale.history
    await cache.drain()

    assert cache.value is stale
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache.claim_calls == ["did:plc:user1"]
    assert cache.release_calls == [("did:plc:user1", True)]
    assert (
        "user_history.cache.refresh_count",
        1,
        {"outcome": "error" if len(failed_indexes) == 2 else "partial"},
    ) in collector.records

    # The expired cooldown is represented by a new successful lease claim.
    failures.clear()
    assert await fetch_user_history_features(object(), "did:plc:user1") is stale.history
    await cache.drain()
    assert len(cache.store_calls) == 1
    assert cache.value is not None
    assert sum(item.embedding is not None for item in cache.value.history.items) == 2
    assert cache.release_calls == [("did:plc:user1", True)]
    assert ("user_history.cache.refresh_count", 1, {"outcome": "success"}) in collector.records


@pytest.mark.asyncio
@pytest.mark.parametrize("stalled", [("posts",), ("replies",), ("posts", "replies")])
async def test_stale_slow_hydration_uses_background_budget_and_preserves_cached_history(
    monkeypatch, stalled
):
    stale = _entry(
        UserHistory(items=[UserHistoryItem("at://old", "time-old", [9.0])]),
        age_seconds=USER_HISTORY_CACHE_TTL_SEC + 1,
    )
    cache = _FakeCache(stale)
    set_user_history_cache(cache)
    collector = _RecordingCollector()
    monkeypatch.setattr(user_history_module, "get_metric_collector", lambda: collector)
    # Background refreshes must use their larger budget, independently of a
    # foreground deadline that would expire before hydration can start.
    monkeypatch.setattr(user_history_module, "USER_HISTORY_ES_FETCH_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(user_history_module, "USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS", 0.01)
    stalled_indexes = set(stalled)
    cancelled_indexes: set[str] = set()
    _recent, hydrate = _install_index_hydration(
        monkeypatch, stalled_indexes=stalled_indexes, cancelled_indexes=cancelled_indexes
    )

    assert await fetch_user_history_features(object(), "did:plc:user1") is stale.history
    await asyncio.wait_for(cache.drain(), timeout=1)

    assert hydrate.await_count == 2
    assert cancelled_indexes == stalled_indexes
    assert cache.value is stale
    assert cache.store_calls == []
    assert cache._pending_histories == {}
    assert cache._tasks == {}
    assert cache.claim_calls == ["did:plc:user1"]
    assert cache.release_calls == [("did:plc:user1", True)]
    assert (
        "user_history.cache.refresh_count",
        1,
        {"outcome": "timeout" if len(stalled) == 2 else "partial"},
    ) in collector.records

    stalled_indexes.clear()
    assert await fetch_user_history_features(object(), "did:plc:user1") is stale.history
    await cache.drain()

    assert hydrate.await_count == 4
    assert len(cache.store_calls) == 1
    assert cache.value is not None
    assert sum(item.embedding is not None for item in cache.value.history.items) == 2
    assert cache.release_calls == [("did:plc:user1", True)]
    assert ("user_history.cache.refresh_count", 1, {"outcome": "success"}) in collector.records


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled_index", ["posts", "replies"])
async def test_index_cancellation_propagates_without_caching(monkeypatch, cancelled_index):
    cache = _FakeCache()
    set_user_history_cache(cache)
    _install_index_hydration(monkeypatch, failures={cancelled_index: asyncio.CancelledError()})

    with pytest.raises(asyncio.CancelledError):
        await fetch_user_history_features(object(), "did:plc:user1")

    await cache.drain()
    assert cache.store_calls == []
    assert cache._pending_histories == {}


@pytest.mark.asyncio
async def test_request_cancellation_cancels_both_hydration_calls(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    _install_index_hydration(monkeypatch)
    started_indexes: set[str] = set()
    cancelled_indexes: set[str] = set()
    both_started = asyncio.Event()

    async def hang(_es, _uris, *, index):
        started_indexes.add(index)
        if started_indexes == {"posts", "replies"}:
            both_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_indexes.add(index)
            raise

    monkeypatch.setattr(user_history_module, "fetch_post_embeddings_and_metadata", hang)
    task = asyncio.create_task(fetch_user_history_features(object(), "did:plc:user1"))
    try:
        await asyncio.wait_for(both_started.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert cancelled_indexes == {"posts", "replies"}
    assert cache.store_calls == []
    assert cache._pending_histories == {}


@pytest.mark.asyncio
async def test_empty_history_is_cached(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    hydrate = AsyncMock()
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)
    monkeypatch.setattr(user_history_module, "fetch_post_embeddings_and_metadata", hydrate)

    first = await fetch_user_history_features(object(), "did:plc:user1")
    second = await fetch_user_history_features(object(), "did:plc:user1")
    await cache.drain()

    assert first == second == UserHistory(items=[])
    assert cache.retrieve_calls == 1
    recent.assert_awaited_once()
    hydrate.assert_not_awaited()
    assert len(cache.store_calls) == 1


@pytest.mark.asyncio
async def test_cache_read_and_write_failures_return_fresh_history(monkeypatch):
    cache = _FakeCache(
        retrieve_error=RuntimeError("read failed"),
        store_error=RuntimeError("write failed"),
    )
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)

    assert await fetch_user_history_features(object(), "did:plc:user1") == UserHistory(items=[])
    await cache.drain()
    recent.assert_awaited_once()
    assert len(cache.store_calls) == 1


@pytest.mark.asyncio
async def test_cache_read_timeout_falls_back_to_elasticsearch(monkeypatch):
    cache = _HangingCache(hang_retrieve=True)
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    monkeypatch.setattr(
        user_history_module,
        "USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        recent,
    )

    history = await asyncio.wait_for(
        fetch_user_history_features(object(), "did:plc:user1"),
        timeout=0.1,
    )
    await cache.drain()

    assert history == UserHistory(items=[])
    assert cache.retrieve_calls == 1
    recent.assert_awaited_once()
    assert cache.store_calls == [("did:plc:user1", history)]


@pytest.mark.asyncio
async def test_cache_write_timeout_returns_fresh_history(monkeypatch):
    cache = _HangingCache(hang_store=True)
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    monkeypatch.setattr(
        user_history_module,
        "USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        recent,
    )

    history = await asyncio.wait_for(
        fetch_user_history_features(object(), "did:plc:user1"),
        timeout=0.1,
    )
    await cache.drain()

    assert history == UserHistory(items=[])
    recent.assert_awaited_once()
    assert cache.store_calls == [("did:plc:user1", history)]


@pytest.mark.asyncio
async def test_expired_miss_does_not_fall_back_to_stale_when_es_fails(monkeypatch):
    cache = _FakeCache(value=None)
    set_user_history_cache(cache)
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        AsyncMock(side_effect=RuntimeError("ES unavailable")),
    )

    with pytest.raises(RuntimeError, match="ES unavailable"):
        await fetch_user_history_features(object(), "did:plc:user1")


@pytest.mark.asyncio
async def test_request_cache_collapses_concurrent_history_lookups(monkeypatch):
    cache = _FakeCache()
    set_user_history_cache(cache)
    recent_calls = 0

    async def fetch_recent(*_args, **_kwargs):
        nonlocal recent_calls
        recent_calls += 1
        await asyncio.sleep(0)
        return [], []

    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", fetch_recent)

    async with request_cache_scope():
        histories = await asyncio.gather(
            fetch_user_history_features(object(), "did:plc:user1"),
            fetch_user_history_features(object(), "did:plc:user1"),
        )
    await cache.drain()

    assert histories == [UserHistory(items=[]), UserHistory(items=[])]
    assert cache.retrieve_calls == 1
    assert recent_calls == 1
    assert len(cache.store_calls) == 1


@pytest.mark.asyncio
async def test_cache_miss_returns_before_background_write_finishes(monkeypatch):
    class _BlockingStoreCache(_FakeCache):
        def __init__(self) -> None:
            super().__init__()
            self.store_started = asyncio.Event()
            self.release_store = asyncio.Event()

        async def store(self, user_did: str, history: UserHistory) -> None:
            self.store_calls.append((user_did, history))
            self.store_started.set()
            await self.release_store.wait()
            self.value = _entry(history)

    cache = _BlockingStoreCache()
    set_user_history_cache(cache)
    recent = AsyncMock(return_value=([], []))
    monkeypatch.setattr(user_history_module, "fetch_recent_liked_post_uris_and_times", recent)

    history = await asyncio.wait_for(
        fetch_user_history_features(object(), "did:plc:user1"),
        timeout=0.1,
    )
    await asyncio.wait_for(cache.store_started.wait(), timeout=0.1)

    drain_task = asyncio.create_task(cache.drain())
    await asyncio.sleep(0)

    assert history == UserHistory(items=[])
    assert not drain_task.done()
    assert cache.store_calls == [("did:plc:user1", history)]

    cache.release_store.set()
    await asyncio.wait_for(drain_task, timeout=0.1)


@pytest.mark.asyncio
async def test_cache_instances_own_and_drain_their_background_tasks(monkeypatch):
    class _BlockingStoreCache(_FakeCache):
        def __init__(self) -> None:
            super().__init__()
            self.store_started = asyncio.Event()
            self.release_store = asyncio.Event()

        async def store(self, user_did: str, history: UserHistory) -> None:
            self.store_calls.append((user_did, history))
            self.store_started.set()
            await self.release_store.wait()

    first_cache = _BlockingStoreCache()
    second_cache = _BlockingStoreCache()
    monkeypatch.setattr(
        user_history_module,
        "fetch_recent_liked_post_uris_and_times",
        AsyncMock(return_value=([], [])),
    )

    try:
        set_user_history_cache(first_cache)
        await fetch_user_history_features(object(), "did:plc:first")
        await asyncio.wait_for(first_cache.store_started.wait(), timeout=0.1)

        set_user_history_cache(second_cache)
        await fetch_user_history_features(object(), "did:plc:second")
        await asyncio.wait_for(second_cache.store_started.wait(), timeout=0.1)

        first_cache.release_store.set()
        await asyncio.wait_for(first_cache.drain(), timeout=0.1)
        assert not second_cache.release_store.is_set()
    finally:
        first_cache.release_store.set()
        second_cache.release_store.set()
        await asyncio.gather(first_cache.drain(), second_cache.drain())

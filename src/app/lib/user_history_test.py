"""Tests for the shared Firestore-backed user-history cache."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from . import user_history as user_history_module
from .embeddings import MINILM_L12_EMBEDDING_KEY, encode_float32_b64
from .request_cache import request_cache_scope
from .user_history import (
    USER_HISTORY_CACHE_COLLECTION,
    USER_HISTORY_CACHE_TTL_SECONDS,
    USER_HISTORY_CACHE_VERSION,
    USER_HISTORY_LIMIT,
    FirestoreUserHistoryCache,
    UserHistory,
    UserHistoryCache,
    UserHistoryItem,
    _user_history_cache_key,
    fetch_user_history_features,
    set_user_history_cache,
)


@pytest.fixture(autouse=True)
def clear_shared_cache():
    set_user_history_cache(None)
    yield
    set_user_history_cache(None)


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
    expires_at: datetime | None = None,
) -> dict:
    now = datetime.now(UTC)
    return {
        "schema_version": USER_HISTORY_CACHE_VERSION,
        "user_did": user_did,
        "history_limit": USER_HISTORY_LIMIT,
        "embedding_key": MINILM_L12_EMBEDDING_KEY,
        "fetched_at": now,
        "expires_at": expires_at or now + timedelta(minutes=5),
        "items": [
            {
                "at_uri": "at://liked/a",
                "liked_at": "2026-01-01T00:00:00+00:00",
                "embedding_b64": encode_float32_b64([1.0, 2.0]),
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

    history = await FirestoreUserHistoryCache(db).retrieve("did:plc:user1")

    db.collection.assert_called_once_with(USER_HISTORY_CACHE_COLLECTION)
    collection_ref.document.assert_called_once_with(_user_history_cache_key("did:plc:user1"))
    assert history == UserHistory(
        items=[
            UserHistoryItem(
                at_uri="at://liked/a",
                liked_at="2026-01-01T00:00:00+00:00",
                embedding=[1.0, 2.0],
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
async def test_firestore_cache_stores_float32_embeddings_with_exact_ttl():
    db, _collection_ref, doc_ref = _mock_firestore_client()
    before = datetime.now(UTC)

    await FirestoreUserHistoryCache(db).store(
        "did:plc:user1",
        UserHistory(
            items=[
                UserHistoryItem(
                    at_uri="at://liked/a",
                    liked_at="2026-01-01T00:00:00+00:00",
                    embedding=[1.0, 2.0],
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
    assert stored["items"][0]["embedding_b64"] == encode_float32_b64([1.0, 2.0])
    assert before <= stored["fetched_at"] <= after
    assert stored["expires_at"] - stored["fetched_at"] == timedelta(
        seconds=USER_HISTORY_CACHE_TTL_SECONDS
    )


class _FakeCache(UserHistoryCache):
    def __init__(
        self,
        value: UserHistory | None = None,
        *,
        retrieve_error: Exception | None = None,
        store_error: Exception | None = None,
    ) -> None:
        self.value = value
        self.retrieve_error = retrieve_error
        self.store_error = store_error
        self.retrieve_calls = 0
        self.store_calls: list[tuple[str, UserHistory]] = []

    async def retrieve(self, user_did: str) -> UserHistory | None:
        self.retrieve_calls += 1
        if self.retrieve_error is not None:
            raise self.retrieve_error
        return self.value

    async def store(self, user_did: str, history: UserHistory) -> None:
        self.store_calls.append((user_did, history))
        if self.store_error is not None:
            raise self.store_error
        self.value = history


class _RecordingCollector:
    def __init__(self) -> None:
        self.records: list[tuple[str, int, dict[str, str]]] = []

    def record(self, name: str, value: int, **attributes: str) -> None:
        self.records.append((name, value, attributes))


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

    assert collector.records == [
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
                ["at://liked/a", "at://liked/missing", "at://liked/c"],
                ["time-a", "time-missing", "time-c"],
            )
        ),
    )
    monkeypatch.setattr(
        user_history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(
            return_value=[
                ("at://liked/c", [3.0], "did:plc:c", 30),
                ("at://liked/a", [1.0], "did:plc:a", 10),
            ]
        ),
    )

    history = await fetch_user_history_features(object(), "did:plc:user1")

    assert history.items == [
        UserHistoryItem("at://liked/a", "time-a", [1.0], "did:plc:a", 10),
        UserHistoryItem("at://liked/missing", "time-missing", None),
        UserHistoryItem("at://liked/c", "time-c", [3.0], "did:plc:c", 30),
    ]
    assert cache.store_calls == [("did:plc:user1", history)]


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

    assert first == second == UserHistory(items=[])
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
    recent.assert_awaited_once()
    assert len(cache.store_calls) == 1


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

    assert histories == [UserHistory(items=[]), UserHistory(items=[])]
    assert cache.retrieve_calls == 1
    assert recent_calls == 1
    assert len(cache.store_calls) == 1

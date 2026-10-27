"""Durable negative-history hydration, retry accounting, and background work."""

from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from elastic_transport import ConnectionTimeout

from . import request_less_history as history_module
from .embeddings import (
    MINILM_L12_EMBEDDING_DIM,
    MINILM_L12_EMBEDDING_KEY,
    encode_float32_b64,
)
from .request_cache import get_request_cache, request_cache_scope

NOW = datetime(2026, 10, 26, 12, tzinfo=UTC)
USER = "did:plc:negative-history-user"
URI = "at://did:plc:author/app.bsky.feed.post/first"
OTHER_URI = "at://did:plc:author/app.bsky.feed.post/second"
VECTOR = [0.25, -0.5, *([0.0] * (MINILM_L12_EMBEDDING_DIM - 2))]
FEATURES = (VECTOR, "did:plc:author", 7)


class _Snapshot:
    def __init__(self, data: dict | None):
        self.exists = data is not None
        self._data = deepcopy(data)

    def to_dict(self):
        return deepcopy(self._data)


class _Reference:
    def __init__(self, store, path):
        self.store = store
        self.path = path

    async def get(self, *, transaction=None):
        self.store.reads.append(self.path)
        if self.store.read_error is not None:
            raise self.store.read_error
        return _Snapshot(self.store.documents.get(self.path))


class _Transaction:
    """Stage writes so a retried callback cannot commit its abandoned result."""

    def __init__(self, store):
        self.store = store
        self.pending: dict[str, dict] = {}

    def set(self, ref, data, merge=False):
        existing = deepcopy(self.store.documents.get(ref.path, {})) if merge else {}
        self.pending[ref.path] = {**existing, **deepcopy(data)}

    async def get(self, ref):
        return await ref.get(transaction=self)

    def commit(self):
        self.store.documents.update(self.pending)


class _Store:
    def __init__(self):
        self.documents: dict[str, dict] = {}
        self.reads: list[str] = []
        self.read_error: Exception | None = None
        self.replay_next = False
        self.replay_every = False
        self.lock = asyncio.Lock()
        self.db = MagicMock()
        self.db.collection.side_effect = self.collection

    def collection(self, name):
        collection = MagicMock()
        collection.document.side_effect = lambda key: _Reference(self, f"{name}/{key}")
        return collection

    async def run_transaction(self, body):
        async with self.lock:
            if self.replay_next or self.replay_every:
                self.replay_next = False
                await body(_Transaction(self))
            transaction = _Transaction(self)
            result = await body(transaction)
            transaction.commit()
            return result

    def data(self, user_did=USER):
        key = hashlib.sha256(user_did.encode()).hexdigest()
        return self.documents[f"{history_module.HISTORY_COLLECTION}/{key}"]


@pytest.fixture(autouse=True)
def _clear_service():
    history_module.set_request_less_history_service(None)
    yield
    history_module.set_request_less_history_service(None)


@pytest.fixture
def clock(monkeypatch):
    current = [NOW]
    monkeypatch.setattr(history_module, "_now", lambda: current[0])
    return current


@pytest.fixture
def store():
    return _Store()


def _service(store, monkeypatch, *, background=False):
    service = history_module.RequestLessHistoryService(store.db, object())
    monkeypatch.setattr(service, "_run_transaction", store.run_transaction)
    if not background:
        monkeypatch.setattr(service, "schedule_hydration", MagicMock())
    return service


async def _record(service, user_did, uri_times):
    await service.record(user_did, uri_times)


async def _save_features(service, uri=URI):
    claims = await service._claim_due(USER)
    assert [claim.at_uri for claim in claims] == [uri]
    result = await service._store_results(USER, claims, {uri: FEATURES})
    assert result.hydrated_uris == {uri}


@pytest.mark.asyncio
async def test_record_uses_dedicated_user_document_and_keeps_latest_receipt(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW, OTHER_URI: NOW - timedelta(minutes=1)})
    await _record(service, USER, {URI: NOW - timedelta(days=1)})
    await _record(service, USER, {OTHER_URI: NOW + timedelta(minutes=1)})

    history = await service.retrieve(USER)

    assert [(item.at_uri, item.received_at) for item in history.items] == [
        (OTHER_URI, NOW + timedelta(minutes=1)),
        (URI, NOW),
    ]
    assert history.items_with_embeddings == []
    assert len(store.documents) == 1
    data = store.data()
    assert data["schema_version"] == history_module.SCHEMA_VERSION
    assert data["embedding_key"] == MINILM_L12_EMBEDDING_KEY
    assert data["user_did"] == USER
    assert "expires_at" not in data


@pytest.mark.asyncio
async def test_record_keeps_only_newest_bounded_history_including_pending_items(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    count = history_module.USER_HISTORY_LIMIT + 3
    await _record(
        service,
        USER,
        {f"at://post/{index}": NOW + timedelta(seconds=index) for index in range(count)},
    )

    history = await service.retrieve(USER)

    assert len(history.items) == history_module.USER_HISTORY_LIMIT
    assert [item.at_uri for item in history.items] == [
        f"at://post/{index}" for index in range(count - 1, 2, -1)
    ]


@pytest.mark.asyncio
async def test_record_empty_batch_does_not_create_history(store, monkeypatch, clock):
    service = _service(store, monkeypatch)

    await _record(service, USER, {})

    assert store.documents == {}
    cast(MagicMock, service.schedule_hydration).assert_not_called()


@pytest.mark.asyncio
async def test_hydrated_snapshot_survives_age_and_repeated_feedback(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    await _save_features(service)
    clock[0] = NOW + timedelta(days=365)
    await _record(service, USER, {URI: clock[0]})

    history = await service.retrieve(USER)

    assert len(history.items_with_embeddings) == 1
    item = history.items_with_embeddings[0]
    assert item.received_at == clock[0]
    assert item.embedding == VECTOR
    assert item.author_did == "did:plc:author"
    assert item.like_count == 7
    assert await service._claim_due(USER) == []
    assert "embedding_b64" in store.data()["items"][0]
    assert "embedding" not in store.data()["items"][0]


@pytest.mark.asyncio
async def test_claim_enforces_three_attempts_fifteen_minute_spacing_and_no_age_deadline(
    store,
    monkeypatch,
    clock,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})

    first = await service._claim_due(USER)
    assert len(first) == 1
    assert first[0].attempt_count == 1
    assert first[0].next_attempt_at == NOW + timedelta(seconds=900)
    assert await service._claim_due(USER) == []
    clock[0] = NOW + timedelta(seconds=899, microseconds=999999)
    assert await service._claim_due(USER) == []
    clock[0] = NOW + timedelta(seconds=900)
    second = await service._claim_due(USER)
    assert second[0].attempt_count == 2

    clock[0] = NOW + timedelta(days=90)
    third = await service._claim_due(USER)
    assert third[0].attempt_count == 3
    clock[0] += timedelta(days=90)
    assert await service._claim_due(USER) == []


@pytest.mark.asyncio
async def test_repeated_pending_feedback_preserves_retry_budget_and_cooldown(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    await service._claim_due(USER)
    clock[0] += timedelta(seconds=100)
    await _record(service, USER, {URI: clock[0]})

    assert await service._claim_due(USER) == []
    clock[0] = NOW + timedelta(seconds=900)
    claims = await service._claim_due(USER)
    assert claims[0].attempt_count == 2


@pytest.mark.asyncio
async def test_concurrent_service_instances_claim_each_post_once(store, monkeypatch, clock):
    first = _service(store, monkeypatch)
    second = _service(store, monkeypatch)
    await _record(first, USER, {URI: NOW, OTHER_URI: NOW})

    claims = await asyncio.gather(first._claim_due(USER), second._claim_due(USER))

    assert sorted(claim.at_uri for batch in claims for claim in batch) == sorted([URI, OTHER_URI])
    assert sorted(len(batch) for batch in claims) == [0, 2]


@pytest.mark.asyncio
async def test_hydration_completion_preserves_feedback_arriving_in_flight(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    claims = await service._claim_due(USER)
    later = NOW + timedelta(minutes=1)
    await _record(service, USER, {URI: later, OTHER_URI: later})

    assert (await service._store_results(USER, claims, {URI: FEATURES})).hydrated_uris == {URI}
    history = await service.retrieve(USER)

    assert {item.at_uri for item in history.items} == {URI, OTHER_URI}
    item = next(item for item in history.items if item.at_uri == URI)
    assert item.received_at == later
    assert item.embedding == VECTOR


@pytest.mark.asyncio
async def test_stale_attempt_completion_cannot_overwrite_newer_attempt(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    first = await service._claim_due(USER)
    clock[0] += timedelta(seconds=900)
    second = await service._claim_due(USER)

    assert (await service._store_results(USER, first, {URI: FEATURES})).hydrated_uris == set()
    assert (await service._store_results(USER, second, {URI: FEATURES})).hydrated_uris == {URI}


@pytest.mark.asyncio
async def test_completion_does_not_resurrect_pruned_history(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    claims = await service._claim_due(USER)
    await _record(
        service,
        USER,
        {
            f"at://post/{index}": NOW + timedelta(seconds=index + 1)
            for index in range(history_module.USER_HISTORY_LIMIT)
        },
    )

    assert (await service._store_results(USER, claims, {URI: FEATURES})).hydrated_uris == set()
    assert URI not in [item.at_uri for item in (await service.retrieve(USER)).items]


@pytest.mark.asyncio
async def test_old_attempt_cannot_match_a_pruned_then_recreated_uri(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    old_claims = await service._claim_due(USER)
    await _record(
        service,
        USER,
        {
            f"at://post/{index}": NOW + timedelta(seconds=index + 1)
            for index in range(history_module.USER_HISTORY_LIMIT)
        },
    )
    await _record(service, USER, {URI: NOW + timedelta(hours=1)})
    new_claim = next(claim for claim in await service._claim_due(USER) if claim.at_uri == URI)

    assert new_claim.attempt_count == old_claims[0].attempt_count
    assert new_claim.next_attempt_at == old_claims[0].next_attempt_at
    assert new_claim.attempt_id != old_claims[0].attempt_id
    old_result = await service._store_results(USER, old_claims, {URI: FEATURES})
    assert old_result.hydrated_uris == set()
    assert old_result.applicable_uris == set()
    assert (await service._store_results(USER, [new_claim], {URI: FEATURES})).hydrated_uris == {URI}


@pytest.mark.asyncio
async def test_missing_history_returns_empty_without_elasticsearch(store, monkeypatch, clock):
    hydrate = AsyncMock()
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    service = _service(store, monkeypatch, background=True)

    assert (await service.retrieve(USER)).items == []
    await service.drain()

    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_background_hydration_batches_posts_and_replies_and_retains_missing_items(
    store,
    monkeypatch,
    clock,
):
    hydrate = AsyncMock(return_value=[(OTHER_URI, *FEATURES)])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    service = _service(store, monkeypatch, background=True)

    await _record(service, USER, {URI: NOW, OTHER_URI: NOW + timedelta(seconds=1)})
    await service.drain()
    history = await service.retrieve(USER)

    hydrate.assert_awaited_once()
    assert hydrate.await_args is not None
    assert set(hydrate.await_args.args[1]) == {URI, OTHER_URI}
    assert hydrate.await_args.kwargs["index"] == "posts,replies"
    assert [item.at_uri for item in history.items] == [OTHER_URI, URI]
    assert [item.at_uri for item in history.items_with_embeddings] == [OTHER_URI]
    clock[0] += timedelta(seconds=900)
    assert [claim.at_uri for claim in await service._claim_due(USER)] == [URI]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("ES unavailable"), TimeoutError("ES timeout")])
async def test_es_failures_count_toward_exhaustion_without_losing_existing_features(
    store,
    monkeypatch,
    clock,
    failure,
):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    await _save_features(seed)
    await _record(seed, USER, {OTHER_URI: NOW})
    hydrate = AsyncMock(side_effect=failure)
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)

    for attempt in range(3):
        service = _service(store, monkeypatch, background=True)
        service.schedule_hydration(USER)
        await service.drain()
        clock[0] += timedelta(seconds=900)
        assert hydrate.await_count == attempt + 1

    final = _service(store, monkeypatch, background=True)
    history = await final.retrieve(USER)
    await final.drain()

    assert hydrate.await_count == 3
    assert [item.at_uri for item in history.items_with_embeddings] == [URI]
    assert {item.at_uri for item in history.items} == {URI, OTHER_URI}


@pytest.mark.asyncio
async def test_retrieve_returns_before_scheduled_hydration_finishes(store, monkeypatch, clock):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    started = asyncio.Event()
    release = asyncio.Event()

    async def hydrate(*args, **kwargs):
        started.set()
        await release.wait()
        return [(URI, *FEATURES)]

    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    service = _service(store, monkeypatch, background=True)
    try:
        history = await asyncio.wait_for(service.retrieve(USER), timeout=0.2)
        await asyncio.wait_for(started.wait(), timeout=0.2)
        assert history.items_with_embeddings == []
    finally:
        release.set()
        await service.drain()

    assert [item.at_uri for item in (await service.retrieve(USER)).items_with_embeddings] == [URI]


@pytest.mark.asyncio
async def test_drain_waits_for_owned_work_and_prevents_new_hydration(store, monkeypatch, clock):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    started = asyncio.Event()
    release = asyncio.Event()

    async def hydrate(*args, **kwargs):
        started.set()
        await release.wait()
        return []

    fetch = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)
    service.schedule_hydration(USER)
    await asyncio.wait_for(started.wait(), timeout=0.2)
    drain = asyncio.create_task(service.drain())
    await asyncio.sleep(0)
    assert not drain.done()
    release.set()
    await asyncio.wait_for(drain, timeout=0.2)
    clock[0] += timedelta(seconds=900)

    service.schedule_hydration(USER)
    await service.drain()

    fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_transactional_claims_prevent_duplicate_es_from_same_user_tasks(
    store, monkeypatch, clock
):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    hydrate = AsyncMock(return_value=[])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    service = _service(store, monkeypatch, background=True)

    for _ in range(5):
        service.schedule_hydration(USER)
    await service.drain()

    hydrate.assert_awaited_once()


@pytest.mark.asyncio
async def test_reader_sees_newly_saved_features_within_the_same_request(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    history_module.set_request_less_history_service(service)

    async with request_cache_scope():
        before = await history_module.fetch_request_less_history_features(USER)
        await _save_features(service)
        store.reads.clear()
        after = await history_module.fetch_request_less_history_features(USER)

    assert [item.at_uri for item in before.items] == [URI]
    assert before.items_with_embeddings == []
    assert [item.at_uri for item in after.items_with_embeddings] == [URI]
    assert after.items_with_embeddings[0].embedding == VECTOR
    assert len(store.reads) == 1


@pytest.mark.asyncio
async def test_unconfigured_service_returns_empty_history():
    assert history_module.get_request_less_history_service() is None
    assert (await history_module.fetch_request_less_history_features(USER)).items == []


@pytest.mark.asyncio
async def test_firestore_read_failure_returns_empty_without_es_fallback(store, monkeypatch, clock):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    store.read_error = RuntimeError("Firestore unavailable")
    hydrate = AsyncMock()
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    service = _service(store, monkeypatch, background=True)

    assert (await service.retrieve(USER)).items == []
    await service.drain()

    hydrate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("schema_version", 999), ("embedding_key", "other-model")])
async def test_incompatible_documents_are_not_served_rehydrated_or_overwritten(
    store,
    monkeypatch,
    clock,
    field,
    value,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    store.data()[field] = value
    previous = deepcopy(store.data())
    cast(MagicMock, service.schedule_hydration).reset_mock()

    assert (await service.retrieve(USER)).items == []
    cast(MagicMock, service.schedule_hydration).assert_not_called()
    with pytest.raises(ValueError):
        await _record(service, USER, {OTHER_URI: NOW})
    with pytest.raises(ValueError):
        await service._claim_due(USER)

    assert store.data() == previous


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corrupt_blob",
    [
        pytest.param("not base64!", id="invalid-base64"),
        pytest.param("AQ==", id="invalid-float-byte-length"),
        pytest.param(encode_float32_b64([1.0]), id="wrong-dimension"),
        pytest.param(
            encode_float32_b64([float("nan"), *([0.0] * (MINILM_L12_EMBEDDING_DIM - 1))]),
            id="nonfinite-vector",
        ),
    ],
)
async def test_corrupt_saved_embedding_preserves_other_features_and_stored_history(
    store, monkeypatch, clock, caplog, corrupt_blob,
):
    oldest_uri = "at://post/oldest"
    service = _service(store, monkeypatch)
    await _record(service, USER, {
        URI: NOW + timedelta(seconds=2),
        OTHER_URI: NOW + timedelta(seconds=1),
        oldest_uri: NOW,
    })
    claims = await service._claim_due(USER)
    await service._store_results(USER, claims, {
        URI: FEATURES, OTHER_URI: FEATURES, oldest_uri: FEATURES,
    })
    stored_bad = next(item for item in store.data()["items"] if item["at_uri"] == OTHER_URI)
    stored_bad["embedding_b64"] = corrupt_blob
    before = deepcopy(store.data())
    cast(MagicMock, service.schedule_hydration).reset_mock()

    with caplog.at_level("WARNING", logger=history_module.__name__):
        history = await service.retrieve(USER)

    assert [item.at_uri for item in history.items] == [URI, OTHER_URI, oldest_uri]
    assert [item.at_uri for item in history.items_with_embeddings] == [URI, oldest_uri]
    assert all(item.embedding == VECTOR for item in history.items_with_embeddings)
    bad = history.items[1]
    assert bad.embedding is None
    assert bad.received_at == NOW + timedelta(seconds=1)
    assert bad.author_did == "did:plc:author"
    assert bad.like_count == 7
    assert store.data() == before
    cast(MagicMock, service.schedule_hydration).assert_not_called()
    warnings = [record for record in caplog.records if record.name == history_module.__name__]
    assert len(warnings) == 1
    assert warnings[0].levelname == "WARNING"
    assert "embedding" in warnings[0].getMessage().lower()


@pytest.mark.asyncio
async def test_pending_history_still_hydrates_without_retrying_a_corrupt_saved_embedding(
    store, monkeypatch, clock,
):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    await _save_features(seed)
    store.data()["items"][0]["embedding_b64"] = "not base64!"
    corrupt_item = deepcopy(store.data()["items"][0])
    await _record(seed, USER, {OTHER_URI: NOW + timedelta(seconds=1)})
    fetch = AsyncMock(return_value=[(OTHER_URI, *FEATURES)])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)

    before = await service.retrieve(USER)
    await service.drain()
    after = await service.retrieve(USER)

    assert [item.at_uri for item in before.items] == [OTHER_URI, URI]
    assert before.items_with_embeddings == []
    assert [item.at_uri for item in after.items_with_embeddings] == [OTHER_URI]
    assert after.items_with_embeddings[0].embedding == VECTOR
    fetch.assert_awaited_once()
    assert fetch.await_args is not None
    assert fetch.await_args.args[1] == [OTHER_URI]
    assert next(item for item in store.data()["items"] if item["at_uri"] == URI) == corrupt_item
    clock[0] += timedelta(days=90)
    assert await service._claim_due(USER) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", [[1.0], [float("nan")] * 384, [float("inf")] * 384])
async def test_invalid_embedding_is_a_retryable_failed_attempt(store, monkeypatch, clock, vector):
    fetch = AsyncMock(return_value=[(URI, vector, "did:plc:author", 7)])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)

    await _record(service, USER, {URI: NOW})
    await service.drain()
    assert (await service.retrieve(USER)).items_with_embeddings == []
    assert await service._claim_due(USER) == []
    clock[0] += timedelta(seconds=900)

    assert (await service._claim_due(USER))[0].attempt_count == 2


def _metric_total(collector, name, **labels):
    return sum(
        record.args[1]
        for record in collector.record.call_args_list
        if record.args[0] == f"request_less.hydration.{name}"
        and all(record.kwargs.get(key) == value for key, value in labels.items())
    )


@pytest.mark.asyncio
async def test_transaction_replays_do_not_duplicate_attempt_or_success_metrics(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    store.replay_every = True
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES)]),
    )

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="1") == 1
    assert _metric_total(collector, "success_count", attempt="1") == 1
    assert _metric_total(collector, "failure_count") == 0
    assert store.data()["items"][0]["attempt_count"] == 1


@pytest.mark.asyncio
async def test_hydration_metrics_count_each_post_with_its_attempt_number(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    await service._claim_due(USER)
    clock[0] += timedelta(seconds=900)
    await _record(service, USER, {OTHER_URI: clock[0]})
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES)]),
    )

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="2") == 1
    assert _metric_total(collector, "attempt_count", attempt="1") == 1
    assert _metric_total(collector, "success_count", attempt="2") == 1
    assert _metric_total(collector, "failure_count", attempt="1", reason="missing_features") == 1
    assert _metric_total(collector, "exhausted_count") == 0


@pytest.mark.asyncio
async def test_third_failed_attempt_emits_exhaustion_once_and_duplicate_feedback_does_not_reset(
    store,
    monkeypatch,
    clock,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    store.replay_every = True

    for _ in range(3):
        await service._hydrate(USER)
        clock[0] += timedelta(seconds=900)
    await _record(service, USER, {URI: clock[0]})
    clock[0] += timedelta(days=180)
    await service._hydrate(USER)

    assert fetch.await_count == 3
    assert _metric_total(collector, "attempt_count") == 3
    assert _metric_total(collector, "failure_count", reason="missing_features") == 3
    assert _metric_total(collector, "exhausted_count") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,reason",
    [
        (RuntimeError("ES connection failed"), "es_error"),
        (TimeoutError("ES timed out"), "timeout"),
        (ConnectionTimeout("ES transport timed out"), "timeout"),
    ],
)
async def test_transient_failure_metrics_count_every_affected_post(
    store,
    monkeypatch,
    clock,
    failure,
    reason,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW, OTHER_URI: NOW})
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module, "fetch_post_embeddings_and_metadata", AsyncMock(side_effect=failure)
    )

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="1") == 2
    assert _metric_total(collector, "failure_count", attempt="1", reason=reason) == 2
    assert _metric_total(collector, "success_count") == 0


@pytest.mark.asyncio
async def test_failed_result_write_does_not_refund_attempt_or_lose_pending_uri(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES)]),
    )
    monkeypatch.setattr(
        service, "_store_results", AsyncMock(side_effect=RuntimeError("write unavailable"))
    )

    await service._hydrate(USER)

    assert store.data()["items"][0]["attempt_count"] == 1
    assert (await service.retrieve(USER)).items[0].at_uri == URI
    assert (await service.retrieve(USER)).items_with_embeddings == []
    assert await service._claim_due(USER) == []
    assert _metric_total(collector, "failure_count", reason="write_error") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 3])
async def test_cancellation_keeps_claim_consumed_and_records_outcome(
    store,
    monkeypatch,
    clock,
    attempt,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    for _ in range(attempt - 1):
        await service._claim_due(USER)
        clock[0] += timedelta(seconds=900)
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    started = asyncio.Event()

    async def hydrate(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)
    task = asyncio.create_task(service._hydrate(USER))
    await asyncio.wait_for(started.wait(), timeout=0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert _metric_total(collector, "attempt_count", attempt=str(attempt)) == 1
    assert _metric_total(collector, "failure_count", attempt=str(attempt), reason="cancelled") == 1
    assert _metric_total(collector, "exhausted_count") == (1 if attempt == 3 else 0)
    assert await service._claim_due(USER) == []
    clock[0] += timedelta(seconds=900)
    if attempt == 1:
        assert (await service._claim_due(USER))[0].attempt_count == 2
    else:
        assert await service._claim_due(USER) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["missing", "invalid", "exception"])
async def test_eviction_during_last_attempt_suppresses_failure_and_exhaustion_metrics(
    store,
    monkeypatch,
    clock,
    outcome,
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    for _ in range(2):
        await service._claim_due(USER)
        clock[0] += timedelta(seconds=900)
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)

    async def hydrate(*args, **kwargs):
        await _record(
            service,
            USER,
            {
                f"at://post/{index}": clock[0] + timedelta(seconds=index)
                for index in range(history_module.USER_HISTORY_LIMIT)
            },
        )
        if outcome == "exception":
            raise RuntimeError("ES failed after history was pruned")
        if outcome == "invalid":
            return [(URI, [float("nan")] * 384, "did:plc:author", 7)]
        return []

    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="3") == 1
    assert _metric_total(collector, "failure_count") == 0
    assert _metric_total(collector, "exhausted_count") == 0


@pytest.mark.asyncio
async def test_each_attempt_uses_fresh_request_cache_for_real_es_work(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    computations = 0

    async def hydrate(*args, **kwargs):
        async def compute():
            nonlocal computations
            computations += 1
            return [] if computations == 1 else [(URI, *FEATURES)]

        cache = get_request_cache()
        assert cache is not None
        return await cache.get_or_compute(("hydration", URI), compute)

    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)

    async with request_cache_scope():
        await service._hydrate(USER)
        clock[0] += timedelta(seconds=900)
        await service._hydrate(USER)

    assert computations == 2
    assert [item.at_uri for item in (await service.retrieve(USER)).items_with_embeddings] == [URI]


@pytest.mark.asyncio
async def test_hydration_budget_cancels_stalled_es_and_consumes_one_attempt(
    store, monkeypatch, clock
):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    cancelled = asyncio.Event()
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(history_module, "HYDRATION_TIMEOUT_SECONDS", 0.01)

    async def hydrate(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", hydrate)

    await asyncio.wait_for(service._hydrate(USER), timeout=0.2)

    assert cancelled.is_set()
    assert store.data()["items"][0]["attempt_count"] == 1
    assert _metric_total(collector, "failure_count", attempt="1", reason="timeout") == 1


@pytest.mark.asyncio
async def test_real_metric_collector_exports_per_post_counters_and_request_labels(
    store,
    monkeypatch,
    clock,
):
    from opentelemetry.sdk.metrics._internal.point import Sum
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    from .metrics import MetricCollector
    from .request_context import reset_endpoint, reset_traffic, set_endpoint, set_traffic

    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW, OTHER_URI: NOW})
    store.replay_every = True
    reader = InMemoryMetricReader()
    collector = MetricCollector._from_reader(reader, service_name="request-less-test", env="test")
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES)]),
    )
    endpoint_token = set_endpoint("send_interactions")
    traffic_token = set_traffic("real")
    try:
        for _ in range(3):
            service = _service(store, monkeypatch, background=True)
            service.schedule_hydration(USER)
            await service.drain()
            clock[0] += timedelta(seconds=900)
        exported = reader.get_metrics_data()
        assert exported is not None
        counters = {}
        for resource in exported.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    assert isinstance(metric.data, Sum)
                    counters[metric.name] = metric.data.data_points

        assert {
            name.removeprefix("request_less.hydration."): sum(point.value for point in points)
            for name, points in counters.items()
        } == {"attempt_count": 4, "success_count": 1, "failure_count": 3, "exhausted_count": 1}
        attempt_points = counters["request_less.hydration.attempt_count"]
        assert {point.attributes["attempt"]: point.value for point in attempt_points} == {
            "1": 2,
            "2": 1,
            "3": 1,
        }
        for points in counters.values():
            for point in points:
                assert point.attributes["endpoint"] == "send_interactions"
                assert point.attributes["traffic"] == "real"
                assert set(point.attributes) <= {"attempt", "reason", "endpoint", "traffic"}
    finally:
        reset_endpoint(endpoint_token)
        reset_traffic(traffic_token)
        await collector.shutdown()


@pytest.mark.asyncio
async def test_transaction_deadline_prevents_unclaimed_es_attempts(store, monkeypatch, clock):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    cancelled = asyncio.Event()

    async def hanging_transaction(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    run = AsyncMock(side_effect=hanging_transaction)
    monkeypatch.setattr(history_module, "async_transactional", lambda body: run)
    monkeypatch.setattr(history_module, "USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS", 0.01)
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    fetch = AsyncMock()
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = history_module.RequestLessHistoryService(store.db, object())

    await asyncio.wait_for(service._hydrate(USER), timeout=0.2)

    run.assert_awaited_once()
    assert cancelled.is_set()
    fetch.assert_not_awaited()
    assert store.data()["items"][0]["attempt_count"] == 0
    assert _metric_total(collector, "attempt_count") == 0
    assert _metric_total(collector, "failure_count") == 0


@pytest.mark.asyncio
async def test_read_deadline_returns_empty_without_hydration_or_attempt_metrics(
    store, monkeypatch, clock
):
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    cancelled = asyncio.Event()

    async def hanging_read(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(history_module, "USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS", 0.01)
    fetch = AsyncMock()
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)
    ref = MagicMock(get=AsyncMock(side_effect=hanging_read))
    monkeypatch.setattr(service, "_document", lambda user_did: ref)

    result = await asyncio.wait_for(service.retrieve(USER), timeout=0.2)
    await service.drain()

    assert result.items == []
    assert cancelled.is_set()
    fetch.assert_not_awaited()
    assert store.data()["items"][0]["attempt_count"] == 0
    assert _metric_total(collector, "attempt_count") == 0


@pytest.mark.asyncio
async def test_partial_batch_save_failure_preserves_each_posts_original_failure_reason(
    store,
    monkeypatch,
    clock,
):
    missing_uri = "at://post/missing"
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW, OTHER_URI: NOW, missing_uri: NOW})
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES), (OTHER_URI, [1.0], "did:plc:author", 7)]),
    )
    monkeypatch.setattr(
        service, "_store_results", AsyncMock(side_effect=RuntimeError("write unavailable"))
    )

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="1") == 3
    assert _metric_total(collector, "failure_count", reason="write_error") == 1
    assert _metric_total(collector, "failure_count", reason="missing_features") == 1
    assert _metric_total(collector, "failure_count", reason="invalid_features") == 1
    assert _metric_total(collector, "failure_count") == 3
    assert _metric_total(collector, "success_count") == 0
    assert {item["attempt_count"] for item in store.data()["items"]} == {1}


@pytest.mark.asyncio
async def test_new_feedback_hydrates_while_an_earlier_post_is_still_in_flight(
    store, monkeypatch, clock
):
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release_first = asyncio.Event()

    async def hydrate(es, uris, **kwargs):
        if uris == [URI]:
            first_started.set()
            await release_first.wait()
            return [(URI, *FEATURES)]
        assert uris == [OTHER_URI]
        second_started.set()
        return [(OTHER_URI, *FEATURES)]

    fetch = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)
    try:
        await _record(service, USER, {URI: NOW})
        await asyncio.wait_for(first_started.wait(), timeout=0.2)
        await _record(service, USER, {OTHER_URI: NOW + timedelta(seconds=1)})
        await asyncio.wait_for(second_started.wait(), timeout=0.2)
        history = await service.retrieve(USER)
        assert [item.at_uri for item in history.items_with_embeddings] == [OTHER_URI]
        assert not release_first.is_set()
    finally:
        release_first.set()
        await service.drain()

    history = await service.retrieve(USER)
    assert [item.at_uri for item in history.items_with_embeddings] == [OTHER_URI, URI]
    assert fetch.await_count == 2
    assert {item["attempt_count"] for item in store.data()["items"]} == {1}


@pytest.mark.asyncio
async def test_metric_collector_failure_does_not_interrupt_hydration(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    collector = MagicMock()
    collector.record.side_effect = RuntimeError("metric exporter unavailable")
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    fetch = AsyncMock(return_value=[(URI, *FEATURES)])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)

    await service._hydrate(USER)

    fetch.assert_awaited_once()
    assert collector.record.call_count == 2
    assert [item.at_uri for item in (await service.retrieve(USER)).items_with_embeddings] == [URI]
    assert store.data()["items"][0]["attempt_count"] == 1


@pytest.mark.asyncio
async def test_concurrent_record_merges_keep_latest_receipts_and_prune_the_combined_history(
    store,
    monkeypatch,
    clock,
):
    first = _service(store, monkeypatch)
    second = _service(store, monkeypatch)
    limit = history_module.USER_HISTORY_LIMIT
    first_batch = {f"at://post/{index}": NOW + timedelta(seconds=index) for index in range(limit)}
    second_batch = {
        f"at://post/{index}": NOW + timedelta(seconds=index) for index in range(limit, limit * 2)
    }
    second_batch["at://post/0"] = NOW + timedelta(hours=1)

    await asyncio.gather(
        first.record(USER, first_batch),
        second.record(USER, second_batch),
    )

    history = await first.retrieve(USER)
    assert [item.at_uri for item in history.items] == [
        "at://post/0",
        *[f"at://post/{index}" for index in range(limit * 2 - 1, limit, -1)],
    ]
    assert history.items[0].received_at == NOW + timedelta(hours=1)
    assert len(history.items) == limit


@pytest.mark.asyncio
async def test_successful_third_attempt_does_not_emit_exhaustion(store, monkeypatch, clock):
    service = _service(store, monkeypatch)
    await _record(service, USER, {URI: NOW})
    for _ in range(2):
        await service._claim_due(USER)
        clock[0] += timedelta(seconds=900)
    collector = MagicMock()
    monkeypatch.setattr(history_module, "get_metric_collector", lambda: collector)
    monkeypatch.setattr(
        history_module,
        "fetch_post_embeddings_and_metadata",
        AsyncMock(return_value=[(URI, *FEATURES)]),
    )

    await service._hydrate(USER)

    assert _metric_total(collector, "attempt_count", attempt="3") == 1
    assert _metric_total(collector, "success_count", attempt="3") == 1
    assert _metric_total(collector, "failure_count") == 0
    assert _metric_total(collector, "exhausted_count") == 0
    assert [item.at_uri for item in (await service.retrieve(USER)).items_with_embeddings] == [URI]


@pytest.mark.asyncio
async def test_background_task_rechecks_claim_after_reading_a_stale_due_snapshot(
    store, monkeypatch, clock
):
    competing_service = _service(store, monkeypatch)
    await _record(competing_service, USER, {URI: NOW})
    reader = _service(store, monkeypatch, background=True)
    fetch = AsyncMock()
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)

    snapshot = await reader.retrieve(USER)
    claims = await competing_service._claim_due(USER)
    await reader.drain()

    assert [item.at_uri for item in snapshot.items] == [URI]
    assert len(claims) == 1
    fetch.assert_not_awaited()
    assert store.data()["items"][0]["attempt_count"] == 1


@pytest.mark.asyncio
async def test_retrieve_only_reads_and_hydrates_the_requested_user(store, monkeypatch, clock):
    other_user = "did:plc:another-user"
    seed = _service(store, monkeypatch)
    await _record(seed, USER, {URI: NOW})
    await _record(seed, other_user, {OTHER_URI: NOW})
    other_history = deepcopy(store.data(other_user))
    store.reads.clear()
    fetch = AsyncMock(return_value=[(URI, *FEATURES)])
    monkeypatch.setattr(history_module, "fetch_post_embeddings_and_metadata", fetch)
    service = _service(store, monkeypatch, background=True)

    history = await service.retrieve(USER)
    await service.drain()

    assert [item.at_uri for item in history.items] == [URI]
    fetch.assert_awaited_once()
    assert fetch.await_args is not None
    assert fetch.await_args.args[1] == [URI]
    assert store.data(other_user) == other_history
    key = hashlib.sha256(USER.encode()).hexdigest()
    assert set(store.reads) == {f"{history_module.HISTORY_COLLECTION}/{key}"}

"""Heavy ranking consumes saved requestLess features while hydration runs separately."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from ...models import CandidatePost
from .. import request_less_history as history_module
from ..embeddings import (
    MINILM_L12_EMBEDDING_DIM,
    MINILM_L12_EMBEDDING_KEY,
    encode_float32_b64,
)
from ..user_history_cache import UserHistory, UserHistoryItem
from . import heavy_ranker as heavy_module
from .heavy_ranker import HeavyRanker

USER = "did:plc:request-less-integration"
RECEIVED_AT = datetime(2026, 10, 26, 12, tzinfo=UTC)
VECTOR = [0.25] * MINILM_L12_EMBEDDING_DIM
POSITIVE_AUTHOR = "did:plc:liked-author"
NEGATIVE_AUTHOR = "did:plc:request-less-author"


@pytest.fixture
def inference(monkeypatch):
    monkeypatch.setattr(heavy_module, "get_inference_settings", lambda: ("http://model", "key"))
    monkeypatch.setattr(heavy_module, "REQUEST_LESS_COEFFICIENT", 0.75)
    monkeypatch.setattr(heavy_module, "request_less_heavy_ranker_enabled", lambda *_: True)
    monkeypatch.setattr(heavy_module, "get_posthog_client", lambda: None)
    monkeypatch.setattr(heavy_module, "get_traffic", lambda: "real")
    monkeypatch.setattr(heavy_module, "get_metric_collector", lambda: None)
    monkeypatch.setattr(
        heavy_module,
        "fetch_user_history_features",
        AsyncMock(return_value=UserHistory([
            UserHistoryItem("at://liked", RECEIVED_AT.isoformat(), VECTOR, POSITIVE_AUTHOR, 5),
        ])),
    )

    async def predict(_embeddings, authors, *_args, **_kwargs):
        if authors == [POSITIVE_AUTHOR]:
            return [0.8]
        assert authors == [NEGATIVE_AUTHOR]
        return [0.4]

    mock = AsyncMock(side_effect=predict)
    monkeypatch.setattr(heavy_module, "predict_heavy_ranker_single_user", mock)
    return mock


def _item(kind):
    row = {"at_uri": f"at://request-less/{kind}", "received_at": RECEIVED_AT}
    if kind == "exhausted":
        row["attempt_count"] = history_module.MAX_HYDRATION_ATTEMPTS
    elif kind == "corrupt":
        row["embedding_b64"] = "invalid-base64"
    elif kind == "ready":
        row.update(
            embedding_b64=encode_float32_b64(VECTOR),
            author_did=NEGATIVE_AUTHOR,
            like_count=7,
        )
    return row


def _service(monkeypatch, items):
    data = {
        "schema_version": history_module.SCHEMA_VERSION,
        "user_did": USER,
        "embedding_key": MINILM_L12_EMBEDDING_KEY,
        "items": items,
    }
    snapshot = MagicMock(exists=True)
    snapshot.to_dict.side_effect = lambda: deepcopy(data)
    db = MagicMock()
    read = AsyncMock(return_value=snapshot)
    db.collection.return_value.document.return_value.get = read
    service = history_module.RequestLessHistoryService(db, object())
    monkeypatch.setattr(history_module, "_service", service)
    return service, data, read


async def _rank():
    return await HeavyRanker("heavy_ranker", "actual").predict(
        None,
        USER,
        [CandidatePost(
            at_uri="at://candidate",
            author_did="did:plc:candidate-author",
            minilm_l12_embedding=encode_float32_b64(VECTOR),
            like_count=3,
        )],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "expected_score", "schedules_hydration"),
    [
        ("pending", 0.8, True),
        ("exhausted", 0.8, False),
        ("corrupt", 0.8, False),
        ("invalid_document", 0.8, False),
        ("partial", 0.5, True),
    ],
)
async def test_rank_uses_only_valid_saved_features(
    monkeypatch, inference, kind, expected_score, schedules_hydration
):
    items = (
        [_item(state) for state in ["pending", "ready", "exhausted", "corrupt"]]
        if kind == "partial" else [_item(kind)]
    )
    service, data, read = _service(monkeypatch, items)
    if kind == "invalid_document":
        data["schema_version"] = -1
    hydrate = AsyncMock()
    monkeypatch.setattr(service, "_hydrate", hydrate)

    try:
        result = await _rank()
    finally:
        await service.drain()

    assert result.result.rankings[0].rank_score == pytest.approx(expected_score)
    read.assert_awaited_once_with()
    if schedules_hydration:
        hydrate.assert_awaited_once_with(USER)
    else:
        hydrate.assert_not_awaited()
    negative_calls = [
        call for call in inference.await_args_list if call.args[1] != [POSITIVE_AUTHOR]
    ]
    if kind == "partial":
        assert len(negative_calls) == 1
        assert negative_calls[0].args[:4] == (
            [VECTOR], [NEGATIVE_AUTHOR], [RECEIVED_AT.isoformat()], [7],
        )
    else:
        assert negative_calls == []


@pytest.mark.asyncio
async def test_hydration_changes_the_next_rank_without_delaying_the_current_one(
    monkeypatch, inference
):
    service, data, read = _service(monkeypatch, [_item("pending")])
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def hydrate(user_did):
        assert user_did == USER
        started.set()
        await release.wait()
        data["items"] = [{**_item("ready"), "at_uri": "at://request-less/pending"}]
        finished.set()

    worker = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(service, "_hydrate", worker)
    try:
        first = await asyncio.wait_for(_rank(), timeout=1)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert first.result.rankings[0].rank_score == pytest.approx(0.8)
        assert not finished.is_set()
        assert inference.await_count == 1

        release.set()
        await asyncio.wait_for(finished.wait(), timeout=1)
        second = await _rank()
        assert second.result.rankings[0].rank_score == pytest.approx(0.5)
        assert inference.await_count == 3
        assert read.await_count == 2
        worker.assert_awaited_once_with(USER)
    finally:
        release.set()
        await service.drain()


@pytest.mark.asyncio
async def test_rank_cancellation_leaves_service_hydration_for_shutdown_to_drain(
    monkeypatch, inference
):
    service, _, _ = _service(monkeypatch, [_item("pending")])
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    positive_started = asyncio.Event()
    positive_release = asyncio.Event()

    async def hydrate(_user_did):
        started.set()
        await release.wait()
        finished.set()

    async def positive_inference(*_args, **_kwargs):
        positive_started.set()
        await positive_release.wait()
        return [0.8]

    monkeypatch.setattr(service, "_hydrate", hydrate)
    inference.side_effect = positive_inference
    ranking = asyncio.create_task(_rank())
    try:
        await asyncio.wait_for(asyncio.gather(started.wait(), positive_started.wait()), timeout=1)
        ranking.cancel()
        with pytest.raises(asyncio.CancelledError):
            await ranking

        assert len(service._tasks) == 1
        assert not next(iter(service._tasks)).cancelled()
        draining = asyncio.create_task(service.drain())
        await asyncio.sleep(0)
        assert not draining.done()
        release.set()
        await asyncio.wait_for(draining, timeout=1)
        assert finished.is_set()
        assert service._tasks == set()
    finally:
        release.set()
        positive_release.set()
        ranking.cancel()
        await asyncio.gather(ranking, return_exceptions=True)
        await service.drain()


@pytest.mark.asyncio
async def test_drain_removes_completed_hydration_before_done_callbacks_run(monkeypatch):
    service, _, _ = _service(monkeypatch, [])
    monkeypatch.setattr(service, "_hydrate", AsyncMock())
    service.schedule_hydration(USER)
    # The worker finishes on this turn; its cleanup callback is still queued.
    await asyncio.sleep(0)
    assert len(service._tasks) == 1
    assert next(iter(service._tasks)).done()

    await service.drain()

    assert service._tasks == set()

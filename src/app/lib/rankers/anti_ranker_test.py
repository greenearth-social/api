"""Tests for requestLess history ranking."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from ...models import CandidatePost, RankModelSpec, RankPredictRequest
from .. import user_history_cache
from ..embeddings import encode_float32_b64
from . import anti_ranker as anti_ranker_module
from .anti_ranker import AntiRanker, set_anti_ranker_db
from .predict import run_predict


def _time(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=UTC)


def _configure_db(data: dict | None):
    db = MagicMock()
    snapshot = MagicMock()
    snapshot.exists = data is not None
    snapshot.to_dict.return_value = data
    db.collection.return_value.document.return_value.get = AsyncMock(return_value=snapshot)
    set_anti_ranker_db(db)
    return db


@pytest.fixture(autouse=True)
def reset_db():
    set_anti_ranker_db(None)
    yield
    set_anti_ranker_db(None)


@pytest.fixture
def inference(monkeypatch):
    predict = AsyncMock(return_value=[0.2, 0.9])
    settings = MagicMock(return_value=("https://example.com", "secret"))
    monkeypatch.setattr(anti_ranker_module, "predict_heavy_ranker_single_user", predict)
    monkeypatch.setattr(anti_ranker_module, "get_inference_settings", settings)
    return predict, settings


@pytest.mark.asyncio
async def test_reads_firestore_and_aligns_recent_history_with_candidate_scores(
    monkeypatch, inference
):
    db = _configure_db({
        "user_did": "did:plc:user1",
        "request_less_items": {
            "at://history/old": _time(1),
            "at://history/missing": _time(2),
            "at://history/new": _time(3),
        },
    })
    # RequestLess history must work without involving the positive-history cache.
    monkeypatch.setattr(
        user_history_cache,
        "get_user_history_cache",
        MagicMock(side_effect=AssertionError("requestLess should read Firestore directly")),
    )
    recorder = MagicMock()
    monkeypatch.setattr(anti_ranker_module, "current_recorder", lambda: recorder)
    es = object()

    async def fetch_metadata(client, uris, index="posts"):
        assert client is es
        if index == "posts":
            assert uris == ["at://history/new", "at://history/missing", "at://history/old"]
            return [
                ("at://history/new", [0.0, 1.0], "did:plc:new", 7),
                ("at://history/old", [1.0, 0.0], "", 0),
            ]
        assert index == "posts_recent"
        assert uris == ["at://post/fetched", "at://post/missing"]
        return [("at://post/fetched", [0.0, 1.0], "did:plc:fetched", 8)]

    fetch = AsyncMock(side_effect=fetch_metadata)
    monkeypatch.setattr(anti_ranker_module, "fetch_post_embeddings_and_metadata", fetch)
    result = await AntiRanker(name="anti_ranker").predict(
        es,
        "did:plc:user1",
        [
            CandidatePost(
                at_uri="at://post/embedded",
                minilm_l12_embedding=encode_float32_b64([1.0, 0.0]),
                author_did="did:plc:embedded",
                like_count=12,
            ),
            CandidatePost(at_uri="at://post/fetched"),
            CandidatePost(at_uri="at://post/missing"),
        ],
    )

    db.collection.assert_called_once_with("users")
    db.collection.return_value.document.assert_called_once_with("user1")
    db.collection.return_value.document.return_value.get.assert_awaited_once_with()
    inference[0].assert_awaited_once_with(
        [[0.0, 1.0], [1.0, 0.0]],
        ["did:plc:new", ""],
        ["2026-01-01T03:00:00Z", "2026-01-01T01:00:00Z"],
        [7, 0],
        [[1.0, 0.0], [0.0, 1.0]],
        ["did:plc:embedded", "did:plc:fetched"],
        [12, 8],
        base_url="https://example.com",
        api_key="secret",
    )
    recorder.record_user_features.assert_called_once_with(
        "anti_ranker", ["at://history/new", "at://history/missing", "at://history/old"], 2
    )
    assert result.model == "anti_ranker"
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/fetched", "rank": 1, "rank_score": 0.9},
        {"at_uri": "at://post/embedded", "rank": 2, "rank_score": 0.2},
        {"at_uri": "at://post/missing", "rank": 3, "rank_score": None},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("history_state", [
    "no_db", "missing_user", "legacy_user", "no_history", "no_embeddings",
])
async def test_no_usable_history_returns_empty_without_inference(
    monkeypatch, inference, history_state
):
    if history_state == "missing_user":
        _configure_db(None)
    elif history_state != "no_db":
        data: dict = {"user_did": "did:plc:user1"}
        if history_state == "no_history":
            data["request_less_items"] = {}
        elif history_state == "no_embeddings":
            data["request_less_items"] = {"at://history/missing": _time(1)}
        _configure_db(data)
    fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(anti_ranker_module, "fetch_post_embeddings_and_metadata", fetch)

    result = await AntiRanker(name="anti_ranker").predict(
        None, "did:plc:user1", [CandidatePost(at_uri="at://post/1")]
    )

    assert result.result.rankings == []
    inference[0].assert_not_awaited()
    inference[1].assert_not_called()
    if history_state != "no_embeddings":
        fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_candidate_embeddings_skips_inference(monkeypatch, inference):
    _configure_db({
        "user_did": "did:plc:user1",
        "request_less_items": {"at://history/1": _time(1)},
    })

    async def fetch_metadata(es, uris, index="posts"):
        if index == "posts":
            return [("at://history/1", [1.0, 0.0], "did:plc:history", 1)]
        return []

    monkeypatch.setattr(anti_ranker_module, "fetch_post_embeddings_and_metadata", fetch_metadata)
    result = await AntiRanker(name="anti_ranker").predict(
        None, "did:plc:user1", [CandidatePost(at_uri="at://post/missing")]
    )

    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/missing", "rank": 1, "rank_score": None},
    ]
    inference[0].assert_not_awaited()
    inference[1].assert_not_called()


@pytest.mark.asyncio
async def test_anti_only_pipeline_returns_empty_without_history(inference):
    _configure_db({"user_did": "did:plc:user1", "request_less_items": {}})

    result = await run_predict(
        RankPredictRequest(
            user_did="did:plc:user1",
            candidates=[CandidatePost(at_uri="at://post/1")],
            models=[RankModelSpec(name="anti_ranker", weight=1.0)],
        ),
        es=None,
    )

    assert result.rankings == []
    inference[0].assert_not_awaited()

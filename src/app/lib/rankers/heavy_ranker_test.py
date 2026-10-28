"""Tests for the heavy ranker."""

import asyncio
import threading
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from ...models import CandidatePost
from ..embeddings import encode_float32_b64
from ..inference import InferenceResponseFormatError
from ..request_context import reset_endpoint, reset_traffic, set_endpoint, set_traffic
from ..request_less_history import RequestLessHistory, RequestLessHistoryItem
from ..user_history_cache import UserHistory, UserHistoryItem
from . import get_ranker
from . import heavy_ranker as heavy_ranker_module
from .heavy_ranker import HeavyRanker


def _time(hour: int) -> str:
    return datetime(2026, 1, 1, hour, tzinfo=UTC).isoformat()


def test_empty_history_variant_is_registered_with_empty_history_mode():
    ranker = get_ranker("heavy_ranker_empty_history")

    assert isinstance(ranker, HeavyRanker)
    assert ranker.name == "heavy_ranker_empty_history"
    assert ranker.history_mode == "empty"


def test_predict_requires_inference_env_vars(monkeypatch):
    monkeypatch.delenv("GE_INFERENCE_BASE_URL", raising=False)
    monkeypatch.delenv("GE_INFERENCE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="GE_INFERENCE_BASE_URL"):
        asyncio.run(
            HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
                es=None,
                user_did="did:plc:user1",
                candidates=[CandidatePost(at_uri="at://post/1")],
            )
        )


def test_predict_filters_history_times_to_embeddable_likes_and_ranks_candidates(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    liked_times = [_time(1), _time(2), _time(3)]
    seen = {}

    async def fake_fetch_user_history_features(es, user_did):
        assert user_did == "did:plc:user1"
        return UserHistory(
            items=[
                UserHistoryItem(
                    "at://liked/a",
                    liked_times[0],
                    [1.0, 0.0],
                    "did:plc:liked-a",
                    5,
                ),
                UserHistoryItem("at://liked/b", liked_times[1], None),
                UserHistoryItem(
                    "at://liked/c",
                    liked_times[2],
                    [0.0, 1.0],
                    "did:plc:liked-c",
                    7,
                ),
            ]
        )

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        seen.setdefault("fetch_calls", []).append((list(at_uris), index))
        if at_uris == ["at://post/a", "at://post/b", "at://post/missing"]:
            return [
                ("at://post/b", [0.0, 1.0], "did:plc:b", 20),
                ("at://post/a", [1.0, 0.0], "did:plc:a", 10),
            ]
        raise AssertionError(f"unexpected at_uris: {at_uris}")

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        history_like_counts,
        candidate_post_embeddings,
        candidate_author_dids,
        candidate_like_counts,
        *,
        base_url,
        api_key,
    ):
        seen["ranker_call"] = {
            "history_embeddings": history_embeddings,
            "history_author_dids": history_author_dids,
            "history_liked_at_times": history_liked_at_times,
            "history_like_counts": history_like_counts,
            "candidate_post_embeddings": candidate_post_embeddings,
            "candidate_author_dids": candidate_author_dids,
            "candidate_like_counts": candidate_like_counts,
            "base_url": base_url,
            "api_key": api_key,
        }
        return [0.2, 0.9]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fake_fetch_user_history_features,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(at_uri="at://post/a"),
                CandidatePost(at_uri="at://post/b"),
                CandidatePost(at_uri="at://post/missing"),
            ],
        )
    )

    assert seen["fetch_calls"] == [
        (["at://post/a", "at://post/b", "at://post/missing"], "posts_recent"),
    ]
    assert seen["ranker_call"] == {
        "history_embeddings": [[1.0, 0.0], [0.0, 1.0]],
        "history_author_dids": ["did:plc:liked-a", "did:plc:liked-c"],
        "history_liked_at_times": [_time(1), _time(3)],
        "history_like_counts": [5, 7],
        "candidate_post_embeddings": [[0.0, 1.0], [1.0, 0.0]],
        "candidate_author_dids": ["did:plc:b", "did:plc:a"],
        "candidate_like_counts": [20, 10],
        "base_url": "https://example.com",
        "api_key": "secret",
    }
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": 0.9},
        {"at_uri": "at://post/b", "rank": 2, "rank_score": 0.2},
        {"at_uri": "at://post/missing", "rank": 3, "rank_score": None},
    ]


def test_predict_uses_embedded_candidate_features_and_fetches_missing_candidates(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    seen = {}

    async def fake_fetch_user_history_features(es, user_did):
        return UserHistory(items=[])

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        seen["fetched_candidate_uris"] = list(at_uris)
        seen["fetched_candidate_index"] = index
        return [("at://post/fetched", [0.0, 1.0], "did:plc:fetched", 8)]

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        history_like_counts,
        candidate_post_embeddings,
        candidate_author_dids,
        candidate_like_counts,
        *,
        base_url,
        api_key,
    ):
        seen["ranker_call"] = {
            "history_embeddings": history_embeddings,
            "history_author_dids": history_author_dids,
            "history_liked_at_times": history_liked_at_times,
            "history_like_counts": history_like_counts,
            "candidate_post_embeddings": candidate_post_embeddings,
            "candidate_author_dids": candidate_author_dids,
            "candidate_like_counts": candidate_like_counts,
        }
        return [0.4, 0.7]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fake_fetch_user_history_features,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(
                    at_uri="at://post/embedded",
                    minilm_l12_embedding=encode_float32_b64([1.0, 0.0]),
                    author_did="did:plc:embedded",
                    like_count=6,
                ),
                CandidatePost(at_uri="at://post/fetched"),
            ],
        )
    )

    assert seen["fetched_candidate_uris"] == ["at://post/fetched"]
    assert seen["fetched_candidate_index"] == "posts_recent"
    assert seen["ranker_call"] == {
        "history_embeddings": [],
        "history_author_dids": [],
        "history_liked_at_times": [],
        "history_like_counts": [],
        "candidate_post_embeddings": [[1.0, 0.0], [0.0, 1.0]],
        "candidate_author_dids": ["did:plc:embedded", "did:plc:fetched"],
        "candidate_like_counts": [6, 8],
    }
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/fetched", "rank": 1, "rank_score": 0.7},
        {"at_uri": "at://post/embedded", "rank": 2, "rank_score": 0.4},
    ]


def test_empty_history_mode_skips_like_history_and_ranks_candidates(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    fetch_history = AsyncMock()
    recorder = MagicMock()
    seen = {}

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        seen["fetch_calls"] = [(list(at_uris), index)]
        return [("at://post/fetched", [0.0, 1.0], "did:plc:fetched", 8)]

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        history_like_counts,
        candidate_post_embeddings,
        candidate_author_dids,
        candidate_like_counts,
        *,
        base_url,
        api_key,
    ):
        seen["ranker_call"] = {
            "history_embeddings": history_embeddings,
            "history_author_dids": history_author_dids,
            "history_liked_at_times": history_liked_at_times,
            "history_like_counts": history_like_counts,
            "candidate_post_embeddings": candidate_post_embeddings,
            "candidate_author_dids": candidate_author_dids,
            "candidate_like_counts": candidate_like_counts,
            "base_url": base_url,
            "api_key": api_key,
        }
        return [0.4, 0.7]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fetch_history,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "current_recorder",
        lambda: recorder,
    )

    result = asyncio.run(
        HeavyRanker(
            name="heavy_ranker_empty_history",
            history_mode="empty",
        ).predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(
                    at_uri="at://post/embedded",
                    minilm_l12_embedding=encode_float32_b64([1.0, 0.0]),
                    author_did="did:plc:embedded",
                    like_count=6,
                ),
                CandidatePost(at_uri="at://post/fetched"),
            ],
        )
    )

    fetch_history.assert_not_awaited()
    recorder.record_user_features.assert_called_once_with(
        "heavy_ranker_empty_history",
        [],
        0,
    )
    assert seen["fetch_calls"] == [
        (["at://post/fetched"], "posts_recent"),
    ]
    assert seen["ranker_call"] == {
        "history_embeddings": [],
        "history_author_dids": [],
        "history_liked_at_times": [],
        "history_like_counts": [],
        "candidate_post_embeddings": [[1.0, 0.0], [0.0, 1.0]],
        "candidate_author_dids": ["did:plc:embedded", "did:plc:fetched"],
        "candidate_like_counts": [6, 8],
        "base_url": "https://example.com",
        "api_key": "secret",
    }
    assert result.model == "heavy_ranker_empty_history"
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/fetched", "rank": 1, "rank_score": 0.7},
        {"at_uri": "at://post/embedded", "rank": 2, "rank_score": 0.4},
    ]


def test_predict_calls_ranker_with_empty_history_when_likes_have_no_embeddings(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    seen = {}

    async def fake_fetch_user_history_features(es, user_did):
        return UserHistory(
            items=[UserHistoryItem("at://liked/a", _time(1), None)]
        )

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        return [("at://post/a", [1.0, 0.0], "did:plc:a", 12)]

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        history_like_counts,
        candidate_post_embeddings,
        candidate_author_dids,
        candidate_like_counts,
        *,
        base_url,
        api_key,
    ):
        seen["history_embeddings"] = history_embeddings
        seen["history_author_dids"] = history_author_dids
        seen["history_liked_at_times"] = history_liked_at_times
        seen["history_like_counts"] = history_like_counts
        return [0.5]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fake_fetch_user_history_features,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[CandidatePost(at_uri="at://post/a")],
        )
    )

    assert seen == {
        "history_embeddings": [],
        "history_author_dids": [],
        "history_liked_at_times": [],
        "history_like_counts": [],
    }
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": 0.5},
    ]


def test_predict_returns_unscored_candidates_when_candidate_features_are_missing(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )

    async def fake_fetch_user_history_features(es, user_did):
        return UserHistory(items=[])

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        return []

    async def fake_predict_heavy_ranker_single_user(*args, **kwargs):
        raise AssertionError("ranker should not be called without candidate features")

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fake_fetch_user_history_features,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(at_uri="at://post/a"),
                CandidatePost(at_uri="at://post/b"),
            ],
        )
    )

    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": None},
        {"at_uri": "at://post/b", "rank": 2, "rank_score": None},
    ]


def test_predict_returns_unscored_candidates_when_output_count_mismatches(monkeypatch):
    monkeypatch.setattr(
        heavy_ranker_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )

    async def fake_fetch_user_history_features(es, user_did):
        return UserHistory(items=[])

    async def fake_fetch_post_embeddings_and_metadata(es, at_uris, index="posts"):
        return [
            ("at://post/a", [1.0, 0.0], "did:plc:a", 1),
            ("at://post/b", [0.0, 1.0], "did:plc:b", 2),
        ]

    async def fake_predict_heavy_ranker_single_user(*args, **kwargs):
        return [0.5]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_user_history_features",
        fake_fetch_user_history_features,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_metadata",
        fake_fetch_post_embeddings_and_metadata,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker(name="heavy_ranker", history_mode="actual").predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(at_uri="at://post/a"),
                CandidatePost(at_uri="at://post/b"),
            ],
        )
    )

    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": None},
        {"at_uri": "at://post/b", "rank": 2, "rank_score": None},
    ]


@pytest.fixture
def request_less_setup(monkeypatch):
    """Two candidates, one liked post, and one hydrated requestLess post."""
    state = SimpleNamespace(
        ranker=HeavyRanker(name="heavy_ranker", history_mode="actual"),
        candidates=[CandidatePost(at_uri="at://post/a"), CandidatePost(at_uri="at://post/b")],
        flag=MagicMock(return_value=True),
        history=AsyncMock(
            return_value=RequestLessHistory(
                [
                    RequestLessHistoryItem(
                        "at://less/a",
                        datetime.fromisoformat(_time(3)),
                        [0.0, 1.0],
                        "did:plc:less",
                        7,
                    ),
                    RequestLessHistoryItem(
                        "at://less/pending", datetime.fromisoformat(_time(2)), None
                    ),
                ]
            )
        ),
        user_history=AsyncMock(
            return_value=UserHistory(
                [
                    UserHistoryItem("at://liked/a", _time(1), [1.0, 0.0], "did:plc:liked", 9),
                ]
            )
        ),
        candidates_fetch=AsyncMock(
            return_value=[
                ("at://post/b", [0.0, 1.0], "did:plc:b", 20),
                ("at://post/a", [1.0, 0.0], "did:plc:a", 10),
            ]
        ),
        collector=MagicMock(),
    )

    async def infer(_embeddings, authors, *_args, **_kwargs):
        return [0.8, 0.4] if authors == ["did:plc:less"] else [0.2, 0.8]

    state.inference = AsyncMock(side_effect=infer)
    monkeypatch.setattr(
        heavy_ranker_module, "get_inference_settings", lambda: ("https://example.com", "secret")
    )
    monkeypatch.setattr(heavy_ranker_module, "request_less_heavy_ranker_enabled", state.flag)
    monkeypatch.setattr(heavy_ranker_module, "fetch_request_less_history_features", state.history)
    monkeypatch.setattr(heavy_ranker_module, "fetch_user_history_features", state.user_history)
    monkeypatch.setattr(
        heavy_ranker_module, "fetch_post_embeddings_and_metadata", state.candidates_fetch
    )
    monkeypatch.setattr(heavy_ranker_module, "predict_heavy_ranker_single_user", state.inference)
    monkeypatch.setattr(heavy_ranker_module, "get_metric_collector", lambda: state.collector)
    token = set_traffic("real")
    try:
        yield state
    finally:
        reset_traffic(token)


async def _predict_request_less(state):
    return await state.ranker.predict(None, "did:plc:user1", state.candidates)


def _scores(result):
    return {row.at_uri: row.rank_score for row in result.result.rankings}


def _assert_outcome(state, outcome):
    calls = [
        call
        for call in state.collector.record.call_args_list
        if call.args[0].endswith("outcome_count")
    ]
    assert len(calls) == 1
    assert calls[0].args == ("rank.request_less.outcome_count", 1)
    assert calls[0].kwargs == {"outcome": outcome}


@pytest.mark.asyncio
async def test_request_less_uses_same_candidate_batch_and_saved_history_order(request_less_setup):
    state = request_less_setup
    state.history.return_value.items.append(
        RequestLessHistoryItem(
            "at://less/older", datetime.fromisoformat(_time(1)), [0.5, 0.5], "did:plc:older", 3
        )
    )

    async def infer(_embeddings, authors, *_args, **_kwargs):
        return [0.8, 0.4] if authors[0] == "did:plc:less" else [0.2, 0.8]

    state.inference.side_effect = infer
    result = await _predict_request_less(state)

    assert _scores(result) == pytest.approx({"at://post/b": -0.4, "at://post/a": 0.5})
    state.history.assert_awaited_once_with("did:plc:user1")
    state.candidates_fetch.assert_awaited_once_with(
        None, ["at://post/a", "at://post/b"], index="posts_recent"
    )
    assert state.inference.await_count == 2
    positive = next(
        call for call in state.inference.await_args_list if call.args[1] == ["did:plc:liked"]
    )
    negative = next(
        call for call in state.inference.await_args_list if call.args[1][0] == "did:plc:less"
    )
    assert negative.args[:4] == (
        [[0.0, 1.0], [0.5, 0.5]],
        ["did:plc:less", "did:plc:older"],
        [_time(3), _time(1)],
        [7, 3],
    )
    assert (
        positive.args[4:]
        == negative.args[4:]
        == ([[0.0, 1.0], [1.0, 0.0]], ["did:plc:b", "did:plc:a"], [20, 10])
    )
    assert (
        positive.kwargs
        == negative.kwargs
        == {"base_url": "https://example.com", "api_key": "secret"}
    )
    _assert_outcome(state, "applied")


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["zero", "empty", "logged_out"])
async def test_request_less_disabled_paths_skip_flag_history_and_negative_inference(
    request_less_setup, monkeypatch, gate
):
    state = request_less_setup
    if gate == "zero":
        monkeypatch.setattr(heavy_ranker_module, "REQUEST_LESS_COEFFICIENT", 0)
    elif gate == "empty":
        state.ranker = HeavyRanker(name="heavy_ranker_empty_history", history_mode="empty")
    else:
        monkeypatch.setattr(heavy_ranker_module, "get_traffic", lambda: gate)

    result = await _predict_request_less(state)

    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    state.flag.assert_not_called()
    state.history.assert_not_awaited()
    assert state.inference.await_count == 1
    _assert_outcome(state, "disabled")
    assert len(state.collector.record.call_args_list) == 1


@pytest.mark.asyncio
async def test_request_less_flag_off_preserves_positive_scores(request_less_setup):
    state = request_less_setup
    state.flag.return_value = False
    result = await _predict_request_less(state)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    state.history.assert_not_awaited()
    assert state.inference.await_count == 1
    _assert_outcome(state, "flag_off")


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_only", [False, True])
async def test_request_less_without_usable_history_skips_negative_inference(
    request_less_setup, pending_only
):
    state = request_less_setup
    state.history.return_value = RequestLessHistory(
        [RequestLessHistoryItem("at://less/pending", datetime.fromisoformat(_time(1)), None)]
        if pending_only
        else []
    )
    result = await _predict_request_less(state)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    assert state.inference.await_count == 1
    _assert_outcome(state, "missing_features")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outputs",
    [
        None,
        {},
        "bad",
        [],
        [0.5],
        [0.1, 0.2, 0.3],
        [True, 0.5],
        ["0.2", 0.5],
        [None, 0.5],
        [float("nan"), 0.5],
        [float("inf"), 0.5],
        [-0.1, 0.5],
        [1.1, 0.5],
    ],
)
async def test_request_less_invalid_batch_preserves_all_positive_scores(
    request_less_setup, outputs
):
    state = request_less_setup

    async def infer(_embeddings, authors, *_args, **_kwargs):
        return outputs if authors == ["did:plc:less"] else [0.2, 0.8]

    state.inference.side_effect = infer
    result = await _predict_request_less(state)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    _assert_outcome(state, "invalid_output")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["flag", "history", "inference"])
async def test_request_less_errors_preserve_positive_scores(request_less_setup, failure_stage):
    state = request_less_setup
    if failure_stage == "flag":
        state.flag.side_effect = RuntimeError("flag unavailable")
    elif failure_stage == "history":
        state.history.side_effect = RuntimeError("Firestore unavailable")
    else:

        async def infer(_embeddings, authors, *_args, **_kwargs):
            if authors == ["did:plc:less"]:
                raise RuntimeError("inference unavailable")
            return [0.2, 0.8]

        state.inference.side_effect = infer

    result = await _predict_request_less(state)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    _assert_outcome(state, "error")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure, outcome",
    [
        (InferenceResponseFormatError("invalid JSON"), "invalid_output"),
        (httpx.ReadTimeout("HTTP deadline"), "timeout"),
    ],
)
async def test_negative_inference_helper_failures_have_specific_outcomes(
    request_less_setup, failure, outcome
):
    state = request_less_setup

    async def infer(_embeddings, authors, *_args, **_kwargs):
        if authors == ["did:plc:less"]:
            raise failure
        return [0.2, 0.8]

    state.inference.side_effect = infer
    result = await _predict_request_less(state)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    _assert_outcome(state, outcome)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["history", "inference"])
async def test_optional_deadline_cancels_stalled_work_and_returns_positive_scores(
    request_less_setup, monkeypatch, stage
):
    state = request_less_setup
    monkeypatch.setattr(heavy_ranker_module, "RANK_MODEL_TIMEOUT_SEC", 0.05)
    cancelled = asyncio.Event()

    async def block():
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    if stage == "history":

        async def read(_did):
            await block()

        state.history.side_effect = read
    else:

        async def infer(_embeddings, authors, *_args, **_kwargs):
            if authors == ["did:plc:less"]:
                await block()
            return [0.2, 0.8]

        state.inference.side_effect = infer

    result = await asyncio.wait_for(_predict_request_less(state), 0.5)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    assert cancelled.is_set()
    _assert_outcome(state, "timeout")


@pytest.mark.asyncio
async def test_flag_thread_does_not_block_positive_scoring_or_start_late_work(
    request_less_setup, monkeypatch
):
    state = request_less_setup
    monkeypatch.setattr(heavy_ranker_module, "RANK_MODEL_TIMEOUT_SEC", 0.1)
    release_flag = threading.Event()
    flag_started = asyncio.Event()
    flag_finished = asyncio.Event()
    positive_started = asyncio.Event()
    loop = asyncio.get_running_loop()

    def flag(*_args):
        loop.call_soon_threadsafe(flag_started.set)
        release_flag.wait(1)
        loop.call_soon_threadsafe(flag_finished.set)
        return True

    async def infer(*_args, **_kwargs):
        positive_started.set()
        return [0.2, 0.8]

    state.flag.side_effect = flag
    state.inference.side_effect = infer
    task = asyncio.create_task(_predict_request_less(state))
    try:
        await asyncio.wait_for(flag_started.wait(), 0.5)
        await asyncio.wait_for(positive_started.wait(), 0.5)
        result = await asyncio.wait_for(task, 0.5)
        assert not release_flag.is_set()
        assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    finally:
        release_flag.set()
        await asyncio.wait_for(flag_finished.wait(), 0.5)
        await asyncio.gather(task, return_exceptions=True)
    state.history.assert_not_awaited()
    assert state.inference.await_count == 1
    _assert_outcome(state, "timeout")


@pytest.mark.asyncio
async def test_optional_deadline_does_not_cancel_shared_candidate_preparation(
    request_less_setup, monkeypatch
):
    state = request_less_setup
    monkeypatch.setattr(heavy_ranker_module, "RANK_MODEL_TIMEOUT_SEC", 0.05)
    optional_done = asyncio.Event()
    release_candidates = asyncio.Event()
    candidates_cancelled = asyncio.Event()
    features = state.candidates_fetch.return_value

    async def fetch(*_args, **_kwargs):
        try:
            await release_candidates.wait()
            return features
        except asyncio.CancelledError:
            candidates_cancelled.set()
            raise

    def metric(name, *_args, **_kwargs):
        if name == "rank.request_less.duration_ms":
            optional_done.set()

    state.candidates_fetch.side_effect = fetch
    state.collector.record.side_effect = metric
    task = asyncio.create_task(_predict_request_less(state))
    try:
        await asyncio.wait_for(optional_done.wait(), 0.5)
        assert not candidates_cancelled.is_set()
        assert not task.done()
    finally:
        release_candidates.set()
    result = await asyncio.wait_for(task, 0.5)
    assert _scores(result) == {"at://post/b": 0.2, "at://post/a": 0.8}
    assert state.inference.await_count == 1
    _assert_outcome(state, "timeout")


@pytest.mark.asyncio
@pytest.mark.parametrize("positive_result", ["error", "empty", "mismatch"])
async def test_positive_failure_cleans_up_negative_inference_without_negative_only_result(
    request_less_setup, positive_result
):
    state = request_less_setup
    negative_started = asyncio.Event()
    negative_cancelled = asyncio.Event()

    async def infer(_embeddings, authors, *_args, **_kwargs):
        if authors == ["did:plc:less"]:
            negative_started.set()
            try:
                await asyncio.Future()
            finally:
                negative_cancelled.set()
        await negative_started.wait()
        if positive_result == "error":
            raise RuntimeError("positive failure")
        return [] if positive_result == "empty" else [0.5]

    state.inference.side_effect = infer
    if positive_result == "error":
        with pytest.raises(RuntimeError, match="positive failure"):
            await asyncio.wait_for(_predict_request_less(state), 0.5)
    else:
        result = await asyncio.wait_for(_predict_request_less(state), 0.5)
        assert _scores(result) == {"at://post/a": None, "at://post/b": None}
    assert negative_cancelled.is_set()
    _assert_outcome(state, "cancelled")


@pytest.mark.asyncio
async def test_external_cancellation_drains_positive_and_negative_work(request_less_setup):
    state = request_less_setup
    started = {kind: asyncio.Event() for kind in ("positive", "negative")}
    cancelled = {kind: asyncio.Event() for kind in started}

    async def infer(_embeddings, authors, *_args, **_kwargs):
        kind = "negative" if authors == ["did:plc:less"] else "positive"
        started[kind].set()
        try:
            await asyncio.Future()
        finally:
            cancelled[kind].set()

    state.inference.side_effect = infer
    task = asyncio.create_task(_predict_request_less(state))
    for event in started.values():
        await asyncio.wait_for(event.wait(), 0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(event.is_set() for event in cancelled.values())
    _assert_outcome(state, "cancelled")


@pytest.mark.asyncio
async def test_completed_negative_scores_are_not_applied_when_positive_inference_fails(
    request_less_setup,
):
    state = request_less_setup
    negative_done = asyncio.Event()

    async def infer(_embeddings, authors, *_args, **_kwargs):
        if authors == ["did:plc:less"]:
            negative_done.set()
            return [0.8, 0.4]
        await negative_done.wait()
        raise RuntimeError("positive failure")

    state.inference.side_effect = infer
    with pytest.raises(RuntimeError, match="positive failure"):
        await asyncio.wait_for(_predict_request_less(state), 0.5)
    _assert_outcome(state, "cancelled")


@pytest.mark.asyncio
async def test_external_cancellation_drains_shared_candidate_and_like_history_tasks(
    request_less_setup,
):
    state = request_less_setup
    started = {kind: asyncio.Event() for kind in ("candidates", "likes", "request_less")}
    cancelled = {kind: asyncio.Event() for kind in started}

    async def block(kind):
        started[kind].set()
        try:
            await asyncio.Future()
        finally:
            cancelled[kind].set()

    async def candidates(*_args, **_kwargs):
        await block("candidates")

    async def likes(*_args, **_kwargs):
        await block("likes")

    async def request_less(*_args, **_kwargs):
        await block("request_less")

    state.candidates_fetch.side_effect = candidates
    state.user_history.side_effect = likes
    state.history.side_effect = request_less
    task = asyncio.create_task(_predict_request_less(state))
    for event in started.values():
        await asyncio.wait_for(event.wait(), 0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(event.is_set() for event in cancelled.values())
    state.inference.assert_not_awaited()
    _assert_outcome(state, "cancelled")


@pytest.mark.asyncio
async def test_request_less_metrics_are_best_effort(request_less_setup):
    state = request_less_setup
    state.collector.record.side_effect = RuntimeError("metrics unavailable")
    result = await _predict_request_less(state)
    assert _scores(result) == pytest.approx({"at://post/b": -0.4, "at://post/a": 0.5})


@pytest.mark.asyncio
async def test_request_less_metrics_export_outcome_duration_and_inherited_labels(
    request_less_setup, monkeypatch
):
    from opentelemetry.sdk.metrics.export import (
        HistogramDataPoint,
        InMemoryMetricReader,
        NumberDataPoint,
    )

    from ..metrics import MetricCollector

    reader = InMemoryMetricReader()
    collector = MetricCollector._from_reader(
        reader, service_name="request-less-rank-test", env="test"
    )
    monkeypatch.setattr(heavy_ranker_module, "get_metric_collector", lambda: collector)
    endpoint_token = set_endpoint("rank_predict")
    try:
        await _predict_request_less(request_less_setup)
        exported = reader.get_metrics_data()
        assert exported is not None
        metrics = {
            metric.name: metric.data.data_points
            for resource in exported.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }
        outcome = metrics["rank.request_less.outcome_count"][0]
        assert isinstance(outcome, NumberDataPoint)
        assert outcome.value == 1
        assert outcome.attributes == {
            "endpoint": "rank_predict",
            "traffic": "real",
            "outcome": "applied",
        }
        duration = metrics["rank.request_less.duration_ms"][0]
        assert isinstance(duration, HistogramDataPoint)
        assert duration.count == 1
        assert duration.sum >= 0
        assert duration.attributes == {"endpoint": "rank_predict", "traffic": "real"}
    finally:
        reset_endpoint(endpoint_token)
        await collector.shutdown()

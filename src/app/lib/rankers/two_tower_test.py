"""Tests for the two-tower ranker configuration behavior."""

import asyncio

import pytest

from ...models import CandidatePost
from . import two_tower as two_tower_module
from .base import RankerExecutionError
from .two_tower import TwoTowerRanker


def test_predict_requires_inference_env_vars(monkeypatch):
    monkeypatch.delenv("GE_INFERENCE_BASE_URL", raising=False)
    monkeypatch.delenv("GE_INFERENCE_API_KEY", raising=False)

    ranker = TwoTowerRanker()

    with pytest.raises(RankerExecutionError, match="GE_INFERENCE_BASE_URL"):
        asyncio.run(
            ranker.predict(
                es=None,
                user_did="did:plc:user1",
                candidates=[CandidatePost(at_uri="at://post/1")],
            )
        )


def test_predict_keeps_candidate_uris_aligned_with_embeddings(monkeypatch):
    monkeypatch.setattr(
        two_tower_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )

    async def fake_fetch_recent_liked_post_uris(es, user_did):
        return ["at://liked/1"]

    monkeypatch.setattr(
        two_tower_module,
        "fetch_recent_liked_post_uris",
        fake_fetch_recent_liked_post_uris,
    )

    async def fake_fetch_post_embeddings(es, at_uris):
        if at_uris == ["at://liked/1"]:
            return [("at://liked/1", [0.5, 0.5])]
        return [
            ("at://post/b", [0.0, 1.0]),
            ("at://post/a", [1.0, 0.0]),
        ]

    monkeypatch.setattr(two_tower_module, "fetch_post_embeddings", fake_fetch_post_embeddings)

    async def fake_predict_user_tower_single(history_embeddings, *, base_url, api_key):
        return [[1.0, 0.0]]

    async def fake_predict_post_tower_batch(post_embeddings, *, base_url, api_key):
        assert post_embeddings == [[0.0, 1.0], [1.0, 0.0]]
        return post_embeddings

    monkeypatch.setattr(
        two_tower_module,
        "predict_user_tower_single",
        fake_predict_user_tower_single,
    )
    monkeypatch.setattr(two_tower_module, "predict_post_tower_batch", fake_predict_post_tower_batch)

    result = asyncio.run(
        TwoTowerRanker().predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(at_uri="at://post/a"),
                CandidatePost(at_uri="at://post/b"),
                CandidatePost(at_uri="at://post/missing"),
            ],
        )
    )

    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": 1.0},
        {"at_uri": "at://post/b", "rank": 2, "rank_score": 0.0},
        {"at_uri": "at://post/missing", "rank": 3, "rank_score": None},
    ]


def test_predict_calls_user_tower_with_empty_history_when_user_has_no_likes(monkeypatch):
    monkeypatch.setattr(
        two_tower_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    seen = {}

    async def fake_fetch_recent_liked_post_uris(es, user_did):
        return []

    async def fake_fetch_post_embeddings(es, at_uris):
        seen.setdefault("fetch_post_embeddings_calls", []).append(at_uris)
        return [("at://post/a", [2.0, 0.0])]

    async def fake_predict_user_tower_single(history_embeddings, *, base_url, api_key):
        seen["history_embeddings"] = history_embeddings
        return [[1.0, 0.0]]

    async def fake_predict_post_tower_batch(post_embeddings, *, base_url, api_key):
        return post_embeddings

    monkeypatch.setattr(
        two_tower_module,
        "fetch_recent_liked_post_uris",
        fake_fetch_recent_liked_post_uris,
    )
    monkeypatch.setattr(two_tower_module, "fetch_post_embeddings", fake_fetch_post_embeddings)
    monkeypatch.setattr(
        two_tower_module,
        "predict_user_tower_single",
        fake_predict_user_tower_single,
    )
    monkeypatch.setattr(two_tower_module, "predict_post_tower_batch", fake_predict_post_tower_batch)

    result = asyncio.run(
        TwoTowerRanker().predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[CandidatePost(at_uri="at://post/a")],
        )
    )

    assert seen["history_embeddings"] == []
    assert seen["fetch_post_embeddings_calls"] == [["at://post/a"]]
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": 2.0},
    ]


def test_predict_calls_user_tower_with_empty_history_when_likes_have_no_embeddings(monkeypatch):
    monkeypatch.setattr(
        two_tower_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )
    seen = {}

    async def fake_fetch_recent_liked_post_uris(es, user_did):
        return ["at://liked/1"]

    async def fake_fetch_post_embeddings(es, at_uris):
        seen.setdefault("fetch_post_embeddings_calls", []).append(at_uris)
        if at_uris == ["at://liked/1"]:
            return []
        return [("at://post/a", [2.0, 0.0])]

    async def fake_predict_user_tower_single(history_embeddings, *, base_url, api_key):
        seen["history_embeddings"] = history_embeddings
        return [[1.0, 0.0]]

    async def fake_predict_post_tower_batch(post_embeddings, *, base_url, api_key):
        return post_embeddings

    monkeypatch.setattr(
        two_tower_module,
        "fetch_recent_liked_post_uris",
        fake_fetch_recent_liked_post_uris,
    )
    monkeypatch.setattr(two_tower_module, "fetch_post_embeddings", fake_fetch_post_embeddings)
    monkeypatch.setattr(
        two_tower_module,
        "predict_user_tower_single",
        fake_predict_user_tower_single,
    )
    monkeypatch.setattr(two_tower_module, "predict_post_tower_batch", fake_predict_post_tower_batch)

    result = asyncio.run(
        TwoTowerRanker().predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[CandidatePost(at_uri="at://post/a")],
        )
    )

    assert seen["history_embeddings"] == []
    assert seen["fetch_post_embeddings_calls"] == [
        ["at://liked/1"],
        ["at://post/a"],
    ]
    assert [ranking.model_dump() for ranking in result.result.rankings] == [
        {"at_uri": "at://post/a", "rank": 1, "rank_score": 2.0},
    ]


def test_predict_returns_unscored_candidates_when_candidate_embeddings_are_missing(monkeypatch):
    monkeypatch.setattr(
        two_tower_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )

    async def fake_fetch_recent_liked_post_uris(es, user_did):
        return []

    async def fake_fetch_post_embeddings(es, at_uris):
        return []

    async def fake_predict_user_tower_single(history_embeddings, *, base_url, api_key):
        return [[1.0, 0.0]]

    async def fake_predict_post_tower_batch(post_embeddings, *, base_url, api_key):
        raise AssertionError("post tower should not be called without candidate embeddings")

    monkeypatch.setattr(
        two_tower_module,
        "fetch_recent_liked_post_uris",
        fake_fetch_recent_liked_post_uris,
    )
    monkeypatch.setattr(two_tower_module, "fetch_post_embeddings", fake_fetch_post_embeddings)
    monkeypatch.setattr(
        two_tower_module,
        "predict_user_tower_single",
        fake_predict_user_tower_single,
    )
    monkeypatch.setattr(two_tower_module, "predict_post_tower_batch", fake_predict_post_tower_batch)

    result = asyncio.run(
        TwoTowerRanker().predict(
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


def test_predict_raises_when_user_tower_returns_wrong_number_of_embeddings(monkeypatch):
    monkeypatch.setattr(
        two_tower_module,
        "get_inference_settings",
        lambda: ("https://example.com", "secret"),
    )

    async def fake_fetch_recent_liked_post_uris(es, user_did):
        return ["at://liked/1"]

    async def fake_fetch_post_embeddings(es, at_uris):
        if at_uris == ["at://liked/1"]:
            return [("at://liked/1", [0.5, 0.5])]
        return [("at://post/a", [1.0, 0.0])]

    async def fake_predict_user_tower_single(history_embeddings, *, base_url, api_key):
        return []

    monkeypatch.setattr(
        two_tower_module,
        "fetch_recent_liked_post_uris",
        fake_fetch_recent_liked_post_uris,
    )
    monkeypatch.setattr(two_tower_module, "fetch_post_embeddings", fake_fetch_post_embeddings)
    monkeypatch.setattr(
        two_tower_module,
        "predict_user_tower_single",
        fake_predict_user_tower_single,
    )

    with pytest.raises(
        RankerExecutionError,
        match="user inference returned 0 embeddings; expected 1",
    ):
        asyncio.run(
            TwoTowerRanker().predict(
                es=None,
                user_did="did:plc:user1",
                candidates=[CandidatePost(at_uri="at://post/a")],
            )
        )

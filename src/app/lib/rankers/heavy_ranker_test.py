"""Tests for the heavy ranker."""

import asyncio
from datetime import datetime, timezone

import pytest

from ...models import CandidatePost
from ..embeddings import encode_float32_b64
from . import heavy_ranker as heavy_ranker_module
from .heavy_ranker import HeavyRanker


def _time(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=timezone.utc)


def test_predict_requires_inference_env_vars(monkeypatch):
    monkeypatch.delenv("GE_INFERENCE_BASE_URL", raising=False)
    monkeypatch.delenv("GE_INFERENCE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="GE_INFERENCE_BASE_URL"):
        asyncio.run(
            HeavyRanker().predict(
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

    async def fake_fetch_recent_liked_post_uris_and_times(es, user_did):
        assert user_did == "did:plc:user1"
        return ["at://liked/a", "at://liked/b", "at://liked/c"], liked_times

    async def fake_fetch_post_embeddings_and_authors(es, at_uris, index="posts"):
        seen.setdefault("fetch_calls", []).append((list(at_uris), index))
        if at_uris == ["at://liked/a", "at://liked/b", "at://liked/c"]:
            return [
                ("at://liked/a", [1.0, 0.0], "did:plc:liked-a"),
                ("at://liked/c", [0.0, 1.0], "did:plc:liked-c"),
            ]
        if at_uris == ["at://post/a", "at://post/b", "at://post/missing"]:
            return [
                ("at://post/b", [0.0, 1.0], "did:plc:b"),
                ("at://post/a", [1.0, 0.0], "did:plc:a"),
            ]
        raise AssertionError(f"unexpected at_uris: {at_uris}")

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        candidate_post_embeddings,
        candidate_author_dids,
        *,
        base_url,
        api_key,
    ):
        seen["ranker_call"] = {
            "history_embeddings": history_embeddings,
            "history_author_dids": history_author_dids,
            "history_liked_at_times": history_liked_at_times,
            "candidate_post_embeddings": candidate_post_embeddings,
            "candidate_author_dids": candidate_author_dids,
            "base_url": base_url,
            "api_key": api_key,
        }
        return [0.2, 0.9]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_recent_liked_post_uris_and_times",
        fake_fetch_recent_liked_post_uris_and_times,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_authors",
        fake_fetch_post_embeddings_and_authors,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker().predict(
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
        (["at://liked/a", "at://liked/b", "at://liked/c"], "posts"),
        (["at://post/a", "at://post/b", "at://post/missing"], "posts_recent"),
    ]
    assert seen["ranker_call"] == {
        "history_embeddings": [[1.0, 0.0], [0.0, 1.0]],
        "history_author_dids": ["did:plc:liked-a", "did:plc:liked-c"],
        "history_liked_at_times": [_time(1), _time(3)],
        "candidate_post_embeddings": [[0.0, 1.0], [1.0, 0.0]],
        "candidate_author_dids": ["did:plc:b", "did:plc:a"],
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

    async def fake_fetch_recent_liked_post_uris_and_times(es, user_did):
        return [], []

    async def fake_fetch_post_embeddings_and_authors(es, at_uris, index="posts"):
        seen["fetched_candidate_uris"] = list(at_uris)
        seen["fetched_candidate_index"] = index
        return [("at://post/fetched", [0.0, 1.0], "did:plc:fetched")]

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        candidate_post_embeddings,
        candidate_author_dids,
        *,
        base_url,
        api_key,
    ):
        seen["ranker_call"] = {
            "history_embeddings": history_embeddings,
            "history_author_dids": history_author_dids,
            "history_liked_at_times": history_liked_at_times,
            "candidate_post_embeddings": candidate_post_embeddings,
            "candidate_author_dids": candidate_author_dids,
        }
        return [0.4, 0.7]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_recent_liked_post_uris_and_times",
        fake_fetch_recent_liked_post_uris_and_times,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_authors",
        fake_fetch_post_embeddings_and_authors,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker().predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[
                CandidatePost(
                    at_uri="at://post/embedded",
                    minilm_l12_embedding=encode_float32_b64([1.0, 0.0]),
                    author_did="did:plc:embedded",
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
        "candidate_post_embeddings": [[1.0, 0.0], [0.0, 1.0]],
        "candidate_author_dids": ["did:plc:embedded", "did:plc:fetched"],
    }
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

    async def fake_fetch_recent_liked_post_uris_and_times(es, user_did):
        return ["at://liked/a"], [_time(1)]

    async def fake_fetch_post_embeddings_and_authors(es, at_uris, index="posts"):
        if at_uris == ["at://liked/a"]:
            return []
        return [("at://post/a", [1.0, 0.0], "did:plc:a")]

    async def fake_predict_heavy_ranker_single_user(
        history_embeddings,
        history_author_dids,
        history_liked_at_times,
        candidate_post_embeddings,
        candidate_author_dids,
        *,
        base_url,
        api_key,
    ):
        seen["history_embeddings"] = history_embeddings
        seen["history_author_dids"] = history_author_dids
        seen["history_liked_at_times"] = history_liked_at_times
        return [0.5]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_recent_liked_post_uris_and_times",
        fake_fetch_recent_liked_post_uris_and_times,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_authors",
        fake_fetch_post_embeddings_and_authors,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker().predict(
            es=None,
            user_did="did:plc:user1",
            candidates=[CandidatePost(at_uri="at://post/a")],
        )
    )

    assert seen == {
        "history_embeddings": [],
        "history_author_dids": [],
        "history_liked_at_times": [],
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

    async def fake_fetch_recent_liked_post_uris_and_times(es, user_did):
        return [], []

    async def fake_fetch_post_embeddings_and_authors(es, at_uris, index="posts"):
        return []

    async def fake_predict_heavy_ranker_single_user(*args, **kwargs):
        raise AssertionError("ranker should not be called without candidate features")

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_recent_liked_post_uris_and_times",
        fake_fetch_recent_liked_post_uris_and_times,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_authors",
        fake_fetch_post_embeddings_and_authors,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker().predict(
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

    async def fake_fetch_recent_liked_post_uris_and_times(es, user_did):
        return [], []

    async def fake_fetch_post_embeddings_and_authors(es, at_uris, index="posts"):
        return [
            ("at://post/a", [1.0, 0.0], "did:plc:a"),
            ("at://post/b", [0.0, 1.0], "did:plc:b"),
        ]

    async def fake_predict_heavy_ranker_single_user(*args, **kwargs):
        return [0.5]

    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_recent_liked_post_uris_and_times",
        fake_fetch_recent_liked_post_uris_and_times,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "fetch_post_embeddings_and_authors",
        fake_fetch_post_embeddings_and_authors,
    )
    monkeypatch.setattr(
        heavy_ranker_module,
        "predict_heavy_ranker_single_user",
        fake_predict_heavy_ranker_single_user,
    )

    result = asyncio.run(
        HeavyRanker().predict(
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

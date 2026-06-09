"""Tests for the shared ranker pipeline."""

import asyncio

import pytest
from pydantic import ValidationError

from ...models import CandidatePost, RankModelSpec, RankPredictRequest, RankPredictResult, RankedCandidate
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from . import predict as predict_module
from .base import RankerExecutionError, RankerResult


class StubRanker:
    """A ranker that returns pre-configured raw scores keyed by `at_uri`."""

    def __init__(self, name: str, bounds: tuple[float, float], scores: dict[str, float]):
        self._name = name
        self._bounds = bounds
        self._scores = scores

    @property
    def name(self) -> str:
        return self._name

    @property
    def score_bounds(self) -> tuple[float, float]:
        return self._bounds

    async def predict(self, es, user_did, candidates):
        rankings = [
            RankedCandidate(
                at_uri=candidate.at_uri,
                rank=idx,
                rank_score=self._scores.get(candidate.at_uri),
            )
            for idx, candidate in enumerate(candidates, start=1)
            if candidate.at_uri is not None
        ]
        return RankerResult(model=self.name, result=RankPredictResult(rankings=rankings))


class ExplodingRanker:
    @property
    def name(self) -> str:
        return "exploding"

    @property
    def score_bounds(self) -> tuple[float, float]:
        return (0.0, 1.0)

    async def predict(self, es, user_did, candidates):
        raise RuntimeError("downstream boom")


def _request(models: list[RankModelSpec], candidates: list[CandidatePost]) -> RankPredictRequest:
    return RankPredictRequest(
        models=models,
        user_did="did:plc:user1",
        candidates=candidates,
    )


def test_run_predict_wraps_unexpected_ranker_failure(monkeypatch):
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: ExplodingRanker())

    with pytest.raises(RankerExecutionError, match="Ranker 'exploding' failed: downstream boom"):
        asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[RankModelSpec(name="exploding", weight=1.0)],
                    candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
                ),
                es=object(),
            )
        )


def test_run_predict_raises_for_unknown_model(monkeypatch):
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: None)

    with pytest.raises(predict_module.RankModelNotFoundError):
        asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[RankModelSpec(name="does_not_exist", weight=1.0)],
                    candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
                ),
                es=object(),
            )
        )


def test_run_predict_normalizes_and_combines_with_weights(monkeypatch):
    """Each model's raw scores are linearly mapped from its `score_bounds` into
    [-1, 1], then combined via a weighted average (weights normalized to sum
    to 1)."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]

    # Model "x": bounds [0, 1] -> raw 1.0 normalizes to 1.0, raw 0.0 to -1.0
    # Model "y": bounds [-10, 10] -> raw 0.0 normalizes to 0.0 for both
    rankers = {
        "x": StubRanker("x", (0.0, 1.0), {"at://post/a": 1.0, "at://post/b": 0.0}),
        "y": StubRanker("y", (-10.0, 10.0), {"at://post/a": 0.0, "at://post/b": 0.0}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="x", weight=3.0),
                    RankModelSpec(name="y", weight=1.0),
                ],
                candidates=candidates,
            ),
            es=object(),
        )
    )

    # combined(a) = (3/4)*1.0 + (1/4)*0.0 = 0.75
    # combined(b) = (3/4)*(-1.0) + (1/4)*0.0 = -0.75
    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(0.75)),
        ("at://post/b", 2, pytest.approx(-0.75)),
    ]


def test_run_predict_treats_missing_scores_as_neutral(monkeypatch):
    """A candidate a ranker didn't score normalizes to 0.0 (neutral) when combined."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", (0.0, 1.0), {"at://post/a": 1.0}),  # "b" missing -> neutral
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="x", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(1.0)),
        ("at://post/b", 2, pytest.approx(0.0)),
    ]


def test_run_predict_records_normalized_model_scores_and_weight(monkeypatch):
    """`record_model_scores` is invoked once per configured model with its
    weight and *normalized* (not raw) per-uri scores — and the final combined
    score is not separately recorded there (it's captured via `record_ranking`
    by the caller)."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", (0.0, 1.0), {"at://post/a": 1.0, "at://post/b": 0.0}),
        "y": StubRanker("y", (-1.0, 1.0), {"at://post/a": 0.5, "at://post/b": -0.5}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[
                        RankModelSpec(name="x", weight=2.0),
                        RankModelSpec(name="y", weight=1.0),
                    ],
                    candidates=candidates,
                ),
                es=object(),
            )
        )

    assert rec.model_scores == [
        ("x", 2.0, {"at://post/a": pytest.approx(1.0), "at://post/b": pytest.approx(-1.0)}),
        ("y", 1.0, {"at://post/a": pytest.approx(0.5), "at://post/b": pytest.approx(-0.5)}),
    ]


def test_run_predict_preserves_duplicate_candidate_count(monkeypatch):
    """Combination is keyed by `at_uri` (raw scores collapse last-write-wins
    for duplicate uris), but the output still contains one ranking per input
    candidate, with same-uri ties broken by original candidate order."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/a", score=0.9),
        CandidatePost(at_uri="at://post/b", score=0.4),
    ]
    rankers = {
        "x": StubRanker("x", (0.0, 1.0), {"at://post/a": 0.5, "at://post/b": 0.5}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="x", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert [r.at_uri for r in result.rankings] == ["at://post/a", "at://post/a", "at://post/b"]
    assert all(r.rank_score == pytest.approx(0.0) for r in result.rankings)
    assert [r.rank for r in result.rankings] == [1, 2, 3]


def test_rank_predict_request_requires_user_did():
    with pytest.raises(ValidationError, match="user_did"):
        RankPredictRequest(  # pyright: ignore[reportCallIssue]
            models=[RankModelSpec(name="two_tower", weight=1.0)],
            candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
        )

"""Tests for the shared ranker pipeline."""

import asyncio

import pytest
from pydantic import ValidationError

from ...models import (
    CandidatePost,
    RankedCandidate,
    RankModelSpec,
    RankPredictRequest,
    RankPredictResult,
)
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from . import predict as predict_module
from .base import RankerExecutionError, RankerResult
from .candidate_score import CandidateScoreRanker


class StubRanker:
    """A ranker that returns pre-configured raw scores keyed by `at_uri`."""

    def __init__(
        self,
        name: str,
        scores: dict[str, float],
        *,
        include_missing: bool = True,
    ):
        self._name = name
        self._scores = scores
        self._include_missing = include_missing

    @property
    def name(self) -> str:
        return self._name

    async def predict(self, es, user_did, candidates):
        rankings = [
            RankedCandidate(
                at_uri=candidate.at_uri,
                rank=idx,
                rank_score=self._scores.get(candidate.at_uri),
            )
            for idx, candidate in enumerate(candidates, start=1)
            if candidate.at_uri is not None
            and (self._include_missing or candidate.at_uri in self._scores)
        ]
        return RankerResult(model=self.name, result=RankPredictResult(rankings=rankings))


class ExplodingRanker:
    @property
    def name(self) -> str:
        return "exploding"

    async def predict(self, es, user_did, candidates):
        raise RuntimeError("downstream boom")


class HangingRanker:
    """A ranker that never returns within any reasonable per-model timeout."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def predict(self, es, user_did, candidates):
        await asyncio.sleep(9999)


class TimingOutRanker:
    """A ranker that raises TimeoutError immediately.

    Used instead of an actual hang + short timeout when the assertion also
    depends on a concurrently-running ranker's results, since racing a
    real sub-second `asyncio.wait_for` deadline against another task's
    completion is inherently timing-sensitive (flaky under CI scheduling
    jitter). This exercises the same except-TimeoutError handling in
    `_run_one` without depending on wall-clock timing.
    """

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def predict(self, es, user_did, candidates):
        raise TimeoutError("simulated timeout")


def _request(
    models: list[RankModelSpec],
    candidates: list[CandidatePost],
    *,
    politics: float = 1.0,
) -> RankPredictRequest:
    return RankPredictRequest(
        models=models,
        user_did="did:plc:user1",
        candidates=candidates,
        politics=politics,
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


def test_run_predict_combines_supplied_scores_without_rescaling_or_clipping(monkeypatch):
    """Only weights are normalized; signed and above-one scores pass through."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]

    rankers = {
        "x": StubRanker("x", {"at://post/a": 2.0, "at://post/b": -1.0}),
        "y": StubRanker("y", {"at://post/a": 0.5, "at://post/b": 0.5}),
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

    # combined(a) = (3/4)*2.0 + (1/4)*0.5 = 1.625
    # combined(b) = (3/4)*-1.0 + (1/4)*0.5 = -0.625
    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(1.625)),
        ("at://post/b", 2, pytest.approx(-0.625)),
    ]


@pytest.mark.parametrize(
    ("purpose", "expected"),
    [
        (0.0, [("a", 0.8), ("b", 0.2)]),
        (0.5, [("a", 0.55), ("c", 0.5), ("b", 0.45)]),
        (1.0, [("b", 0.7), ("c", 0.5), ("a", 0.3)]),
    ],
)
def test_run_predict_preserves_unit_range_heavy_and_perspective_blend(
    monkeypatch, purpose, expected
):
    """Production model scores keep their scale, weights, and median fallback."""
    candidates = [CandidatePost(at_uri=f"at://post/{suffix}") for suffix in "abc"]
    rankers = {
        "heavy_ranker": StubRanker("heavy_ranker", {"at://post/a": 0.8, "at://post/b": 0.2}),
        "perspective": StubRanker(
            "perspective", {"at://post/a": 0.3, "at://post/b": 0.7, "at://post/c": 0.5}
        ),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="heavy_ranker", weight=1.0 - purpose),
                    RankModelSpec(name="perspective", weight=purpose),
                ],
                candidates=candidates,
            ),
            es=object(),
        )
    )

    assert [(r.at_uri, r.rank_score) for r in result.rankings] == [
        (f"at://post/{suffix}", pytest.approx(score)) for suffix, score in expected
    ]


def test_run_predict_uses_median_of_candidate_score_normalized_outputs(monkeypatch):
    candidates = [
        CandidatePost(at_uri="at://post/a", score=10.0),
        CandidatePost(at_uri="at://post/b", score=30.0),
        CandidatePost(at_uri="at://post/c", score=90.0),
        CandidatePost(at_uri="at://post/missing", score=None),
    ]
    rankers = {
        "candidate_score": CandidateScoreRanker(),
        "other": StubRanker(
            "other", {candidate.at_uri: 0.6 for candidate in candidates if candidate.at_uri}
        ),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="candidate_score", weight=1.0),
                    RankModelSpec(name="other", weight=1.0),
                ],
                candidates=candidates,
            ),
            es=object(),
        )
    )

    # The candidate-score outputs are 0, .25, and 1. Their median (.25)
    # fills the missing contribution, since the other model scored that URI.
    assert [(r.at_uri, r.rank_score) for r in result.rankings] == [
        ("at://post/c", pytest.approx(0.8)),
        ("at://post/b", pytest.approx(0.425)),
        ("at://post/missing", pytest.approx(0.425)),
        ("at://post/a", pytest.approx(0.3)),
    ]


def test_run_predict_keeps_zero_weight_model_transparency_only(monkeypatch):
    candidates = [
        CandidatePost(at_uri="at://post/active"),
        CandidatePost(at_uri="at://post/inactive-only"),
    ]
    rankers = {
        "active": StubRanker("active", {"at://post/active": 0.8}),
        "inactive": StubRanker(
            "inactive",
            {"at://post/active": 0.1, "at://post/inactive-only": 1.0},
        ),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        result = asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[
                        RankModelSpec(name="active", weight=1.0),
                        RankModelSpec(name="inactive", weight=0.0),
                    ],
                    candidates=candidates,
                ),
                es=object(),
            )
        )

    assert [(ranking.at_uri, ranking.rank_score) for ranking in result.rankings] == [
        ("at://post/active", pytest.approx(0.8))
    ]
    assert rec.model_scores == [
        ("active", 1.0, {"at://post/active": pytest.approx(0.8)}),
        (
            "inactive",
            0.0,
            {"at://post/active": pytest.approx(0.1)},
        ),
    ]


def test_run_predict_ignores_failure_from_zero_weight_model(monkeypatch):
    active = StubRanker("active", {"at://post/1": 0.8})
    rankers = {"active": active, "inactive": ExplodingRanker()}
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="active", weight=1.0),
                    RankModelSpec(name="inactive", weight=0.0),
                ],
                candidates=[CandidatePost(at_uri="at://post/1")],
            ),
            es=object(),
        )
    )

    assert [(ranking.at_uri, ranking.rank_score) for ranking in result.rankings] == [
        ("at://post/1", pytest.approx(0.8))
    ]


@pytest.mark.parametrize(
    ("politics", "expected"),
    [
        (
            0.5,
            [
                ("at://post/non-political", 1, pytest.approx(0.4)),
                ("at://post/partly-political", 2, pytest.approx(0.2625)),
                ("at://post/political", 3, pytest.approx(0.15)),
            ],
        ),
        (
            1.0,
            [
                ("at://post/non-political", 1, pytest.approx(0.4)),
                ("at://post/partly-political", 2, pytest.approx(0.35)),
                ("at://post/political", 3, pytest.approx(0.3)),
            ],
        ),
        (
            2.0,
            [
                ("at://post/political", 1, pytest.approx(0.6)),
                ("at://post/partly-political", 2, pytest.approx(0.525)),
                ("at://post/non-political", 3, pytest.approx(0.4)),
            ],
        ),
    ],
)
def test_run_predict_applies_politics_multiplier_after_combining_models(
    monkeypatch, politics, expected
):
    candidates = [
        CandidatePost(at_uri="at://post/non-political", politics_score=None),
        CandidatePost(at_uri="at://post/political", politics_score=1.0),
        CandidatePost(at_uri="at://post/partly-political", politics_score=0.5),
    ]
    ranker = StubRanker(
        "x",
        {
            "at://post/non-political": 0.4,
            "at://post/political": 0.3,
            "at://post/partly-political": 0.35,
        },
    )
    monkeypatch.setattr(predict_module, "get_ranker", lambda _name: ranker)

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[RankModelSpec(name="x", weight=1.0)],
                candidates=candidates,
                politics=politics,
            ),
            es=object(),
        )
    )

    assert [
        (ranking.at_uri, ranking.rank, ranking.rank_score) for ranking in result.rankings
    ] == expected


def test_run_predict_drops_candidates_with_no_valid_scores(monkeypatch):
    """Candidates with no valid score from any ranker are excluded."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 1.0}),
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
    ]


@pytest.mark.parametrize(
    ("score_a", "score_c", "expected"),
    [(10.0, 0.0, [7.5, 3.75, 0.0]), (-2.0, -6.0, [-1.5, -3.0, -4.5])],
)
def test_run_predict_uses_ranker_median_for_explicit_missing_score(
    monkeypatch, score_a, score_c, expected
):
    """A valid candidate missing one ranker's score gets that ranker's median score."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
        CandidatePost(at_uri="at://post/c", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": score_a, "at://post/c": score_c}),
        "y": StubRanker(
            "y",
            {"at://post/a": 0.0, "at://post/b": 0.0, "at://post/c": 0.0},
        ),
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

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(expected[0])),
        ("at://post/b", 2, pytest.approx(expected[1])),
        ("at://post/c", 3, pytest.approx(expected[2])),
    ]


@pytest.mark.parametrize(
    ("score_a", "score_c", "expected"),
    [(10.0, 0.0, [7.5, 3.75, 0.0]), (-2.0, -6.0, [-1.5, -3.0, -4.5])],
)
def test_run_predict_uses_ranker_median_for_omitted_score(
    monkeypatch, score_a, score_c, expected
):
    """Median fallback also applies when a ranker omits a still-valid candidate."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
        CandidatePost(at_uri="at://post/c", score=0.5),
    ]
    rankers = {
        "x": StubRanker(
            "x",
            {"at://post/a": score_a, "at://post/c": score_c},
            include_missing=False,
        ),
        "y": StubRanker(
            "y",
            {"at://post/a": 0.0, "at://post/b": 0.0, "at://post/c": 0.0},
        ),
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

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(expected[0])),
        ("at://post/b", 2, pytest.approx(expected[1])),
        ("at://post/c", 3, pytest.approx(expected[2])),
    ]


def test_run_predict_ignores_ranker_with_no_valid_scores(monkeypatch):
    """A ranker with no valid scores does not dilute weights or crash."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 1.0, "at://post/b": 0.0}),
        "empty": StubRanker("empty", {}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="x", weight=1.0),
                    RankModelSpec(name="empty", weight=9.0),
                ],
                candidates=candidates,
            ),
            es=object(),
        )
    )

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(1.0)),
        ("at://post/b", 2, pytest.approx(0.0)),
    ]


def test_run_predict_excludes_timed_out_ranker_without_losing_fast_ranker(monkeypatch):
    """A ranker that exceeds the per-model timeout is excluded from the
    combination (same as an empty-result ranker), and does not cause a
    concurrently-running fast ranker's results to be discarded."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 1.0, "at://post/b": 0.0}),
        "slow": TimingOutRanker("slow"),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(
                models=[
                    RankModelSpec(name="x", weight=1.0),
                    RankModelSpec(name="slow", weight=9.0),
                ],
                candidates=candidates,
            ),
            es=object(),
        )
    )

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(1.0)),
        ("at://post/b", 2, pytest.approx(0.0)),
    ]


def test_run_predict_wait_for_actually_times_out_a_hanging_ranker(monkeypatch):
    """Sanity check that a genuinely hanging ranker is bounded by
    `_RANK_MODEL_TIMEOUT_SEC` via `asyncio.wait_for`, not just handled when
    the exception is raised directly (as in the test above)."""
    monkeypatch.setattr(predict_module, "_RANK_MODEL_TIMEOUT_SEC", 0.01)
    candidates = [CandidatePost(at_uri="at://post/a", score=0.5)]
    rankers = {"slow": HangingRanker("slow")}
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="slow", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert result.rankings == []


def test_run_predict_records_empty_model_scores_for_empty_ranker(monkeypatch):
    """Feed debug includes configured rankers that had no valid scores."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 1.0, "at://post/b": 0.0}),
        "empty": StubRanker("empty", {}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[
                        RankModelSpec(name="x", weight=1.0),
                        RankModelSpec(name="empty", weight=9.0),
                    ],
                    candidates=candidates,
                ),
                es=object(),
            )
        )

    assert rec.model_scores == [
        ("x", 1.0, {"at://post/a": pytest.approx(1.0), "at://post/b": pytest.approx(0.0)}),
        ("empty", 9.0, {}),
    ]


def test_run_predict_returns_empty_when_no_ranker_has_valid_scores(monkeypatch):
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "empty": StubRanker("empty", {}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="empty", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert result.rankings == []


def test_run_predict_treats_zero_scores_as_valid(monkeypatch):
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 0.0}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="x", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert [(r.at_uri, r.rank, r.rank_score) for r in result.rankings] == [
        ("at://post/a", 1, pytest.approx(0.0)),
    ]


def test_run_predict_records_supplied_model_scores_and_weight(monkeypatch):
    """`record_model_scores` is invoked once per configured model with its
    weight and supplied per-uri scores — and the final combined
    score is not separately recorded there (it's captured via `record_ranking`
    by the caller)."""
    candidates = [
        CandidatePost(at_uri="at://post/a", score=0.5),
        CandidatePost(at_uri="at://post/b", score=0.5),
    ]
    rankers = {
        "x": StubRanker("x", {"at://post/a": 2.0, "at://post/b": 0.0}),
        "y": StubRanker("y", {"at://post/a": 0.5, "at://post/b": -0.5}),
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
        ("x", 2.0, {"at://post/a": pytest.approx(2.0), "at://post/b": pytest.approx(0.0)}),
        ("y", 1.0, {"at://post/a": pytest.approx(0.5), "at://post/b": pytest.approx(-0.5)}),
    ]


def test_run_predict_records_politics_score_adjustments(monkeypatch):
    candidates = [
        CandidatePost(at_uri="at://post/missing", politics_score=None),
        CandidatePost(at_uri="at://post/partial", politics_score=0.5),
        CandidatePost(at_uri="at://post/full", politics_score=1.0),
    ]
    ranker = StubRanker(
        "x",
        {
            "at://post/missing": 0.4,
            "at://post/partial": 0.3,
            "at://post/full": 0.2,
        },
    )
    monkeypatch.setattr(predict_module, "get_ranker", lambda _name: ranker)

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        asyncio.run(
            predict_module.run_predict(
                _request(
                    models=[RankModelSpec(name="x", weight=1.0)],
                    candidates=candidates,
                    politics=1.5,
                ),
                es=object(),
            )
        )

    assert rec.politics_setting == 1.5
    assert rec.politics_adjustments == [
        ("at://post/missing", None, 1.0, 0.4, 0.4),
        ("at://post/partial", 0.5, 1.25, 0.3, pytest.approx(0.375)),
        ("at://post/full", 1.0, 1.5, 0.2, pytest.approx(0.3)),
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
        "x": StubRanker("x", {"at://post/a": 0.5, "at://post/b": 0.5}),
    }
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: rankers[name])

    result = asyncio.run(
        predict_module.run_predict(
            _request(models=[RankModelSpec(name="x", weight=1.0)], candidates=candidates),
            es=object(),
        )
    )

    assert [r.at_uri for r in result.rankings] == ["at://post/a", "at://post/a", "at://post/b"]
    assert all(r.rank_score == pytest.approx(0.5) for r in result.rankings)
    assert [r.rank for r in result.rankings] == [1, 2, 3]


def test_rank_predict_request_requires_user_did():
    with pytest.raises(ValidationError, match="user_did"):
        RankPredictRequest(  # pyright: ignore[reportCallIssue]
            models=[RankModelSpec(name="heavy_ranker", weight=1.0)],
            candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
        )

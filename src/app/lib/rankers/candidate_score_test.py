"""Tests for the score-based fallback ranker."""

import asyncio

import pytest

from ...models import CandidatePost, RankedCandidate
from .candidate_score import CandidateScoreRanker


@pytest.fixture
def ranker():
    return CandidateScoreRanker()


def test_name(ranker):
    assert ranker.name == "candidate_score"


def test_predict_ranks_by_descending_score(ranker):
    result = asyncio.run(
        ranker.predict(
            es=None,
            user_did="user_did",
            candidates=[
                CandidatePost(
                    at_uri="at://post/low",
                    content=None,
                    minilm_l12_embedding=None,
                    score=0.1,
                    generator_name="random_posts",
                ),
                CandidatePost(
                    at_uri="at://post/high",
                    content=None,
                    minilm_l12_embedding=None,
                    score=0.9,
                    generator_name="popularity",
                ),
                CandidatePost(
                    at_uri="at://post/mid",
                    content=None,
                    minilm_l12_embedding=None,
                    score=0.4,
                    generator_name=None,
                ),
            ],
        )
    )

    assert [(ranking.at_uri, ranking.rank) for ranking in result.result.rankings] == [
        ("at://post/high", 1),
        ("at://post/mid", 2),
        ("at://post/low", 3),
    ]
    assert [ranking.rank_score for ranking in result.result.rankings] == pytest.approx(
        [1.0, 0.375, 0.0]
    )


def test_predict_ranks_places_candidates_without_scores_last(ranker):
    result = asyncio.run(
        ranker.predict(
            es=None,
            user_did="user_did",
            candidates=[
                CandidatePost(
                    at_uri="at://post/unscored-first",
                    content=None,
                    minilm_l12_embedding=None,
                    score=None,
                    generator_name="random_posts",
                ),
                CandidatePost(
                    at_uri="at://post/high",
                    content=None,
                    minilm_l12_embedding=None,
                    score=0.9,
                    generator_name="popularity",
                ),
                CandidatePost(
                    at_uri="at://post/unscored-second",
                    content=None,
                    minilm_l12_embedding=None,
                    score=None,
                    generator_name="random_posts",
                ),
                CandidatePost(
                    at_uri="at://post/mid",
                    content=None,
                    minilm_l12_embedding=None,
                    score=0.4,
                    generator_name=None,
                ),
            ],
        )
    )

    assert result.result.rankings == [
        RankedCandidate(at_uri="at://post/high", rank=1, rank_score=1.0),
        RankedCandidate(at_uri="at://post/mid", rank=2, rank_score=0.0),
        RankedCandidate(at_uri="at://post/unscored-first", rank=3, rank_score=None),
        RankedCandidate(at_uri="at://post/unscored-second", rank=4, rank_score=None),
    ]


@pytest.mark.parametrize(
    "scores, expected_scores",
    [
        ([10.0, 20.0, 50.0], [1.0, 0.25, 0.0]),
        ([-10.0, -2.0, -8.0], [1.0, 0.25, 0.0]),
        ([-10.0, 10.0, 5.0], [1.0, 0.75, 0.0]),
        ([0.2, 0.3, 0.6], [1.0, 0.25, 0.0]),
        ([5.0, 5.0, 5.0], [0.5, 0.5, 0.5]),
        ([0.0], [0.5]),
        ([None, 7.0], [0.5, None]),
        ([None, None], [None, None]),
        ([], []),
    ],
)
def test_predict_scales_each_batch(ranker, scores, expected_scores):
    candidates = [
        CandidatePost(at_uri=f"at://post/{index}", score=score)
        for index, score in enumerate(scores)
    ]

    result = asyncio.run(ranker.predict(es=None, user_did="user_did", candidates=candidates))

    assert [ranking.rank_score for ranking in result.result.rankings] == pytest.approx(
        expected_scores
    )
    assert [candidate.score for candidate in candidates] == scores


def test_predict_preserves_input_order_for_tied_scores_and_missing_scores(ranker):
    scores = [None, 8.0, 2.0, 8.0, None, 2.0]
    result = asyncio.run(
        ranker.predict(
            es=None,
            user_did="user_did",
            candidates=[
                CandidatePost(at_uri=f"at://post/{index}", score=score)
                for index, score in enumerate(scores)
            ],
        )
    )

    assert result.result.rankings == [
        RankedCandidate(at_uri="at://post/1", rank=1, rank_score=1.0),
        RankedCandidate(at_uri="at://post/3", rank=2, rank_score=1.0),
        RankedCandidate(at_uri="at://post/2", rank=3, rank_score=0.0),
        RankedCandidate(at_uri="at://post/5", rank=4, rank_score=0.0),
        RankedCandidate(at_uri="at://post/0", rank=5, rank_score=None),
        RankedCandidate(at_uri="at://post/4", rank=6, rank_score=None),
    ]

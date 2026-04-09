"""Tests for the score-based fallback ranker."""

import asyncio

import pytest

from ...models import CandidatePost, RankPredictRequest, RankedCandidate
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

    assert result.result.rankings == [
        RankedCandidate(at_uri="at://post/high", rank=1, rank_score=0.9),
        RankedCandidate(at_uri="at://post/mid", rank=2, rank_score=0.4),
        RankedCandidate(at_uri="at://post/low", rank=3, rank_score=0.1),
    ]

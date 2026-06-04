"""Tests for the shared ranker pipeline."""

import asyncio

import pytest
from pydantic import ValidationError

from ...models import CandidatePost, RankPredictRequest, RankPredictResult, RankedCandidate
from . import predict as predict_module
from .base import RankerExecutionError


class EchoRanker:
    @property
    def name(self) -> str:
        return "echo"

    async def predict(self, es, user_did, candidates):
        return type(
            "EchoResult",
            (),
            {
                "result": RankPredictResult(
                    rankings=[
                        RankedCandidate(
                            at_uri=candidate.at_uri,
                            rank=idx,
                            rank_score=candidate.score,
                        )
                        for idx, candidate in enumerate(candidates, start=1)
                    ]
                )
            },
        )()


class ExplodingRanker:
    @property
    def name(self) -> str:
        return "exploding"

    async def predict(self, es, user_did, candidates):
        raise RuntimeError("downstream boom")


def test_run_predict_wraps_unexpected_ranker_failure(monkeypatch):
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: ExplodingRanker())

    with pytest.raises(RankerExecutionError, match="Ranker 'exploding' failed: downstream boom"):
        asyncio.run(
            predict_module.run_predict(
                RankPredictRequest(
                    model="exploding",
                    user_did="did:plc:user1",
                    candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
                ),
                es=object(),
            )
        )


def test_run_predict_preserves_duplicate_candidates(monkeypatch):
    monkeypatch.setattr(predict_module, "get_ranker", lambda name: EchoRanker())

    result = asyncio.run(
        predict_module.run_predict(
            RankPredictRequest(
                model="echo",
                user_did="did:plc:user1",
                candidates=[
                    CandidatePost(at_uri="at://post/a", score=0.5),
                    CandidatePost(at_uri="at://post/a", score=0.9),
                    CandidatePost(at_uri="at://post/b", score=0.4),
                ],
            ),
            es=object(),
        )
    )

    assert result.rankings == [
        RankedCandidate(at_uri="at://post/a", rank=1, rank_score=0.5),
        RankedCandidate(at_uri="at://post/a", rank=2, rank_score=0.9),
        RankedCandidate(at_uri="at://post/b", rank=3, rank_score=0.4),
    ]


def test_rank_predict_request_requires_user_did():
    with pytest.raises(ValidationError, match="user_did"):
        RankPredictRequest(  # pyright: ignore[reportCallIssue]
            model="two_tower",
            candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
        )

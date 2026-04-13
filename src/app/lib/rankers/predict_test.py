"""Tests for ranker pipeline error handling."""

import asyncio

import pytest

from ...models import CandidatePost, RankPredictRequest
from . import predict as predict_module
from .base import RankerError, RankerExecutionError


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


def test_run_predict_requires_user_did_for_two_tower():
    with pytest.raises(RankerError, match="user_did is required for two_tower"):
        asyncio.run(
            predict_module.run_predict(
                RankPredictRequest(
                    model="two_tower",
                    candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
                ),
                es=object(),
            )
        )

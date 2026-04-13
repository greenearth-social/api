"""Tests for rank router error mapping."""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from ..lib.rankers import RankerExecutionError
from ..models import CandidatePost, RankPredictRequest
from . import rank as rank_module


def test_rank_predict_maps_ranker_execution_error_to_502(monkeypatch):
    async def fake_run_predict(payload, es):
        raise RankerExecutionError("two_tower", "downstream boom")

    monkeypatch.setattr(rank_module, "run_predict", fake_run_predict)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(es=object())))
    payload = RankPredictRequest(
        model="two_tower",
        user_did="did:plc:user1",
        candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(rank_module.rank_predict(request, payload))

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == "Ranker 'two_tower' failed: downstream boom"


def test_rank_predict_rejects_two_tower_without_user_did():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(es=object())))
    payload = RankPredictRequest(
        model="two_tower",
        candidates=[CandidatePost(at_uri="at://post/1", score=0.5)],
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(rank_module.rank_predict(request, payload))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "user_did is required for two_tower"

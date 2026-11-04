"""Scoring candidates against the LLM filter prompt (issue #566)."""

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from ...documents import LlmQueryVectorDocument
from ...models import CandidatePost
from ..embeddings import encode_float32_b64 as b64
from . import llm_filter


def _vector(**overrides) -> LlmQueryVectorDocument:
    fields = {
        "prompt_key": "f1",
        "user_did": "did:plc:user1",
        "query_vector": [1.0, 0.0],
        "intercept": 1.0,
        "role": "filter",
        "prompt": "less airplanes",
    }
    fields.update(overrides)
    return LlmQueryVectorDocument(**fields)


def test_scores_use_unit_embeddings_and_skip_unusable_candidates():
    # q = (9, 0), intercept 1: a post pointing along q predicts 10, one
    # orthogonal predicts 1, whatever the embedding's length.
    candidates = [
        CandidatePost(at_uri="at://aligned", minilm_l12_embedding=b64([5.0, 0.0])),
        CandidatePost(at_uri="at://orthogonal", minilm_l12_embedding=b64([0.0, 0.25])),
        CandidatePost(at_uri="at://diagonal", minilm_l12_embedding=b64([1.0, 1.0])),
        CandidatePost(at_uri="at://no-embedding"),
        CandidatePost(at_uri="at://wrong-dim", minilm_l12_embedding=b64([1.0, 0.0, 0.0])),
        CandidatePost(at_uri="at://not-base64", minilm_l12_embedding="!!"),
        CandidatePost(at_uri="at://zero", minilm_l12_embedding=b64([0.0, 0.0])),
        CandidatePost(at_uri=None, minilm_l12_embedding=b64([1.0, 0.0])),
    ]

    scores = llm_filter.llm_filter_scores(candidates, [9.0, 0.0], 1.0)

    assert scores == {
        "at://aligned": pytest.approx(1.0),
        "at://orthogonal": pytest.approx(0.0),
        "at://diagonal": pytest.approx((9 / 2**0.5) / 9),
        "at://zero": pytest.approx(0.0),
    }


def test_scores_empty_without_usable_candidates():
    assert llm_filter.llm_filter_scores([CandidatePost(at_uri="at://x")], [1.0], 1.0) == {}


@pytest.mark.parametrize(
    ("setting", "score", "expected"),
    [(2.0, 1.0, 2.0), (2.0, 0.5, 1.5), (2.0, 0.0, 1.0), (1.0, 1.0, 1.0)],
)
def test_multiplier_matches_the_politics_shape(setting, score, expected):
    assert llm_filter.llm_filter_multiplier(setting, score) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("db", "outcome", "expected_key"),
    [
        (None, None, None),
        (object(), _vector(), "f1"),
        (object(), None, None),
        (object(), RuntimeError("firestore down"), None),
    ],
    ids=["no-firestore", "active-filter", "no-vector", "error"],
)
def test_fetch_reads_the_filter_role_and_fails_open(monkeypatch, caplog, db, outcome, expected_key):
    from .. import llm_prompt_state
    from ..candidates import llm_query_vector as generator

    monkeypatch.setattr(generator, "_db", db)
    current = AsyncMock(
        side_effect=outcome if isinstance(outcome, Exception) else None, return_value=outcome
    )
    monkeypatch.setattr(llm_prompt_state, "get_current_llm_query_vector", current)

    with caplog.at_level(logging.WARNING, logger="app.lib.rankers.llm_filter"):
        fetched = asyncio.run(llm_filter.fetch_active_llm_filter("did:plc:user1"))

    assert (fetched.prompt_key if fetched else None) == expected_key
    if db is not None:
        current.assert_awaited_once_with(db, "did:plc:user1", role="filter")
    assert ("ranking unfiltered" in caplog.text) == isinstance(outcome, Exception)

"""Regression tests for document imports and shared candidate schemas."""

import subprocess
import sys
from pathlib import Path

import pytest

from .documents import FeedDebugDocument
from .lib.candidates.base import CandidateResult as GeneratorCandidateResult
from .models import CandidatePost, CandidateResult


@pytest.mark.parametrize(
    "module_name",
    ["app.lib.firestore", "app.documents", "app.lib.user_history_cache"],
)
def test_document_dependencies_import_in_fresh_process(module_name):
    # conftest imports app.main first; a fresh process exposes import cycles
    # that pytest's already-populated module cache would otherwise conceal.
    result = subprocess.run(
        [sys.executable, "-c", f"import {module_name}"],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_feed_debug_candidate_results_preserve_schema_and_round_trip():
    assert GeneratorCandidateResult is CandidateResult
    assert "CandidateResult" in FeedDebugDocument.model_json_schema()["$defs"]

    document = FeedDebugDocument.model_validate(
        {
            "request_id": "request-1",
            "user_did": "did:plc:user",
            "feed_name": "your-feed",
            "expires_at": "2026-10-15T00:00:00Z",
            "generate_request": {
                "generators": [{"name": "two_tower"}],
                "user_did": "did:plc:user",
            },
            "generator_outputs": [
                {
                    "generator_name": "two_tower",
                    "candidates": [{"at_uri": "at://post/1", "score": 0.5}],
                    "reason": "empty_history",
                    "mode": "fallback",
                }
            ],
        }
    )

    result = document.generator_outputs[0]
    assert isinstance(result, CandidateResult)
    assert isinstance(result.candidates[0], CandidatePost)
    assert result.candidates[0].score == 0.5
    assert result.reason == "empty_history"
    assert result.mode == "fallback"
    assert FeedDebugDocument.model_validate_json(document.model_dump_json()) == document

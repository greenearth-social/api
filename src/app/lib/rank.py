"""Shared ranking helpers used by the `/rank` API and internal callers.

This module currently exposes a simple score-based fallback ranker so the API
surface is in place before the engagement-prediction inference-service-backed
ranker is wired in.
"""

import logging

from ..models import (
    CandidatePost,
    RankModel,
    RankedCandidate,
    RankPredictRequest,
    RankPredictResult,
)

logger = logging.getLogger(__name__)

DEFAULT_RANK_MODEL = "candidate_score"

_MODELS: dict[str, RankModel] = {
    DEFAULT_RANK_MODEL: RankModel(
        name=DEFAULT_RANK_MODEL,
        ready=True,
        metadata={
            "kind": "fallback",
            "description": "Ranks candidates by their existing score field until inference-service ranking is added.",
        },
    ),
}


class RankModelNotFoundError(Exception):
    """Raised when a requested rank model does not exist."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Rank model not found: {name}")


class RankerError(Exception):
    """Raised when ranking cannot be completed for a valid request."""


def list_models() -> list[RankModel]:
    """Return the ranking models currently exposed by this service."""
    return list(_MODELS.values())


def _metadata_for_candidate(candidate: CandidatePost) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if candidate.generator_name is not None:
        metadata["generator_name"] = candidate.generator_name
    if candidate.content is not None:
        metadata["content"] = candidate.content
    return metadata


def _dedup_candidates(candidates: list[CandidatePost]) -> list[CandidatePost]:
    """Remove duplicate candidates by `at_uri`, keeping the first occurrence."""
    seen: set[str] = set()
    deduped: list[CandidatePost] = []
    for candidate in candidates:
        if candidate.at_uri is None:
            raise RankerError("All candidates must include at_uri")
        if candidate.at_uri in seen:
            continue
        seen.add(candidate.at_uri)
        deduped.append(candidate)
    return deduped


async def run_predict(request: RankPredictRequest) -> RankPredictResult:
    """Rank the supplied candidates.

    The current implementation uses the candidate `score` field as a stable
    fallback. A future implementation will call the engagement-prediction
    inference service and preserve that API contract.
    """
    model_name = request.model or DEFAULT_RANK_MODEL
    if model_name not in _MODELS:
        raise RankModelNotFoundError(model_name)

    candidates = _dedup_candidates(request.candidates)
    ranked_candidates = sorted(
        enumerate(candidates),
        key=lambda item: (
            -(item[1].score if item[1].score is not None else float("-inf")),
            item[0],
        ),
    )

    rankings: list[RankedCandidate] = []
    ranked_at_uris: list[str] = []
    for rank_idx, (_, candidate) in enumerate(ranked_candidates, start=1):
        assert candidate.at_uri is not None
        ranked_at_uris.append(candidate.at_uri)
        rankings.append(
            RankedCandidate(
                at_uri=candidate.at_uri,
                rank=rank_idx,
                score=candidate.score,
                metadata=_metadata_for_candidate(candidate),
            )
        )

    return RankPredictResult(
        model=model_name,
        ranked_at_uris=ranked_at_uris,
        rankings=rankings,
    )

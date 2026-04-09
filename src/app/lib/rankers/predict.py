"""Shared ranking pipeline.

Given a `RankPredictRequest`, resolves a named ranker, de-duplicates candidate
posts, and returns the ranker's ordered output.
"""

from ...models import CandidatePost, RankPredictRequest, RankPredictResult
from .base import get_ranker, list_rankers
from ..candidates.generate import dedup_candidates

DEFAULT_RANK_MODEL = "candidate_score"


class RankModelNotFoundError(Exception):
    """Raised when a requested rank model does not exist."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Rank model not found: {name}")


class RankerError(Exception):
    """Raised when ranking cannot be completed for a valid request."""


async def run_predict(
    request: RankPredictRequest,
    es,
    *,
    swallow_errors: bool = False,
) -> RankPredictResult:
    """Rank the supplied candidates using the requested ranker."""
    model_name = request.model or DEFAULT_RANK_MODEL
    ranker = get_ranker(model_name)
    if ranker is None:
        raise RankModelNotFoundError(model_name)

    if any(candidate.at_uri is None for candidate in request.candidates):
        raise RankerError("All candidates must include at_uri")

    deduped_candidates = dedup_candidates(request.candidates)
    result = await ranker.predict(es, request.user_did, deduped_candidates)
    return result.result

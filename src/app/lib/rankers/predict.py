"""Shared ranking pipeline.

Given a `RankPredictRequest`, resolves a named ranker and returns the ranker's
ordered output.
"""

import logging

from ...models import RankPredictRequest, RankPredictResult
from .base import RankerError, RankerExecutionError, get_ranker

DEFAULT_RANK_MODEL = "candidate_score"

logger = logging.getLogger(__name__)


class RankModelNotFoundError(Exception):
    """Raised when a requested rank model does not exist."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Rank model not found: {name}")


async def run_predict(
    request: RankPredictRequest,
    es,
) -> RankPredictResult:
    """Rank the supplied candidates using the requested ranker."""
    model_name = request.model or DEFAULT_RANK_MODEL
    ranker = get_ranker(model_name)
    if ranker is None:
        raise RankModelNotFoundError(model_name)

    if any(candidate.at_uri is None for candidate in request.candidates):
        raise RankerError("All candidates must include at_uri")

    try:
        result = await ranker.predict(es, request.user_did, request.candidates)
    except RankerError:
        raise
    except RankerExecutionError:
        raise
    except Exception as exc:
        logger.exception("Ranker '%s' failed", model_name)
        raise RankerExecutionError(model_name, str(exc) or exc.__class__.__name__) from exc
    return result.result

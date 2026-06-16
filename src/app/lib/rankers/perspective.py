"""Perspective API ranker.

Scores each candidate's text content for conversational quality via the
Perspective API (see :mod:`app.lib.perspective`) and exposes the raw PRC
scores as a `Ranker` so they can be normalized and combined with other rank
models (e.g. the two-tower model) by `run_predict`.
"""

import logging

from ...models import CandidatePost, RankedCandidate, RankPredictResult
from ..perspective import _PRC_WEIGHTS, _weighted_score_bounds, score_candidates
from .base import Ranker, RankerResult

logger = logging.getLogger(__name__)

PERSPECTIVE_MODEL_NAME = "perspective"


class PerspectiveRanker(Ranker):
    """Rank posts by Perspective API PRC score."""

    @property
    def name(self) -> str:
        return PERSPECTIVE_MODEL_NAME

    @property
    def score_bounds(self) -> tuple[float, float]:
        return _weighted_score_bounds(_PRC_WEIGHTS)

    async def predict(
        self,
        es,
        user_did: str,
        candidates: list[CandidatePost],
    ) -> RankerResult:
        valid_candidates = [(c, c.at_uri) for c in candidates if c.at_uri is not None]
        scores = await score_candidates([c for c, _at_uri in valid_candidates])

        def _sort_key(item: tuple[int, tuple[CandidatePost, str]]) -> tuple[bool, float, int]:
            idx, (_candidate, at_uri) = item
            score = scores.get(at_uri)
            return (score is None, -(score if score is not None else 0.0), idx)

        ranked_candidates = sorted(
            enumerate(valid_candidates),
            key=_sort_key,
        )

        rankings: list[RankedCandidate] = []
        for rank_idx, (_, (_candidate, at_uri)) in enumerate(ranked_candidates, start=1):
            rankings.append(
                RankedCandidate(
                    at_uri=at_uri,
                    rank=rank_idx,
                    rank_score=scores.get(at_uri),
                )
            )

        return RankerResult(model=self.name, result=RankPredictResult(rankings=rankings))

"""Score-based fallback ranker.

Ranks candidates using their existing `score` field. This is a temporary
fallback until inference-service-backed ranking is wired in.
"""

from ...models import RankedCandidate, CandidatePost, RankPredictResult
from .base import Ranker, RankerResult


class CandidateScoreRanker(Ranker):
    """Fallback ranker that orders candidates by their existing score."""

    @property
    def name(self) -> str:
        return "candidate_score"

    async def predict(
        self, 
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
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
                    rank_score=candidate.score,
                )
            )

        result = RankPredictResult(
            rankings=rankings,
        )
        return RankerResult(model=self.name, result=result)

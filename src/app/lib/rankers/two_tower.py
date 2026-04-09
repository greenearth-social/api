"""Two-tower model ranker.

Retrieves a user history, and calls inference on a user tower to get a final user embedding.
Also calls inference on a post tower for each post to get final post embeddings.
Performs vector-matrix multiplication to get scores for each post, and returns the posts in order.
"""

import httpx

from ...models import RankedCandidate, CandidatePost, RankPredictResult
from .base import Ranker, RankerResult
from ..elasticsearch import fetch_post_embeddings

import logging

logger = logging.getLogger(__name__)

INFERENCE_BASE_URL = "http://127.0.0.1:8080"
INFERENCE_API_KEY = "dave-dev-key"

async def predict_post_tower(post_embeddings: list[list[float]]) -> list[list[float]]:
    url = f"{INFERENCE_BASE_URL}/models/post-tower/predict"
    headers = {"X-API-Key": "dave-dev-key"}
    payload = {"post_embeddings": post_embeddings}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        return data["outputs"]
    


class TwoTowerRanker(Ranker):
    """Rank posts relative to a user using a two-tower model."""

    @property
    def name(self) -> str:
        return "two_tower"

    async def predict(
        self, 
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        
        # Get the embeddings for all the posts
        candidate_uris = [c.at_uri for c in candidates]
        input_post_embeddings = await fetch_post_embeddings(es, candidate_uris)
        
        if not input_post_embeddings:
            logger.info(
                "No embeddings found for %d liked posts of user %s",
                len(candidate_uris),
                user_did,
            )
            return RankPredictResult(rankings=[])

        # Call the post tower for the whole batch
        output_post_embeddings = await predict_post_tower(input_post_embeddings)
        
        # for now just take the sum, still need to add in user and dot product
        final_scores = [ sum(pe) for pe in output_post_embeddings ]

        candidates_with_scores = zip(candidates, final_scores)
        ranked_candidates = sorted(
            enumerate(candidates_with_scores), # (index, (candidate, score))
            key=lambda item: (
                -(item[1][1] if item[1][1] is not None else float("-inf")),
                item[0],
            ),
        )

        rankings: list[RankedCandidate] = []
        for rank_idx, (_, (candidate, score)) in enumerate(ranked_candidates, start=1):
            assert candidate.at_uri is not None
            rankings.append(
                RankedCandidate(
                    at_uri=candidate.at_uri,
                    rank=rank_idx,
                    rank_score=score,
                )
            )

        result = RankPredictResult(
            rankings=rankings,
        )
        return RankerResult(model=self.name, result=result)

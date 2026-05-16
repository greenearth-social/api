"""Two-tower model ranker.

Retrieves a user history, and calls inference on a user tower to get a final user embedding.
Also calls inference on a post tower for each post to get final post embeddings.
Performs vector-matrix multiplication to get scores for each post, and returns the posts in order.
"""

import logging
import os

import httpx

from ...models import RankedCandidate, CandidatePost, RankPredictResult
from .base import Ranker, RankerExecutionError, RankerResult
from ..elasticsearch import fetch_post_embeddings, fetch_recent_liked_post_uris


logger = logging.getLogger(__name__)
TWO_TOWER_MODEL_NAME = "two_tower"

def get_inference_settings() -> tuple[str, str]:
    """Load inference configuration only when the two-tower ranker is used."""
    base_url = os.environ.get("GE_INFERENCE_BASE_URL", "").rstrip("/")
    if not base_url:
        raise RankerExecutionError(
            TWO_TOWER_MODEL_NAME,
            "GE_INFERENCE_BASE_URL environment variable is required",
        )

    api_key = os.environ.get("GE_INFERENCE_API_KEY")
    if not api_key:
        raise RankerExecutionError(
            TWO_TOWER_MODEL_NAME,
            "GE_INFERENCE_API_KEY environment variable is required",
        )

    return base_url, api_key


async def predict_post_tower_batch(
    post_embeddings: list[list[float]],
    *,
    base_url: str,
    api_key: str,
) -> list[list[float]]:
    url = f"{base_url}/models/post-tower/predict"
    headers = {"X-API-Key": api_key}
    payload = {"post_embeddings": post_embeddings}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers) # type: ignore
        resp.raise_for_status()
        data = resp.json()
        return data["outputs"]


async def predict_user_tower_single(
    history_embeddings: list[list[float]],
    *,
    base_url: str,
    api_key: str,
) -> list[list[float]]:
    url = f"{base_url}/models/user-tower/predict"
    headers = {"X-API-Key": api_key}
    payload = {"history_embeddings": history_embeddings}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers) # type: ignore
        resp.raise_for_status()
        data = resp.json()
        return data["outputs"]


class TwoTowerRanker(Ranker):
    """Rank posts relative to a user using a two-tower model."""

    @property
    def name(self) -> str:
        return TWO_TOWER_MODEL_NAME


    async def predict(
        self, 
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        inference_base_url, inference_api_key = (
            get_inference_settings()
        )
        
        ####### USER #######
        # 1. Get recently liked post URIs
        user_history_vectors = []
        user_history_liked_uris = await fetch_recent_liked_post_uris(es, user_did)

        if not user_history_liked_uris:
            logger.info("No likes found for user %s", user_did)
        else:
            # 2. Fetch embeddings for those posts
            user_history_embedding_pairs: list[tuple[str, list[float]]] = await fetch_post_embeddings(es, user_history_liked_uris)

            if not user_history_embedding_pairs:
                logger.info(
                    "No embeddings found for %d liked posts of user %s",
                    len(user_history_liked_uris),
                    user_did,
                )
            else:
                user_history_vectors = [embedding for _, embedding in user_history_embedding_pairs]
        
        # Call the inference API for the user tower
        output_user_embedding_list = await predict_user_tower_single(
            user_history_vectors,
            base_url=inference_base_url,
            api_key=inference_api_key,
        )
        if len(output_user_embedding_list) != 1:
            raise RankerExecutionError(
                self.name,
                f"user inference returned {len(output_user_embedding_list)} embeddings; expected 1",
            )
        output_user_embedding = output_user_embedding_list[0]

        ####### CANDIATE POSTS #######
        valid_candidates = [candidate for candidate in candidates if candidate.at_uri is not None]
        candidates_by_uri = {candidate.at_uri: candidate for candidate in candidates if candidate.at_uri is not None}
        
        # Get the embeddings for all the posts
        candidate_embedding_pairs = await fetch_post_embeddings(es, list(candidates_by_uri))
        if not candidate_embedding_pairs:
            logger.info(
                "No embeddings found for %d candidate posts of user %s",
                len(candidates_by_uri),
                user_did,
            )
            rankings = [
                RankedCandidate(
                    at_uri=candidate.at_uri,
                    rank=rank_idx,
                    rank_score=None,
                )
                for rank_idx, candidate in enumerate(valid_candidates, start=1)
                if candidate.at_uri is not None
            ]
            return RankerResult(model=self.name, result=RankPredictResult(rankings=rankings))

        ranked_candidates_input = [
            candidates_by_uri[at_uri]
            for at_uri, _ in candidate_embedding_pairs
            if at_uri in candidates_by_uri
        ]
        input_post_embeddings = [
            embedding for _, embedding in candidate_embedding_pairs
        ]

        # Call the post tower for the whole batch
        output_post_embeddings = await predict_post_tower_batch(
            input_post_embeddings,
            base_url=inference_base_url,
            api_key=inference_api_key,
        )
        if len(output_post_embeddings) != len(ranked_candidates_input):
            raise RankerExecutionError(
                self.name,
                "post inference returned a different number of embeddings than requested",
            )

        # For each candidate post, take the dot product of its output embedding with the user output embedding
        final_scores = []
        for post_embedding in output_post_embeddings:
            if len(post_embedding) != len(output_user_embedding):
                raise RankerExecutionError(
                    self.name,
                    f"embedding dimension mismatch: post={len(post_embedding)} user={len(output_user_embedding)}",
                )
            final_scores.append(sum([ u*p for u,p in zip(post_embedding, output_user_embedding)]))

        # Rank by the final scores, breaking ties by original order in candidates list
        candidates_with_scores = zip(ranked_candidates_input, final_scores)
        ranked_candidates = sorted(
            enumerate(candidates_with_scores), # (index, (candidate, score))
            key=lambda item: (
                -(item[1][1] if item[1][1] is not None else float("-inf")),
                item[0],
            ),
        )

        # Get in correct output format
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

        ranked_uris = {ranking.at_uri for ranking in rankings}
        for candidate in valid_candidates:
            if candidate.at_uri is None or candidate.at_uri in ranked_uris:
                continue
            rankings.append(
                RankedCandidate(
                    at_uri=candidate.at_uri,
                    rank=len(rankings) + 1,
                    rank_score=None,
                )
            )

        result = RankPredictResult(
            rankings=rankings,
        )
        return RankerResult(model=self.name, result=result)

"""Two-tower model ranker.

Retrieves a user history, and calls inference on a user tower to get a final user embedding.
Also calls inference on a post tower for each post to get final post embeddings.
Performs vector-matrix multiplication to get scores for each post, and returns the posts in order.
"""

import asyncio
import logging

from ...models import RankedCandidate, CandidatePost, RankPredictResult
from .base import Ranker, RankerExecutionError, RankerResult
from ..elasticsearch import fetch_post_embeddings_and_authors
from ..embeddings import decode_float32_b64
from ..http_client import get_http_client
from ..telemetry import timed
from ..inference import (
    get_inference_settings,
    build_inference_headers,
    raise_inference_response_error,
    compute_user_embedding,
)


logger = logging.getLogger(__name__)
TWO_TOWER_MODEL_NAME = "two_tower"


POST_TOWER_BATCH_SIZE = 32


async def predict_post_tower_batch(
    post_embeddings: list[list[float]],
    author_dids: list[str],
    *,
    base_url: str,
    api_key: str,
) -> list[list[float]]:
    url = f"{base_url}/models/post-tower/predict"
    headers = build_inference_headers(api_key)

    if len(post_embeddings) != len(author_dids):
        raise RankerExecutionError(
            TWO_TOWER_MODEL_NAME,
            f"number of post embeddings {len(post_embeddings)} does not match number of author DIDs {len(author_dids)}",
        )

    embeddings_and_authors = list(zip(post_embeddings, author_dids))
    chunks = [
        embeddings_and_authors[i : i + POST_TOWER_BATCH_SIZE]
        for i in range(0, len(post_embeddings), POST_TOWER_BATCH_SIZE)
    ]

    client = get_http_client()

    async def _call_chunk(chunk: list[tuple[list[float], str]]) -> list[list[float]]:
        resp = await client.post(
            url, 
            json={
                "post_embeddings": [emb for emb, _ in chunk],
                "target_author_dids": [author_did for _, author_did in chunk]
            },
            headers=headers,
        )
        if resp.is_error:
            logger.error(
                "post-tower predict failed status=%s body=%s",
                resp.status_code,
                resp.text,
            )
            raise_inference_response_error("post-tower", resp.status_code, resp.text)
        return resp.json()["outputs"]

    async with timed(
        logger, "post_tower_http", n_posts=len(post_embeddings), n_chunks=len(chunks)
    ):
        chunk_outputs = await asyncio.gather(*(_call_chunk(c) for c in chunks))
    return [item for chunk_out in chunk_outputs for item in chunk_out]


class TwoTowerRanker(Ranker):
    """Rank posts relative to a user using a two-tower model."""

    @property
    def name(self) -> str:
        return TWO_TOWER_MODEL_NAME

    @property
    def score_bounds(self) -> tuple[float, float]:
        # User tower and post tower each L2-normalize the output embeddings 
        # (they sum to 1), so the dot product is in the range [-1,1]. 
        # Said another way, the two tower performs cosine similarity.
        return (-1.0, 1.0)

    async def predict(
        self,
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        inference_base_url, inference_api_key = (
            get_inference_settings()
        )

        valid_candidates = [candidate for candidate in candidates if candidate.at_uri is not None]
        candidates_by_uri = {candidate.at_uri: candidate for candidate in candidates if candidate.at_uri is not None}

        async def _compute_candidate_post_embeddings() -> (
            tuple[list[CandidatePost], list[list[float]]] | None
        ):
            async with timed(
                logger, "two_tower_post_side", n_candidates=len(candidates_by_uri)
            ):
                # Use embeddings already carried on CandidatePost when available (avoids an ES round-trip).
                uris_embs_authors: list[tuple[str, list[float], str]] = []
                missing_uris: list[str] = []
                for uri, candidate in candidates_by_uri.items():
                    if candidate.minilm_l12_embedding and candidate.author_did:
                        try:
                            vec = decode_float32_b64(candidate.minilm_l12_embedding)
                            uris_embs_authors.append((uri, vec, candidate.author_did))
                            continue
                        except Exception:
                            pass
                    missing_uris.append(uri)

                if missing_uris:
                    fetched = await fetch_post_embeddings_and_authors(es, missing_uris)
                    uris_embs_authors.extend(fetched)

                if not uris_embs_authors:
                    return None

                ranked_candidates_input = [
                    candidates_by_uri[at_uri]
                    for at_uri, _, _ in uris_embs_authors
                    if at_uri in candidates_by_uri
                ]
                input_post_embeddings = [
                    embedding for _, embedding, _ in uris_embs_authors
                ]
                author_dids = [
                    author_did for _, _, author_did in uris_embs_authors
                ]

                output_post_embeddings = await predict_post_tower_batch(
                    input_post_embeddings,
                    author_dids,
                    base_url=inference_base_url,
                    api_key=inference_api_key,
                )
                if len(output_post_embeddings) != len(ranked_candidates_input):
                    raise RankerExecutionError(
                        self.name,
                        "post inference returned a different number of embeddings than requested",
                    )
                return ranked_candidates_input, output_post_embeddings

        output_user_embedding, candidate_result = await asyncio.gather(
            compute_user_embedding(
                user_did,
                es,
                inference_base_url,
                inference_api_key,
                TWO_TOWER_MODEL_NAME
            ),
            _compute_candidate_post_embeddings(),
        )

        if candidate_result is None:
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

        ranked_candidates_input, output_post_embeddings = candidate_result

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

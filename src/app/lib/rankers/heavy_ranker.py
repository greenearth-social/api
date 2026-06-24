"""Heavy ranker model.
"""

import asyncio
import logging

from ...models import RankedCandidate, CandidatePost, RankPredictResult
from .base import Ranker, RankerResult
from ..elasticsearch import (
    fetch_post_embeddings_and_authors,
    fetch_recent_liked_post_uris_and_times,
)
from ..embeddings import decode_float32_b64
from ..telemetry import timed
from ..inference import (
    get_inference_settings,
    predict_heavy_ranker_single_user,
)
from ..feed_debug import current_recorder
from .utils import get_rank_predict_results_from_candidates_and_scores

logger = logging.getLogger(__name__)
HEAVY_RANKER_MODEL_NAME = "heavy_ranker"


class HeavyRanker(Ranker):
    """Rank candidate posts relative to a user using an ML model."""

    @property
    def name(self) -> str:
        return HEAVY_RANKER_MODEL_NAME

    @property
    def score_bounds(self) -> tuple[float, float]:
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

        async def _get_user_features() -> tuple[list[list[float]], list[str], list[str]]:
            async with timed(logger, "ranker_get_user_features", user_did=user_did):
                user_history_vectors: list[list[float]] = []
                history_author_dids: list[str] = []
                filtered_history_liked_at_times: list[str] = []
                user_history_liked_uris, history_liked_at_times = await fetch_recent_liked_post_uris_and_times(es, user_did)

                rec = current_recorder()

                if not user_history_liked_uris:
                    logger.info("No likes found for user %s", user_did)
                    if rec is not None:
                        rec.record_user_features(HEAVY_RANKER_MODEL_NAME, [], 0)
                else:
                    user_history_embedding_pairs: list[tuple[str, list[float], str]] = await fetch_post_embeddings_and_authors(
                        es, user_history_liked_uris,
                    )
                    if rec is not None:
                        rec.record_user_features(
                            HEAVY_RANKER_MODEL_NAME, user_history_liked_uris, len(user_history_embedding_pairs)
                        )
                    if not user_history_embedding_pairs:
                        logger.info(
                            "No embeddings found for %d liked posts of user %s",
                            len(user_history_liked_uris),
                            user_did,
                        )
                    else:
                        user_history_vectors = [embedding for _, embedding, _ in user_history_embedding_pairs]
                        history_author_dids = [author_did for _, _, author_did in user_history_embedding_pairs]
                        filtered_history_uris = [uri for uri, _, _ in user_history_embedding_pairs]
                        filtered_history_liked_at_times = [
                            liked_at_time
                            for uri, liked_at_time in zip(user_history_liked_uris, history_liked_at_times)
                            if uri in filtered_history_uris
                        ]

                return user_history_vectors, history_author_dids, filtered_history_liked_at_times
        # end _get_user_features()


        async def _get_candidate_features() -> (
            tuple[list[CandidatePost], list[list[float]], list[str]] | None
        ):
            async with timed(
                logger, "ranker_get_candidate_features", n_candidates=len(candidates_by_uri)
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
                    fetched = await fetch_post_embeddings_and_authors(es, missing_uris, index="posts_recent")
                    uris_embs_authors.extend(fetched)

                if not uris_embs_authors:
                    return None

                candidates_with_embeddings = [
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
                return candidates_with_embeddings, input_post_embeddings, author_dids
        # end _get_candidate_features()


        candidates_by_uri = {candidate.at_uri: candidate for candidate in candidates if candidate.at_uri is not None}

        user_features, candidate_features = await asyncio.gather(
            _get_user_features(),
            _get_candidate_features(),
        )

        history_embeddings, history_author_dids, history_liked_at_times = user_features

        def _return_empty_ranker_result(msg: str):
            logger.info(msg)
            rankings = [
                RankedCandidate(
                    at_uri=candidate.at_uri,
                    rank=rank_idx,
                    rank_score=None,
                )
                for rank_idx, candidate in enumerate(candidates_by_uri.values(), start=1)
                if candidate.at_uri is not None
            ]
            return RankerResult(model=self.name, result=RankPredictResult(rankings=rankings))

        if candidate_features is None:
            return _return_empty_ranker_result(
                f"No valid features found for any of {len(candidates_by_uri)} candidate posts of user {user_did}"
            )
        candidate_posts, candidate_post_embeddings, candidate_author_dids = candidate_features

        ranker_outputs = await predict_heavy_ranker_single_user(
            history_embeddings,
            history_author_dids,
            history_liked_at_times,
            candidate_post_embeddings,
            candidate_author_dids,
            base_url=inference_base_url,
            api_key=inference_api_key
        )

        if not ranker_outputs:
            return _return_empty_ranker_result(
                f"No ranker outputs for any of {len(candidates_by_uri)} candidate posts of user {user_did}"
            )
        if len(ranker_outputs) != len(candidate_posts):
            return _return_empty_ranker_result(
                f"Heavy ranker returned {len(ranker_outputs)} results but {len(candidate_posts)} were requested."
            )

        result = get_rank_predict_results_from_candidates_and_scores(
            candidate_posts,
            ranker_outputs,
            candidates_by_uri.values()
        )

        return RankerResult(model=self.name, result=result)

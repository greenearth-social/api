"""Heavy ranker model.
"""

import asyncio
import logging
from typing import assert_never

from ...models import CandidatePost, RankedCandidate, RankPredictResult
from ..embeddings import decode_float32_b64
from ..elasticsearch import fetch_post_embeddings_and_metadata
from ..feed_debug import current_recorder
from ..inference import (
    HistoryMode,
    get_inference_settings,
    predict_heavy_ranker_single_user,
)
from ..telemetry import timed
from ..user_history import fetch_user_history_features
from .base import Ranker, RankerResult
from .utils import get_rank_predict_results_from_candidates_and_scores

logger = logging.getLogger(__name__)


class HeavyRanker(Ranker):
    """Rank candidate posts relative to a user using an ML model."""

    def __init__(self, name: str, history_mode: HistoryMode):
        self._name = name
        self.history_mode: HistoryMode = history_mode

    @property
    def name(self) -> str:
        return self._name

    @property
    def score_bounds(self) -> tuple[float, float]:
        return (0.0, 1.0)

    async def predict(
        self,
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        inference_base_url, inference_api_key = (
            get_inference_settings()
        )

        async def _get_user_features() -> tuple[list[list[float]], list[str], list[str], list[int]]:
            rec = current_recorder()

            match self.history_mode:
                case "actual":
                    async with timed(logger, "ranker_get_user_features", user_did=user_did):
                        user_history_vectors: list[list[float]] = []
                        history_author_dids: list[str] = []
                        filtered_history_liked_at_times: list[str] = []
                        history_like_counts: list[int] = []
                        user_history = await fetch_user_history_features(es, user_did)
                        user_history_liked_uris = user_history.liked_uris

                        if not user_history_liked_uris:
                            logger.info("No likes found for user %s", user_did)
                            if rec is not None:
                                rec.record_user_features(self._name, [], 0)
                        else:
                            embedded_history = user_history.items_with_embeddings
                            if rec is not None:
                                rec.record_user_features(
                                    self._name, user_history_liked_uris, len(embedded_history)
                                )
                            if not embedded_history:
                                logger.info(
                                    "No embeddings found for %d liked posts of user %s",
                                    len(user_history_liked_uris),
                                    user_did,
                                )
                            else:
                                user_history_vectors = [
                                    item.embedding
                                    for item in embedded_history
                                    if item.embedding is not None
                                ]
                                history_author_dids = [
                                    item.author_did for item in embedded_history
                                ]
                                history_like_counts = [
                                    item.like_count for item in embedded_history
                                ]
                                filtered_history_liked_at_times = [
                                    item.liked_at for item in embedded_history
                                ]

                        return (
                            user_history_vectors,
                            history_author_dids,
                            filtered_history_liked_at_times,
                            history_like_counts,
                        )
                case "empty":
                    if rec is not None:
                        rec.record_user_features(self._name, [], 0)
                    return [], [], [], []
                case _:
                    assert_never(self.history_mode)
        # end _get_user_features()


        async def _get_candidate_features() -> (
            tuple[list[CandidatePost], list[list[float]], list[str], list[int]] | None
        ):
            async with timed(
                logger, "ranker_get_candidate_features", n_candidates=len(candidates_by_uri)
            ):
                # Use embeddings already carried on CandidatePost when available (avoids an ES round-trip).
                uris_and_metadata: list[tuple[str, list[float], str, int]] = []
                missing_uris: list[str] = []
                for uri, candidate in candidates_by_uri.items():
                    if candidate.minilm_l12_embedding and candidate.author_did:
                        try:
                            vec = decode_float32_b64(candidate.minilm_l12_embedding)
                            uris_and_metadata.append((uri, vec, candidate.author_did, candidate.like_count or 0))
                            continue
                        except Exception:
                            pass
                    missing_uris.append(uri)

                if missing_uris:
                    fetched = await fetch_post_embeddings_and_metadata(es, missing_uris, index="posts_recent")
                    uris_and_metadata.extend(fetched)

                if not uris_and_metadata:
                    return None

                candidates_with_embeddings = [
                    candidates_by_uri[at_uri]
                    for at_uri, _, _, _ in uris_and_metadata
                    if at_uri in candidates_by_uri
                ]
                input_post_embeddings = [
                    embedding for _, embedding, _, _ in uris_and_metadata
                ]
                author_dids = [
                    author_did for _, _, author_did, _ in uris_and_metadata
                ]
                like_counts = [
                    like_count for _, _, _, like_count in uris_and_metadata
                ]
                return candidates_with_embeddings, input_post_embeddings, author_dids, like_counts
        # end _get_candidate_features()


        candidates_by_uri = {candidate.at_uri: candidate for candidate in candidates if candidate.at_uri is not None}

        user_features, candidate_features = await asyncio.gather(
            _get_user_features(),
            _get_candidate_features(),
        )

        history_embeddings, history_author_dids, history_liked_at_times, history_like_counts = user_features

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
        candidate_posts, candidate_post_embeddings, candidate_author_dids, candidate_like_counts = candidate_features

        ranker_outputs = await predict_heavy_ranker_single_user(
            history_embeddings,
            history_author_dids,
            history_liked_at_times,
            history_like_counts,
            candidate_post_embeddings,
            candidate_author_dids,
            candidate_like_counts,
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

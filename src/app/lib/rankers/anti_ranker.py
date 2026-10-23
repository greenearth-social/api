"""Score candidates against a user's requestLess history.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ...models import CandidatePost, RankedCandidate, RankPredictResult
from ..embeddings import decode_float32_b64
from ..elasticsearch import fetch_post_embeddings_and_metadata
from ..feed_debug import current_recorder
from ..inference import (
    get_inference_settings,
    predict_heavy_ranker_single_user,
)
from ..telemetry import timed
from .base import Ranker, RankerResult
from .utils import get_rank_predict_results_from_candidates_and_scores

if TYPE_CHECKING:
    from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# Rankers are registered at import time; startup installs the shared Firestore client.
_db: AsyncClient | None = None


def set_anti_ranker_db(db: AsyncClient | None) -> None:
    global _db
    _db = db


class AntiRanker(Ranker):
    """Rank candidates by affinity to posts the user requested less of."""

    def __init__(self, name: str):
        self._name = name

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
        # Defer this import to avoid the documents/ranker registry import cycle.
        from ..firestore import get_user

        user_doc = await get_user(_db, user_did) if _db is not None else None
        request_less_items = user_doc.request_less_items if user_doc is not None else {}
        rec = current_recorder()
        if not request_less_items:
            if rec is not None:
                rec.record_user_features(self._name, [], 0)
            return RankerResult(model=self.name, result=RankPredictResult(rankings=[]))

        async def _get_user_features() -> tuple[list[list[float]], list[str], list[str], list[int]]:
            async with timed(logger, "ranker_get_user_features", user_did=user_did):
                # Inference keeps the first N history entries, so put recent requests first.
                history_uris = sorted(
                    request_less_items, key=request_less_items.__getitem__, reverse=True
                )
                embedded_history = await fetch_post_embeddings_and_metadata(es, history_uris)
                if rec is not None:
                    rec.record_user_features(self._name, history_uris, len(embedded_history))

                # The ES helper preserves URI order and skips posts without embeddings.
                # Build every feature from the same returned rows to keep them aligned.
                return (
                    [embedding for _, embedding, _, _ in embedded_history],
                    [author_did for _, _, author_did, _ in embedded_history],
                    [
                        request_less_items[uri].isoformat(timespec="seconds").replace("+00:00", "Z")
                        for uri, _, _, _ in embedded_history
                    ],
                    [like_count for _, _, _, like_count in embedded_history],
                )
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
        if not history_embeddings:
            # Empty history would score the model's general prior, not requestLess affinity.
            return RankerResult(model=self.name, result=RankPredictResult(rankings=[]))

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

        inference_base_url, inference_api_key = get_inference_settings()
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

        filtered_result = RankPredictResult(rankings=[ 
            r for r in result.rankings 
            if r.rank_score is not None and r.rank_score >= 0.2
        ])

        return RankerResult(model=self.name, result=filtered_result)

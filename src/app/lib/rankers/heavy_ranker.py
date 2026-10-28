"""Heavy ranker model.
"""

import asyncio
import logging
import math
from typing import assert_never

import httpx

from ...models import CandidatePost, RankedCandidate, RankPredictResult
from ..elasticsearch import fetch_post_embeddings_and_metadata
from ..embeddings import decode_float32_b64
from ..feed_debug import current_recorder
from ..inference import (
    HistoryMode,
    InferenceResponseFormatError,
    get_inference_settings,
    predict_heavy_ranker_single_user,
)
from ..metrics import get_metric_collector
from ..posthog_client import get_posthog_client, request_less_heavy_ranker_enabled
from ..request_context import get_traffic
from ..request_less_history import fetch_request_less_history_features
from ..telemetry import timed
from ..user_history_cache import fetch_user_history_features
from .base import RANK_MODEL_TIMEOUT_SEC, Ranker, RankerResult
from .utils import get_rank_predict_results_from_candidates_and_scores

logger = logging.getLogger(__name__)

# Affinity to requestLess history is a penalty, not a calibrated rejection
# probability. Zero disables the entire optional path, including flag lookup.
REQUEST_LESS_COEFFICIENT = 0.75


def _request_less_metric(name: str, value: float, **labels: str) -> None:
    collector = get_metric_collector()
    if collector is not None:
        try:
            collector.record(f"rank.request_less.{name}", value, **labels)
        except Exception:
            logger.warning("Failed to record requestLess ranking metric", exc_info=True)


class HeavyRanker(Ranker):
    """Score affinity to liked posts, optionally subtracting requestLess affinity."""

    def __init__(self, name: str, history_mode: HistoryMode):
        self._name = name
        self.history_mode: HistoryMode = history_mode

    @property
    def name(self) -> str:
        return self._name

    async def predict(
        self,
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        loop = asyncio.get_running_loop()
        # Reserve the end of the pipeline's ranker budget for positive scoring
        # and cleanup, even if flag evaluation or negative inference stalls.
        request_less_deadline = loop.time() + 0.8 * RANK_MODEL_TIMEOUT_SEC
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

        # Both inference calls share this exact candidate batch and ordering.
        # Start the positive prerequisites before any optional flag/history work.
        user_task = asyncio.create_task(_get_user_features())
        candidate_task = asyncio.create_task(_get_candidate_features())
        request_less_outcome = "disabled"

        async def _get_request_less_scores() -> list[float] | None:
            nonlocal request_less_outcome
            started = loop.time()
            try:
                async with asyncio.timeout_at(request_less_deadline):
                    # PostHog is synchronous. If we stop awaiting the thread,
                    # its eventual result cannot start history or inference work.
                    enabled = await asyncio.to_thread(
                        request_less_heavy_ranker_enabled, get_posthog_client(), user_did
                    )
                    if not enabled:
                        request_less_outcome = "flag_off"
                        return None

                    history = await fetch_request_less_history_features(user_did)
                    embedded_history = history.items_with_embeddings
                    if not embedded_history:
                        request_less_outcome = "missing_features"
                        return None
                    logger.info(
                        "Scoring requestLess affinity with %d hydrated history entries",
                        len(embedded_history),
                    )
                    # The optional deadline must not cancel candidate preparation:
                    # positive inference still needs it after this path times out.
                    candidate_features = await asyncio.shield(candidate_task)
                    if candidate_features is None:
                        request_less_outcome = "missing_features"
                        return None
                    candidate_posts, candidate_embeddings, candidate_authors, candidate_likes = candidate_features
                    outputs = await predict_heavy_ranker_single_user(
                        [item.embedding for item in embedded_history if item.embedding is not None],
                        [item.author_did for item in embedded_history],
                        [item.received_at.isoformat() for item in embedded_history],
                        [item.like_count for item in embedded_history],
                        candidate_embeddings,
                        candidate_authors,
                        candidate_likes,
                        base_url=inference_base_url,
                        api_key=inference_api_key,
                    )
                    # Treat a malformed batch as unavailable rather than mixing
                    # adjusted and unadjusted scores within the same ranking.
                    if (
                        not isinstance(outputs, list)
                        or len(outputs) != len(candidate_posts)
                        or any(
                            type(score) not in (int, float)
                            or not math.isfinite(score)
                            or not 0.0 <= score <= 1.0
                            for score in outputs
                        )
                    ):
                        raise InferenceResponseFormatError("Invalid requestLess inference scores")
                    return outputs
            except InferenceResponseFormatError:
                request_less_outcome = "invalid_output"
                logger.warning("Ignoring invalid requestLess inference output", exc_info=True)
                return None
            except (TimeoutError, httpx.TimeoutException):
                request_less_outcome = "timeout"
                logger.warning("RequestLess scoring timed out; using positive scores")
                return None
            except Exception:
                request_less_outcome = "error"
                logger.warning("RequestLess scoring failed; using positive scores", exc_info=True)
                return None
            finally:
                _request_less_metric("duration_ms", (loop.time() - started) * 1000)

        request_less_task = None
        if (
            REQUEST_LESS_COEFFICIENT != 0
            and self.history_mode == "actual"
            and get_traffic() != "logged_out"
        ):
            # Only mark an adjustment applied after positive scoring succeeds
            # and the adjusted scores are included in the returned result.
            request_less_outcome = "cancelled"
            request_less_task = asyncio.create_task(_get_request_less_scores())

        try:
            user_features, candidate_features = await asyncio.gather(user_task, candidate_task)
            history_embeddings, history_author_dids, history_liked_at_times, history_like_counts = user_features

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

            negative_outputs = await request_less_task if request_less_task is not None else None
            if negative_outputs is not None:
                ranker_outputs = [
                    positive - REQUEST_LESS_COEFFICIENT * negative
                    for positive, negative in zip(ranker_outputs, negative_outputs, strict=True)
                ]

            result = get_rank_predict_results_from_candidates_and_scores(
                candidate_posts,
                ranker_outputs,
                candidates_by_uri.values()
            )
            if negative_outputs is not None:
                request_less_outcome = "applied"
            return RankerResult(model=self.name, result=result)
        finally:
            # Drain only tasks owned by this invocation. Hydration scheduled by
            # the history reader belongs to the service and continues separately.
            tasks = [user_task, candidate_task]
            if request_less_task is not None:
                tasks.append(request_less_task)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            _request_less_metric("outcome_count", 1, outcome=request_less_outcome)

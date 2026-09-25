"""Two-tower candidate generator.

Blends the user tower's prediction with an average-user prior, then searches
for relevant posts via the pre-calculated post embeddings.
"""

import logging
import math

from ...models import MaxAgeHours
from ..average_user_embedding import get_average_user_embedding, get_average_user_embedding_error
from ..elasticsearch import POSTS_QUALITY_KNN_INDEX, two_tower_knn_index
from ..embeddings import GE_POST_EMBEDDING_FIELD
from ..feed_debug import current_recorder
from ..inference import (
    HistoryMode,
    InferenceResponseFormatError,
    get_inference_settings,
    predict_user_embedding,
)
from ..telemetry import timed
from ..user_history_cache import fetch_user_history_features
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts

logger = logging.getLogger(__name__)

# Treat the prior as two usable history items: it dominates sparse histories,
# has equal weight at two items, and fades as actual history grows.
AVG_USER_EMBEDDING_WEIGHT = 2.0


def _valid_query_embedding(vector: list[float] | tuple[float, ...]) -> bool:
    """Cosine retrieval requires finite coordinates and nonzero magnitude."""
    try:
        return (
            bool(vector)
            and all(type(value) in (int, float) and math.isfinite(value) for value in vector)
            and 0 < math.hypot(*vector) < math.inf
        )
    except OverflowError:
        return False


# Traction bar for the posts_recent fallback path only (see two_tower_knn_index
# in lib/elasticsearch.py). Corpus membership in posts_recent_quality already
# guarantees like_count was at or above ingex's own promotion threshold at
# promotion time, so applying this filter there would be redundant — worse,
# it would require this constant to be kept in sync with ingex's
# GE_QUALITY_LIKE_THRESHOLD, which is exactly the cross-repo coupling this
# design should not need. Against posts_recent it is the only thing enforcing
# any traction preference at all, so it stays load-bearing there. The two
# values are intentionally independent: this one describes what bar the api
# wants on an *unfiltered* corpus, not ingex's corpus-entry criterion.
MIN_LIKE_COUNT = 20


class TwoTowerCandidateGenerator(CandidateGenerator):
    """Candidate generator using the two tower model.

    With usable history, blend the user prediction with a compatible prior,
    falling back to the actual prediction if the prior cannot be used.
    Without usable history (or in empty-history mode), use the prior alone if
    available; otherwise return no candidates.
    All paths search post embeddings from the corresponding post-tower model.
    """

    def __init__(self, name: str, history_mode: HistoryMode):
        self._name = name
        self.history_mode: HistoryMode = history_mode

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        prior = get_average_user_embedding()
        fallback_reason = None
        num_likes = 0
        async with timed(logger, "two_tower_user_side", user_did=user_did):
            history = None
            if self.history_mode == "actual":
                # Reuse the cached, latest-64-likes history also used by the
                # embedding endpoint. Only items passed to the tower add weight;
                # likes whose post embeddings are missing do not count.
                history = await fetch_user_history_features(es, user_did)
                num_likes = len(history.items_with_embeddings)
            # Empty-history mode deliberately skips the loader even for an
            # established user. Snapshots still record that no history was used.
            rec = current_recorder()
            if rec is not None:
                rec.record_user_features(
                    self.name, history.liked_uris if history is not None else [], num_likes
                )

            if history is not None and num_likes:
                # Prior fallback handles an unavailable prior, not broken user
                # inference. Prediction errors propagate to pipeline diagnostics.
                inference_base_url, inference_api_key = get_inference_settings()
                prediction = await predict_user_embedding(
                    history, base_url=inference_base_url, api_key=inference_api_key
                )
                actual_embedding = prediction.embedding
                if actual_embedding is None or not _valid_query_embedding(actual_embedding):
                    raise InferenceResponseFormatError(
                        "Expected a finite nonzero user embedding for two-tower retrieval"
                    )
                if not prediction.user_model_uuid or not prediction.post_model_uuid:
                    raise InferenceResponseFormatError("Expected user-tower model pair metadata")
                user_embedding = actual_embedding
                # This is paired_post_model_uuid from the prediction response.
                # A separate /ready lookup could observe a different model pair
                # during an inference rollout, so this response is authoritative.
                post_tower_uuid = prediction.post_model_uuid
                retrieval_mode = "actual_only"
                # Equal dimensions alone do not imply a shared embedding space:
                # both tower UUIDs must match before their vectors can be blended.
                if prior is None:
                    fallback_reason = "average_user_embedding_unavailable"
                elif (
                    prediction.user_model_uuid != prior.user_model_uuid
                    or prediction.post_model_uuid != prior.post_model_uuid
                ):
                    fallback_reason = "average_user_embedding_model_pair_mismatch"
                elif len(actual_embedding) != prior.dimension:
                    fallback_reason = "average_user_embedding_dimension_mismatch"
                else:
                    # Part 1 saved a normalized mean and the tower returns its
                    # own embedding. Preserve both inputs and the weighted mean;
                    # cosine search does not require normalizing the blend again.
                    blended = [
                        (AVG_USER_EMBEDDING_WEIGHT * average + num_likes * actual)
                        / (AVG_USER_EMBEDDING_WEIGHT + num_likes)
                        for average, actual in zip(prior.embedding, actual_embedding, strict=True)
                    ]
                    if _valid_query_embedding(blended):
                        user_embedding = blended
                        retrieval_mode = "blended"
                    else:
                        # Even valid inputs can cancel to zero or overflow when
                        # blended. Keep the already-validated actual prediction.
                        fallback_reason = "average_user_embedding_invalid_blend"
                if fallback_reason:
                    prior_error = get_average_user_embedding_error() if prior is None else None
                    log_level = (
                        logging.INFO
                        if prior is None and prior_error in (None, "not_configured")
                        else logging.WARNING
                    )
                    logger.log(
                        log_level,
                        "%s using actual embedding only: reason=%s prior_error=%s "
                        "prior_run_id=%s post_model_uuid=%s history_embeddings=%d",
                        self.name,
                        fallback_reason,
                        prior_error,
                        prior.run_id if prior is not None else None,
                        post_tower_uuid,
                        num_likes,
                    )
            elif prior is not None:
                # No usable history means no user-tower or /ready call. Search
                # the artifact's own post-model space, even if serving has moved
                # to a newer pair; never silently substitute another model.
                user_embedding = list(prior.embedding)
                post_tower_uuid = prior.post_model_uuid
                retrieval_mode = "prior_only"
            else:
                # No vector to search with. Other configured sources can still
                # contribute through the existing candidate pipeline.
                logger.info(
                    "%s skipped: no usable history or average embedding; prior_error=%s",
                    self.name,
                    get_average_user_embedding_error(),
                )
                return CandidateResult(
                    generator_name=self.name,
                    candidates=[],
                    status="not_run",
                    reason=(
                        "no_user_like_history"
                        if self.history_mode == "actual"
                        else "average_user_embedding_unavailable"
                    ),
                )

        logger.debug(
            "%s retrieval mode=%s history_embeddings=%d prior_weight=%s prior_run_id=%s "
            "post_model_uuid=%s dimension=%d",
            self.name,
            retrieval_mode,
            num_likes,
            AVG_USER_EMBEDDING_WEIGHT / (AVG_USER_EMBEDDING_WEIGHT + num_likes)
            if retrieval_mode != "actual_only"
            else 0,
            prior.run_id if prior is not None else None,
            post_tower_uuid,
            len(user_embedding),
        )

        # Blended, actual-only, and prior-only retrieval share the same filters
        # and allocation. Selecting a prior does not expand the candidate budget.
        resolved_index = two_tower_knn_index()
        # Only apply the traction filter on the posts_recent fallback — see
        # MIN_LIKE_COUNT's docstring for why it would be redundant against the
        # quality corpus.
        min_like_count = None if resolved_index == POSTS_QUALITY_KNN_INDEX else MIN_LIKE_COUNT
        candidates = await knn_search_posts(
            es,
            user_embedding,
            num_candidates,
            search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            ge_post_embedding_model_uuid=post_tower_uuid,
            min_like_count=min_like_count,
            max_age_hours=max_age_hours,
            index=resolved_index,
        )

        logger.info(
            "%s retrieved %d/%d candidates mode=%s post_model_uuid=%s",
            self.name,
            len(candidates),
            num_candidates,
            retrieval_mode,
            post_tower_uuid,
        )
        # A successful actual-only fallback still carries its reason into feed
        # diagnostics. An empty search takes precedence as the retrieval outcome.
        reason = fallback_reason
        if not candidates:
            reason = "no_recent_authors_topics_posts"

        return CandidateResult(
            generator_name=self.name,
            candidates=candidates,
            reason=reason,
        )

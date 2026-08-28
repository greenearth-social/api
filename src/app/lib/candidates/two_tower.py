"""Two-tower candidate generator.

Runs the user tower to generate a user embedding and then searches
for the most relevant posts via the pre-calculated post embeddings.
"""

import logging

from ...models import MaxAgeHours
from ..elasticsearch import POSTS_QUALITY_KNN_INDEX, two_tower_knn_index
from ..embeddings import GE_POST_EMBEDDING_FIELD
from ..inference import (
    HistoryMode,
    compute_user_embedding,
    get_cached_post_tower_uuid,
    get_inference_settings,
)
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts

logger = logging.getLogger(__name__)


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

    Pipeline:
        user_did → recent likes → post embeddings → user tower → kNN search
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
        inference_base_url, inference_api_key = get_inference_settings()

        post_tower_uuid = await get_cached_post_tower_uuid(inference_base_url, inference_api_key)
        # None means /ready was valid but no post-tower model is configured.
        # Malformed /ready responses raise before this point.
        if post_tower_uuid is None:
            logger.warning(
                "Skipping two-tower candidates because post-tower is not configured",
            )
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_configured",
                reason="post_tower_not_configured",
            )

        # run the user tower to get the user embedding
        allow_empty_history = False
        if self.history_mode == "empty":
            allow_empty_history = True
        user_embedding = await compute_user_embedding(
            user_did,
            es,
            inference_base_url,
            inference_api_key,
            self.name,
            self.history_mode,
            allow_empty_history,
        )
        if user_embedding is None:
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_run",
                reason="no_user_like_history",
            )

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

        reason = None
        if not candidates:
            reason = "no_recent_authors_topics_posts"
            if exclude_uris and num_candidates > 0:
                try:
                    unexcluded = await knn_search_posts(
                        es,
                        user_embedding,
                        1,
                        search_field=GE_POST_EMBEDDING_FIELD,
                        generator_name=self.name,
                        video_only=video_only,
                        exclude_uris=None,
                        ge_post_embedding_model_uuid=post_tower_uuid,
                        min_like_count=min_like_count,
                        max_age_hours=max_age_hours,
                        index=resolved_index,
                    )
                    if unexcluded:
                        reason = "history_exclusions"
                except Exception:
                    # The primary query succeeded, so a diagnostic-only probe
                    # must not turn a truthful empty result into a feed error.
                    logger.warning("Failed to classify empty two-tower history exclusions")

        return CandidateResult(
            generator_name=self.name,
            candidates=candidates,
            reason=reason,
        )

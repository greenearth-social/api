"""Two-tower candidate generator.

Runs the user tower to generate a user embedding and then searches
for the most relevant posts via the pre-calculated post embeddings.
"""

import logging

from ...models import MaxAgeHours
from .base import CandidateGenerator, CandidateResult
from ..inference import (
    get_inference_settings,
    compute_user_embedding,
    get_cached_post_tower_uuid,
    HistoryMode,
)
from .es_candidates import knn_search_posts
from ..embeddings import GE_POST_EMBEDDING_FIELD

logger = logging.getLogger(__name__)


MIN_LIKE_COUNT = 20
# Hard cap on the freshness window for two-tower, independent of the user's
# freshness preference. The selective MIN_LIKE_COUNT filter makes Lucene
# abandon the HNSW graph and brute-force scan every matching vector, so cost
# scales with how many posts pass like_count>=20 inside the window:
# ~928k over the 7-day default (a ~30s cold scan on prod) versus ~320k at 96h
# (~95k/day; 100-350ms warm, measured on prod). The scanned set is identical
# for every user, so it stays hot in the page cache and a cold scan re-warms
# it for everyone in one query. Freshness preferences below this cap (6h-72h)
# pass through unchanged; only the 7-day preset is clamped here.
TWO_TOWER_MAX_AGE_CAP_HOURS = 96


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
        inference_base_url, inference_api_key = (
            get_inference_settings()
        )

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

        # Freshness filters returned candidates, not the interaction history
        # used above to compute the user embedding. Cap the requested window at
        # TWO_TOWER_MAX_AGE_CAP_HOURS to bound the brute-force vector scan.
        effective_max_age_hours = min(max_age_hours, TWO_TOWER_MAX_AGE_CAP_HOURS)
        candidates = await knn_search_posts(
            es, user_embedding, num_candidates, search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=self.name, video_only=video_only, exclude_uris=exclude_uris,
            ge_post_embedding_model_uuid=post_tower_uuid, min_like_count=MIN_LIKE_COUNT,
            max_age_hours=effective_max_age_hours,
        )

        return CandidateResult(generator_name=self.name, candidates=candidates)

"""Two-tower candidate generator.

Runs the user tower to generate a user embedding and then searches
for the most relevant posts via the pre-calculated post embeddings.
"""

import logging

from .base import CandidateGenerator, CandidateResult
from ..inference import get_inference_settings, compute_user_embedding, get_cached_post_tower_uuid
from .es_candidates import knn_search_posts
from ..embeddings import GE_POST_EMBEDDING_FIELD

logger = logging.getLogger(__name__)


TWO_TOWER_GENERATOR_NAME = "two_tower"
MIN_LIKE_COUNT = 20
# Only consider posts created within this window. Besides freshness, this
# bounds the brute-force vector scan that the selective MIN_LIKE_COUNT filter
# forces on ES: like_count>=20 alone matches ~1M posts in posts_recent (a
# multi-second cold scan per query), while the 96h slice is ~320k (~95k/day)
# and stays hot in the page cache because it is identical for every user.
# Warm-scan cost measured on prod: 100-350ms; a fully cold scan re-warms the
# shared set in one query, so longer windows mainly cost re-warm time after
# cache-eviction events.
MAX_POST_AGE = "96h"


class TwoTowerCandidateGenerator(CandidateGenerator):
    """Candidate generator using the two tower model.

    Pipeline:
        user_did → recent likes → post embeddings → user tower → kNN search
    """

    @property
    def name(self) -> str:
        return "two_tower"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
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
        user_embedding = await compute_user_embedding(
            user_did,
            es,
            inference_base_url,
            inference_api_key,
            TWO_TOWER_GENERATOR_NAME,
        )

        # kNN search for the most relevant posts given the user embedding
        candidates = await knn_search_posts(
            es, user_embedding, num_candidates, search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=self.name, video_only=video_only, exclude_uris=exclude_uris,
            ge_post_embedding_model_uuid=post_tower_uuid, min_like_count=MIN_LIKE_COUNT,
            max_age=MAX_POST_AGE,
        )

        return CandidateResult(generator_name=self.name, candidates=candidates)

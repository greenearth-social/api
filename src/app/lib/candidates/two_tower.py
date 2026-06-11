"""Two-tower candidate generator.

Runs the user tower to generate a user embedding and then searches
for the most relevant posts via the pre-calculated post embeddings.
"""

import logging

from .base import CandidateGenerator, CandidateResult
from ..feed_debug import current_recorder
from ..inference import get_inference_settings, compute_user_embedding
from .es_candidates import knn_search_posts
from ..telemetry import timed

logger = logging.getLogger(__name__)


TWO_TOWER_GENERATOR_NAME = "two_tower"
POST_EMBEDDING_FIELD = "ge_post_embedding"


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
        rec = current_recorder()

        inference_base_url, inference_api_key = (
            get_inference_settings()
        )

        async with timed(logger, "two_tower_candidate_user_side"):
            # run the user tower to get the user embedding
            user_embedding = await compute_user_embedding(
                user_did,
                es,
                inference_base_url,
                inference_api_key,
                TWO_TOWER_GENERATOR_NAME,
            )

        async with timed(logger, "two_tower_candidate_posts_search", n_candidates=num_candidates):
            # kNN search for the most relevant posts given the user embedding
            candidates = await knn_search_posts(
                es, user_embedding, num_candidates, search_field=POST_EMBEDDING_FIELD,
                generator_name=self.name, video_only=video_only, exclude_uris=exclude_uris
            )

        return CandidateResult(generator_name=self.name, candidates=candidates)

"""Two-tower candidate generator.
"""

import logging

from .base import CandidateGenerator, CandidateResult
from ..feed_debug import current_recorder
from ..inference import get_inference_settings, compute_user_embedding
from .es_candidates import knn_search_posts

logger = logging.getLogger(__name__)


TWO_TOWER_GENERATOR_NAME = "two_tower"


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
            es, user_embedding, num_candidates, search_field="NEW_FIELD_HERE",
            generator_name=self.name, video_only=video_only, exclude_uris=exclude_uris
        )

        return CandidateResult(generator_name=self.name, candidates=candidates)

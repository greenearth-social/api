"""LLM query vector candidate generator.

Reads a precomputed query vector from Firestore via ``LlmQueryVectorCache``
and runs a kNN search in Elasticsearch using the MiniLM-L12 embedding field.
"""

import logging

from ...models import MaxAgeHours
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts
from .llm_query_vector_cache import get_llm_query_vector_cache

logger = logging.getLogger(__name__)


class LlmQueryVectorCandidateGenerator(CandidateGenerator):
    """Candidate generator driven by a user's LLM-generated query vector.

    Reads the most recently updated query vector via the in-process cache and
    searches Elasticsearch using the MiniLM-L12 embedding field.  If no vector
    is found the generator returns an empty result so the pipeline can fall
    back to other sources.
    """

    @property
    def name(self) -> str:
        return "llm_query_vector"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        cache = get_llm_query_vector_cache()
        if cache is None:
            logger.warning("llm_query_vector generator called before cache was configured")
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_run",
                reason="cache_not_configured",
            )

        latest = await cache.get_latest(user_did)
        if latest is None:
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_run",
                reason="no_query_vector",
            )

        candidates = await knn_search_posts(
            es,
            latest.query_vector,
            num_candidates,
            search_field=MINILM_L12_EMBEDDING_FIELD,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            max_age_hours=max_age_hours,
        )

        return CandidateResult(generator_name=self.name, candidates=candidates)

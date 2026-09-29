"""LLM query vector candidate generator.

Reads the user's newest fitted query vector from Firestore and runs a kNN
search in Elasticsearch using the MiniLM-L12 embedding field.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...models import MaxAgeHours
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts

if TYPE_CHECKING:
    from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# Candidate generators are constructed at import time and ``generate`` only
# receives the Elasticsearch client, so the Firestore client is reached the
# same way the popularity and followed-users caches are: a process-level
# handle installed during app startup.  When it is unset (unit tests,
# scripts) the generator returns an empty result.
_db: AsyncClient | None = None


def set_llm_query_vector_db(db: AsyncClient | None) -> None:
    global _db
    _db = db


def get_llm_query_vector_db() -> AsyncClient | None:
    return _db


class LlmQueryVectorCandidateGenerator(CandidateGenerator):
    """Candidate generator driven by a user's LLM-generated query vector.

    Reads the most recently updated query vector from Firestore and searches
    Elasticsearch using the MiniLM-L12 embedding field.  If no vector is
    found the generator returns an empty result so the pipeline can fall
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
        db = get_llm_query_vector_db()
        if db is None:
            logger.warning("llm_query_vector generator called before Firestore was configured")
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_run",
                reason="firestore_not_configured",
            )

        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> candidates -> llm_query_vector
        # -> lib.firestore -> documents.  Same pattern as popularity_cache.
        from ..firestore import get_latest_llm_query_vector

        latest = await get_latest_llm_query_vector(db, user_did)
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

        reason = None
        if not candidates:
            reason = "no_posts_match_query_vector"

        return CandidateResult(generator_name=self.name, candidates=candidates, reason=reason)

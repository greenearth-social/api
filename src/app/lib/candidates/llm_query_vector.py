"""LLM query vector candidate generator.

Reads a precomputed query vector from Firestore (written by the prompt
ingestion service) and runs a kNN search in Elasticsearch using the
MiniLM-L12 embedding field.
"""

import logging

from ...models import MaxAgeHours
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from ..firestore import get_latest_llm_query_vector
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Db singleton — injected by main.py at startup, same pattern as
# popularity_cache.py's set_popularity_cache / get_popularity_cache.
# ---------------------------------------------------------------------------
# TODO: register this generator in candidates/__init__.py and wire
# set_llm_query_vector_db(app.state.firestore) in main.py's lifespan handler.

_db = None


def set_llm_query_vector_db(db) -> None:
    global _db
    _db = db


def get_llm_query_vector_db():
    return _db


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class LlmQueryVectorCandidateGenerator(CandidateGenerator):
    """Candidate generator driven by a user's LLM-generated query vector.

    Reads the most recently updated query vector from Firestore and searches
    Elasticsearch using the MiniLM-L12 embedding field.  If no vector is
    found the generator returns an empty result so the pipeline can fall back
    to other sources.
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
            logger.warning("llm_query_vector generator called before db was configured")
            return CandidateResult(
                generator_name=self.name,
                candidates=[],
                status="not_run",
                reason="db_not_configured",
            )

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

        return CandidateResult(generator_name=self.name, candidates=candidates)

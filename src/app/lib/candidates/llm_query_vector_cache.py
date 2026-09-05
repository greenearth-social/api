"""LLM query vector candidate generator and its Firestore-backed cache.

Reads a precomputed query vector from Firestore (written by the prompt
ingestion service) via an in-process TTL cache and runs a kNN search in
Elasticsearch using the MiniLM-L12 embedding field.

Query vectors are written by a separate ingestion service and change
infrequently, so a 60-second local TTL captures almost all reuse while
staying responsive to newly-generated vectors.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ...models import MaxAgeHours
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from ..firestore import get_latest_llm_query_vector
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts

if TYPE_CHECKING:
    from ...documents import LlmQueryVectorDocument

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


def ttl_seconds() -> int:
    try:
        return int(os.environ.get("GE_LLM_QUERY_VECTOR_CACHE_TTL_SEC", "60"))
    except ValueError:
        logger.warning("Invalid GE_LLM_QUERY_VECTOR_CACHE_TTL_SEC; using default 60")
        return 60


@dataclass
class _LocalEntry:
    document: LlmQueryVectorDocument
    fetched_at: float  # monotonic clock


class LlmQueryVectorCache:
    """In-process TTL cache for per-user LLM query vectors backed by Firestore.

    Cache hits skip the Firestore round-trip entirely.  Misses (including users
    with no query vector yet) always fall through to Firestore so a freshly
    generated vector is picked up on the next request.  Firestore errors are
    logged and treated as a miss rather than surfaced as exceptions.
    """

    def __init__(self, db: AsyncClient) -> None:
        self._db = db
        self._local: dict[str, _LocalEntry] = {}

    async def get_latest(self, user_did: str) -> LlmQueryVectorDocument | None:
        """Return the most recent query vector for *user_did*.

        Returns the locally-cached document when it is within the TTL, or
        fetches from Firestore otherwise.  Returns ``None`` when no vector
        exists for the user.
        """
        entry = self._local.get(user_did)
        if entry is not None and time.monotonic() - entry.fetched_at < ttl_seconds():
            return entry.document

        try:
            doc = await get_latest_llm_query_vector(self._db, user_did)
        except Exception:
            logger.exception("Failed to read LLM query vector for %s", user_did)
            return None

        if doc is not None:
            self._local[user_did] = _LocalEntry(document=doc, fetched_at=time.monotonic())
        return doc


# ---------------------------------------------------------------------------
# Process-level accessor
# ---------------------------------------------------------------------------

_llm_query_vector_cache: LlmQueryVectorCache | None = None


def set_llm_query_vector_cache(cache: LlmQueryVectorCache | None) -> None:
    global _llm_query_vector_cache
    _llm_query_vector_cache = cache


def get_llm_query_vector_cache() -> LlmQueryVectorCache | None:
    return _llm_query_vector_cache


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


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

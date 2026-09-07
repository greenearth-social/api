"""Per-user in-process cache for LLM query vectors.

Wraps ``get_latest_llm_query_vector`` from Firestore with a short in-process
TTL layer so repeated feed requests within the same window skip the Firestore
round-trip.  Query vectors are written by a separate ingestion service and
change infrequently, so a 60-second local TTL captures almost all reuse while
staying responsive to newly-generated vectors.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from ...documents import LlmQueryVectorDocument

logger = logging.getLogger(__name__)


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

        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> ... -> llm_query_vector_cache
        # -> lib.firestore -> documents.  Same pattern as popularity_cache.
        from ..firestore import get_latest_llm_query_vector

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
#
# Candidate generators are constructed at import time and their ``generate``
# signature is fixed by the CandidateGenerator interface, so the cache is
# reached the same way the metric collector and PostHog client are: a
# process-level handle installed during app startup.  When it is unset (unit
# tests, scripts) the generator returns an empty result.
# ---------------------------------------------------------------------------

_llm_query_vector_cache: LlmQueryVectorCache | None = None


def set_llm_query_vector_cache(cache: LlmQueryVectorCache | None) -> None:
    global _llm_query_vector_cache
    _llm_query_vector_cache = cache


def get_llm_query_vector_cache() -> LlmQueryVectorCache | None:
    return _llm_query_vector_cache

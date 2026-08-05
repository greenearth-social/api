"""Helpers for elasticsearch related to candidate generation
"""

import logging

from ...models import CandidatePost
from ..elasticsearch import POSTS_KNN_INDEX
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response
from ..telemetry import timed

logger = logging.getLogger(__name__)


async def knn_search_posts(
    es,
    query_vector: list[float],
    num_candidates: int,
    search_field: str,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    ge_post_embedding_model_uuid: str | None = None,
    min_like_count: int | None = None,
    max_age_hours: int = 168,
    index: str = POSTS_KNN_INDEX,
) -> list[CandidatePost]:
    """Run a kNN search against a posts index and return candidate posts.

    Filters are passed inside the kNN clause so ES applies them during HNSW
    traversal. Both ``posts_recent`` and ``posts_recent_quality`` contain only
    top-level posts (no replies), so no reply-exclusion filter is needed.

    ``index`` selects the corpus. Filter *selectivity* is what decides whether
    Lucene stays on the HNSW graph: a filter matching a small fraction of a
    segment makes it bypass the graph and exact-scan instead. ``two_tower``
    searches ``posts_recent_quality`` by default, where corpus membership
    already guarantees the traction bar, so it omits ``min_like_count``
    entirely there rather than reapplying a filter that would be redundant —
    matching every document, it costs nothing, but it also isn't doing
    anything. Its ``posts_recent`` fallback still passes it, since it's the
    only thing enforcing traction preference on the unfiltered corpus; that
    filter matches ~4.6% there, which is the exact-scan problem
    greenearth-social/ingex#442 built the quality corpus to avoid — an
    accepted cost of the fallback, not something the fallback fixes.

    ``max_age_hours`` bounds candidates to recently created posts. It is typed
    ``int`` rather than ``MaxAgeHours`` because callers may pass a value off the
    user-facing freshness ladder — ``two_tower`` caps its window below the
    7-day default (see ``TWO_TOWER_MAX_AGE_CAP_HOURS``).
    """
    # Freshness filters returned candidate posts only. Actual availability can
    # be shorter when posts_recent retains less data than the requested bound.
    filters: list[dict] = [
        {"range": {"created_at": {"gte": f"now-{max_age_hours}h"}}},
    ]
    if video_only:
        filters.append({"term": {"contains_video": True}})

    if ge_post_embedding_model_uuid:
        filters.append({"term": {"ge_post_embedding_model_uuid": ge_post_embedding_model_uuid}})

    if min_like_count is not None:
        filters.append({"range": {"like_count": {"gte": min_like_count}}})

    must_not: list[dict] = []
    if exclude_uris:
        must_not.append({"terms": {"at_uri": exclude_uris}})

    knn_clause: dict = {
        "field": search_field,
        "query_vector": query_vector,
        "k": num_candidates,
        "num_candidates": max(100, num_candidates * 10),
    }
    if filters or must_not:
        knn_clause["filter"] = {
            "bool": {
                **({"filter": filters} if filters else {}),
                **({"must_not": must_not} if must_not else {}),
            }
        }

    async with timed(
        logger,
        "knn_search_posts",
        index=index,
        num_candidates=num_candidates,
    ):
        resp = await es.search(
            index=index,
            op="knn",
            knn=knn_clause,
            size=num_candidates,
            _source=CANDIDATE_SOURCE_FIELDS,
            # Callers abandon this search after a few seconds (the candidate
            # generator timeout), but ES keeps executing abandoned queries to
            # completion — a long client timeout here lets a pathological query
            # burn cluster resources long after anyone is waiting for it.
            request_timeout=10,
        )

    return candidate_posts_from_es_response(resp, generator_name=generator_name)

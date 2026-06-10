"""Helpers for elastic search related to candidate generation
"""

import logging

from ...models import CandidatePost
from ..elasticsearch import POSTS_KNN_INDEX, unwrap_es_response
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_post_from_hit
from ..telemetry import timed

logger = logging.getLogger(__name__)


# How many extra hits to fetch from ES per requested candidate. The kNN
# neighborhood of an averaged-likes vector is empirically ~75% replies and
# ~5-10% videos, so we need significant margin to still hit num_candidates
# after Python-side filtering. Capped so we don't blow out latency on large
# requests.
OVERFETCH_MULTIPLIER = 5
MIN_OVERFETCH = 60
MAX_OVERFETCH = 500


async def knn_search_posts(
    es,
    query_vector: list[float],
    num_candidates: int,
    search_field: str,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Run a kNN search against the ``posts_recent`` index and return candidate posts.

    Reply exclusion (``thread_parent_post exists``) and the rare
    ``video_only`` filter are applied **in Python** rather than via the ES
    ``knn.filter`` parameter. Empirically, putting those filters in the
    kNN clause forces ES into brute-force scoring (>1 s per shard, several
    seconds total), even when ~55% of docs survive the filter. Profiling
    shows the per-shard ``vector_operations_count`` jumps from a few
    thousand to >100k when this happens.

    Cheap, bitmap-friendly filters (``exclude_uris`` as a ``terms``
    must_not) stay in ES because they don't trigger the fallback and
    they save bandwidth.
    """
    fetch_size = max(
        MIN_OVERFETCH,
        min(MAX_OVERFETCH, num_candidates * OVERFETCH_MULTIPLIER),
    )

    knn_clause: dict = {
        "field": search_field,
        "query_vector": query_vector,
        "k": fetch_size,
        "num_candidates": min(1500, fetch_size * 3),
    }
    if exclude_uris:
        knn_clause["filter"] = {
            "bool": {"must_not": [{"terms": {"at_uri": exclude_uris}}]}
        }

    async with timed(
        logger,
        "knn_search_posts",
        index=POSTS_KNN_INDEX,
        num_candidates=num_candidates,
        fetch_size=fetch_size,
    ):
        resp = await es.search(
            index=POSTS_KNN_INDEX,
            knn=knn_clause,
            size=fetch_size,
            _source=CANDIDATE_SOURCE_FIELDS,
            request_timeout=60,
        )

    data = unwrap_es_response(resp)
    candidates: list[CandidatePost] = []
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        # Skip replies — exclude documents where thread_parent_post is set.
        if src.get("thread_parent_post"):
            continue
        if video_only and not src.get("contains_video"):
            continue
        candidates.append(candidate_post_from_hit(hit, generator_name=generator_name))
        if len(candidates) >= num_candidates:
            break
    
    # drop candidates without embeddings
    candidates_with_embeddings = [
        candidate for candidate in candidates if candidate.minilm_l12_embedding
    ]
    if len(candidates_with_embeddings) < len(candidates):
        logger.info(
            "Dropped %d post-similarity candidates without embeddings",
            len(candidates) - len(candidates_with_embeddings),
        )
    return candidates_with_embeddings

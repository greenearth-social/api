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
) -> list[CandidatePost]:
    """Run a kNN search against the ``posts_recent`` index and return candidate posts.

    Filters are passed inside the kNN clause so ES applies them during HNSW
    traversal. The ``posts_recent`` index contains only top-level posts (no
    replies), so no reply-exclusion filter is needed.
    """
    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    if ge_post_embedding_model_uuid:
        filters.append({"term": {"ge_post_embedding_model_uuid": ge_post_embedding_model_uuid}})

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
        index=POSTS_KNN_INDEX,
        num_candidates=num_candidates,
    ):
        resp = await es.search(
            index=POSTS_KNN_INDEX,
            knn=knn_clause,
            size=num_candidates,
            _source=CANDIDATE_SOURCE_FIELDS,
            request_timeout=60,
        )

    return candidate_posts_from_es_response(resp, generator_name=generator_name)


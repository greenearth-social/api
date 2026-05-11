"""Shared Elasticsearch utilities.

Helpers for working with Elasticsearch responses that are used across
routers and candidate generators.
"""

import logging

from elastic_transport import ObjectApiResponse
from fastapi import HTTPException

from .embeddings import MINILM_L12_EMBEDDING_FIELD, MINILM_L12_EMBEDDING_KEY

logger = logging.getLogger(__name__)

# How many recent likes to consider when building the query vector.
DEFAULT_LIKED_POSTS_LIMIT = 50

# Index alias for KNN searches — targets only the last ~1 week of posts for speed.
POSTS_KNN_INDEX = "posts_recent"


def unwrap_es_response(resp) -> dict:
    """Unwrap an Elasticsearch response, handling both ObjectApiResponse and dict.

    Raises ``HTTPException`` with 502 if the response type is unexpected.
    """
    if isinstance(resp, ObjectApiResponse):
        return resp.body
    elif isinstance(resp, dict):
        return resp
    else:
        logger.error("Unexpected Elasticsearch response type: %s", type(resp))
        raise HTTPException(status_code=502, detail="Invalid Elasticsearch response")


async def fetch_recent_liked_post_uris(
    es,
    user_did: str,
    limit: int = DEFAULT_LIKED_POSTS_LIMIT,
) -> list[str]:
    """Return the AT URIs of posts the user most recently liked.

    Queries the ``likes`` index for documents where ``author_did`` matches
    *user_did*, sorted by ``created_at`` descending, and extracts the
    ``subject_uri`` field from each hit.
    """
    query = {
        "bool": {
            "filter": [{"term": {"author_did": user_did}}],
        }
    }

    resp = await es.search(
        index="likes",
        query=query,
        size=limit,
        sort=[{"created_at": "desc"}],
        _source=["subject_uri"],
    )

    data = unwrap_es_response(resp)
    uris: list[str] = []
    for hit in data.get("hits", {}).get("hits", []):
        uri = (hit.get("_source") or {}).get("subject_uri")
        if uri:
            uris.append(uri)
    return uris


async def fetch_post_embeddings(
    es,
    at_uris: list[str],
) -> list[tuple[str, list[float]]]:
    """Fetch MiniLM L12 embeddings for a list of post AT URIs.

    Returns ``(at_uri, embedding)`` pairs in the same order as ``at_uris``.
    Posts without embeddings are silently skipped.
    """
    if not at_uris:
        return []

    query = {"terms": {"at_uri": at_uris}}

    resp = await es.search(
        index="posts",
        query=query,
        size=len(at_uris),
        _source=["at_uri", MINILM_L12_EMBEDDING_FIELD],
    )

    data = unwrap_es_response(resp)
    embeddings_by_uri: dict[str, list[float]] = {}
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        at_uri = src.get("at_uri")
        if not at_uri:
            continue
        emb = src.get("embeddings")
        if isinstance(emb, dict):
            vec = emb.get(MINILM_L12_EMBEDDING_KEY)
            if vec:
                embeddings_by_uri[at_uri] = vec

    ordered_embeddings: list[tuple[str, list[float]]] = []
    for at_uri in at_uris:
        vec = embeddings_by_uri.get(at_uri)
        if vec:
            ordered_embeddings.append((at_uri, vec))
    return ordered_embeddings

"""Shared Elasticsearch utilities.

Helpers for working with Elasticsearch responses that are used across
routers and candidate generators.
"""

import logging

from elastic_transport import ObjectApiResponse
from fastapi import HTTPException

from .embeddings import MINILM_L12_EMBEDDING_FIELD, MINILM_L12_EMBEDDING_KEY
from .request_cache import get_request_cache
from .telemetry import timed

logger = logging.getLogger(__name__)

# How many recent likes to consider when building the query vector.
DEFAULT_LIKED_POSTS_LIMIT = 50

# Index alias for KNN searches — targets only the last ~1 week of posts for speed.
POSTS_KNN_INDEX = "posts_recent"


POST_EMBEDDING_SOURCE_FIELDS = [ "content" ]


def _has_nonblank_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def post_has_embedding_source(src: dict) -> bool:
    """Return whether a post has source text that can justify an embedding."""
    if _has_nonblank_string(src.get("content")):
        return True
    return False


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
    user_dids: str | list[str],
    limit: int = DEFAULT_LIKED_POSTS_LIMIT,
) -> list[str]:
    """Return the AT URIs of posts the user most recently liked.

    Queries the ``likes`` index for documents where ``author_did`` matches
    *user_did*, sorted by ``created_at`` descending, and extracts the
    ``subject_uri`` field from each hit.

    When a request cache is active the result is memoized so repeat
    calls within the same request (e.g. post_similarity and the two-tower
    ranker) share a single ES round-trip.
    """
    if isinstance(user_dids, str):
        user_dids = [user_dids]

    if not user_dids:
        return []

    async def _fetch() -> list[str]:
        async with timed(
            logger, "es_recent_likes", n_users=len(user_dids), limit=limit
        ):
            query = {
                "bool": {
                    "filter": [{"terms": {"author_did": user_dids}}],
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

    cache = get_request_cache()
    if cache is None:
        return await _fetch()
    key = ("fetch_recent_liked_post_uris", tuple(sorted(user_dids)), limit)
    return await cache.get_or_compute(key, _fetch)


async def fetch_post_embeddings(
    es,
    at_uris: list[str],
) -> list[tuple[str, list[float]]]:
    """Fetch MiniLM L12 embeddings for a list of post AT URIs.

    Returns ``(at_uri, embedding)`` pairs in the same order as ``at_uris``.
    Posts without embeddings or embedding source text are silently skipped.

    When a request cache is active the result is memoized so repeat
    calls within the same request share a single ES round-trip.
    """
    if not at_uris:
        return []

    async def _fetch() -> list[tuple[str, list[float]]]:
        async with timed(logger, "es_post_embeddings", n_uris=len(at_uris)):
            query = {"terms": {"at_uri": at_uris}}

            resp = await es.search(
                index="posts",
                query=query,
                size=len(at_uris),
                _source=[
                    "at_uri",
                    MINILM_L12_EMBEDDING_FIELD,
                    *POST_EMBEDDING_SOURCE_FIELDS,
                ],
            )

            data = unwrap_es_response(resp)
            embeddings_by_uri: dict[str, list[float]] = {}
            for hit in data.get("hits", {}).get("hits", []):
                src = hit.get("_source") or {}
                at_uri = src.get("at_uri")
                if not at_uri:
                    continue
                if not post_has_embedding_source(src):
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

    cache = get_request_cache()
    if cache is None:
        return await _fetch()
    key = ("fetch_post_embeddings", tuple(at_uris))
    return await cache.get_or_compute(key, _fetch)


async def fetch_post_embeddings_and_authors(
    es,
    at_uris: list[str],
) -> list[tuple[str, list[float], str]]:
    """Fetch MiniLM L12 embeddings for a list of post AT URIs, as well as their author DIDs.

    Returns ``(at_uri, embedding, author_did)`` triples in the same order as ``at_uris``.
    Posts without embeddings or embedding source text are silently skipped.
    Posts without author DIDs are kept, with an empty string ("") author_did.

    When a request cache is active the result is memoized so repeat
    calls within the same request share a single ES round-trip.
    """
    if not at_uris:
        return []

    async def _fetch() -> list[tuple[str, list[float], str]]:
        async with timed(logger, "es_post_embeddings", n_uris=len(at_uris)):
            query = {"terms": {"at_uri": at_uris}}

            resp = await es.search(
                index="posts",
                query=query,
                size=len(at_uris),
                _source=[
                    "at_uri",
                    MINILM_L12_EMBEDDING_FIELD,
                    "author_did",
                    *POST_EMBEDDING_SOURCE_FIELDS,
                ],
            )

            data = unwrap_es_response(resp)
            embeddings_by_uri: dict[str, list[float]] = {}
            author_dids_by_uri: dict[str, str] = {}
            for hit in data.get("hits", {}).get("hits", []):
                src = hit.get("_source") or {}
                at_uri = src.get("at_uri")
                if not at_uri:
                    continue
                if not post_has_embedding_source(src):
                    continue
                emb = src.get("embeddings")
                if isinstance(emb, dict):
                    vec = emb.get(MINILM_L12_EMBEDDING_KEY)
                    if vec:
                        embeddings_by_uri[at_uri] = vec
                author_did = src.get("author_did")
                if isinstance(author_did, str):
                    author_dids_by_uri[at_uri] = author_did

            ordered_embeddings: list[tuple[str, list[float], str]] = []
            for at_uri in at_uris:
                vec = embeddings_by_uri.get(at_uri)
                author_did = author_dids_by_uri.get(at_uri, "")
                if vec:
                    ordered_embeddings.append((at_uri, vec, author_did))
            return ordered_embeddings

    cache = get_request_cache()
    if cache is None:
        return await _fetch()
    key = ("fetch_post_embeddings_and_authors", tuple(at_uris))
    return await cache.get_or_compute(key, _fetch)

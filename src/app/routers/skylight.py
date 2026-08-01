import json
import logging

# The `elasticsearch` package exposes several specific exceptions; catch
# client errors as general exceptions here to avoid import-time issues.
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from ..security import verify_api_key
from pydantic import BaseModel, Field
from ..models import CandidatePost

from ..lib.embeddings import (
    MINILM_L12_EMBEDDING_FIELD,
    encode_float32_b64,
)
from ..lib.elasticsearch import embedding_from_fields, unwrap_es_response

# Fields pulled from `_source` for skylight results. Deliberately excludes
# `embeddings` (~65% of a post doc's `_source` size) — the MiniLM L12 vector
# is fetched separately via `docvalue_fields`, which reads it from doc values
# instead of decompressing the full stored `_source` blob.
SKYLIGHT_SOURCE_FIELDS = ["at_uri", "author_did", "content"]

router = APIRouter(tags=["skylight"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


class SkylightSearchResponse(BaseModel):
    """Search response returning a list of post results."""

    results: list[CandidatePost] = Field(
        default_factory=list,
        description="Matching posts in relevance order.",
    )


def posts_response_to_results(resp) -> list[CandidatePost]:
    """Convert an Elasticsearch response from the posts index to a list of CandidatePost objects.

    Handles ObjectApiResponse unwrapping, extracts hits, encodes embeddings as base64,
    and constructs CandidatePost objects.  The ES ``_score`` is forwarded when present.
    """
    data = unwrap_es_response(resp)
    results = []

    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source", {}) or {}
        l12 = embedding_from_fields(hit)

        encoded = None
        if l12 is not None:
            try:
                encoded = encode_float32_b64(l12)
            except Exception:
                encoded = None

        results.append(
            CandidatePost(
                author_did=src.get("author_did"),
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
                score=hit.get("_score"),
                generator_name=None,
            )
        )

    return results


@router.get(
    "/skylight/search",
    response_model=SkylightSearchResponse,
    responses={502: {"description": "Upstream Elasticsearch request failed"}},
)
async def skylight_search(
    request: Request,
    q: str = Query(..., description="Elasticsearch query string syntax — supports field qualifiers, wildcards, and boolean operators"),
    size: int = Query(10, ge=1, le=100, description="Maximum number of results to return (1–100)"),
) -> SkylightSearchResponse:
    """Search the `posts` index `content` field and return matching posts.

    Results are currently scoped to posts that contain video.
    Stored MiniLM L12 vectors are returned on each result when present.
    """
    # Only return posts that contain video. Use a boolean query with a
    # `must` for the original query_string and a `filter` for the
    # `contains_video` flag (non-scoring, cached by ES).
    body = {
        "query": {
            "bool": {
                "must": {
                    "query_string": {"query": q, "fields": ["content"]}
                },
                "filter": [{"term": {"contains_video": True}}]
            }
        }
    }

    # Use the application-scoped AsyncElasticsearch client created in the
    # FastAPI lifespan. In production this client is attached to
    # `app.state.es` in `main.py`. Tests should set `app.state.es` to a
    # fake/spy object that implements an async `search(...)` method.
    es = request.app.state.es
    try:
        resp = await es.search(
            index="posts",
            op="search",
            query=body.get("query"),
            size=size,
            _source=SKYLIGHT_SOURCE_FIELDS,
            docvalue_fields=[MINILM_L12_EMBEDDING_FIELD],
        )
    except Exception as exc:
        try:
            body_str = json.dumps(body, ensure_ascii=False)
        except Exception:
            body_str = repr(body)

        logger.exception(
            "Elasticsearch search failed",
            extra={"index": "posts", "request_body": body_str},
        )
        raise HTTPException(status_code=502, detail="Elasticsearch request failed") from exc

    results = posts_response_to_results(resp)
    return SkylightSearchResponse(results=results)

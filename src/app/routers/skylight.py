import base64
import json
import logging
from elastic_transport import ObjectApiResponse
import os
import struct

# The `elasticsearch` package exposes several specific exceptions; catch
# client errors as general exceptions here to avoid import-time issues.
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

router = APIRouter(tags=["skylight"])

logger = logging.getLogger(__name__)


class GreenEarthPost(BaseModel):
    """Represents a post returned by any search method."""
    at_uri: str | None = Field(
        None, description="The AT URI of the post (e.g. at://...)")
    content: str | None = Field(None, description="The post text content")
    minilm_l12_embedding: str | None = Field(
        None, description="Base64-encoded float32 array (384 floats)"
    )

class SkylightSearchResponse(BaseModel):
    """Search response returning a list of `GreenEarthPost` results."""
    results: list[GreenEarthPost]


def encode_float32_b64(vec: list[float]) -> str:
    """Encode a list of floats as little-endian float32 bytes, then base64.

    Uses struct.pack with little-endian `<f` format for portability.
    """
    if vec is None:
        raise TypeError("vec must not be None")
    if not isinstance(vec, (list, tuple)):
        raise TypeError("vec must be a list or tuple of floats")
    packed = struct.pack(f"<{len(vec)}f", *vec)
    return base64.b64encode(packed).decode("ascii")



@router.get("/skylight/search", response_model=SkylightSearchResponse)
async def skylight_search(
    request: Request,
    q: str = Query(..., description="Elasticsearch query string"),
    size: int = Query(10, ge=1, le=100),
) -> SkylightSearchResponse:
    """Search the `posts` index `content` field and return matching posts.

    Returns stored MiniLM vectors (`embeddings.all_MiniLM_L12_v2` and
    `embeddings.all_MiniLM_L6_v2`) when present.
    """
    body = {
        "query": {
            "query_string": {
                "query": q,
                "fields": ["content"]
            }
        }
    }

    # Use the application-scoped AsyncElasticsearch client created in the
    # FastAPI lifespan. In production this client is attached to
    # `app.state.es` in `main.py`. Tests should set `app.state.es` to a
    # fake/spy object that implements an async `search(...)` method.
    es = request.app.state.es
    try:
        resp = await es.search(index="posts", query=body.get("query"), size=size)
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

    # `AsyncElasticsearch.search` returns an elastic_transport.ObjectApiResponse
    # wrapper; prefer the underlying body (a dict) for downstream processing.
    if isinstance(resp, ObjectApiResponse):
        data = resp.body
    elif isinstance(resp, dict):
        data = resp
    else:
        logger.error("Unexpected Elasticsearch response type: %s", type(resp))
        raise HTTPException(status_code=502, detail="Invalid Elasticsearch response")
    results = []
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source", {}) or {}
        embeddings_obj = src.get("embeddings") or {}

        l12 = (
            embeddings_obj.get("all_MiniLM_L12_v2")
            if isinstance(embeddings_obj, dict)
            else None
        )

        encoded = None
        if l12 is not None:
            try:
                encoded = encode_float32_b64(l12)
            except Exception:
                encoded = None

        results.append(
            GreenEarthPost(
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
            )
        )

    return SkylightSearchResponse(results=results)

import base64
import json
import logging
from elastic_transport import ObjectApiResponse
import struct

# The `elasticsearch` package exposes several specific exceptions; catch
# client errors as general exceptions here to avoid import-time issues.
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from ..security import verify_api_key
from pydantic import BaseModel
from ..models import GreenEarthPost

router = APIRouter(tags=["skylight"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


class SkylightSearchResponse(BaseModel):
    """Search response returning a list of `GreenEarthPost` results."""
    results: list[GreenEarthPost]


class SkylightSimilarRequest(BaseModel):
    at_uris: list[str] | None = None
    embeddings: list[str] | None = None
    size: int = 10


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


def decode_float32_b64(b64: str) -> list[float]:
    """Decode a base64 float32 little-endian encoded vector to list[float]."""
    raw = base64.b64decode(b64)
    if len(raw) % 4 != 0:
        raise ValueError("invalid float32 byte length")
    count = len(raw) // 4
    return list(struct.unpack(f"<{count}f", raw))



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



@router.post("/skylight/similar", response_model=SkylightSearchResponse)
async def skylight_similar(request: Request, payload: SkylightSimilarRequest):
    """Return posts most similar to the average MiniLM L12 embedding for
    the supplied `at_uris` and/or base64-encoded `embeddings`.

    If `at_uris` are provided but none of them are found with embeddings,
    return 404.
    """
    vectors: list[list[float]] = []

    # 1) fetch embeddings for provided at_uris
    if payload.at_uris:
        lookup_query = {"terms": {"at_uri": payload.at_uris}}
        try:
            hits_resp = await request.app.state.es.search(
                index="posts", query=lookup_query, size=len(payload.at_uris)
            )
        except Exception as exc:
            logger.exception("Failed to lookup at_uris for similar search")
            raise HTTPException(status_code=502, detail="Elasticsearch request failed") from exc

        # unwrap ObjectApiResponse if necessary
        if isinstance(hits_resp, ObjectApiResponse):
            hits_data = hits_resp.body
        else:
            hits_data = hits_resp

        for hit in hits_data.get("hits", {}).get("hits", []):
            src = hit.get("_source", {}) or {}
            emb = src.get("embeddings", {}) if isinstance(src.get("embeddings"), dict) else None
            if emb:
                l12 = emb.get("all_MiniLM_L12_v2")
                if l12:
                    vectors.append(l12)

        if payload.at_uris and not vectors and not payload.embeddings:
            raise HTTPException(status_code=404, detail="No embeddings found for supplied at_uris")

    # 2) decode provided base64 embeddings
    if payload.embeddings:
        for b64 in payload.embeddings:
            try:
                vec = decode_float32_b64(b64)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid base64 embedding")
            vectors.append(vec)

    if not vectors:
        raise HTTPException(status_code=400, detail="No embeddings supplied or found for at_uris")

    # 3) compute average embedding
    dim = len(vectors[0])
    for v in vectors:
        if len(v) != dim:
            raise HTTPException(status_code=400, detail="Embedding dimension mismatch")

    avg = [0.0] * dim
    for v in vectors:
        for i, val in enumerate(v):
            avg[i] += val
    n = len(vectors)
    avg = [x / n for x in avg]

    # 4) perform similarity search using native `knn` query for nearest neighbors
    knn_q = {
        "bool": {
            "must": {
                "knn": {
                    "field": "embeddings.all_MiniLM_L12_v2",
                    "query_vector": avg,
                    "k": payload.size,
                    "num_candidates": max(100, payload.size * 10),
                }
            },
            "filter": [{"term": {"contains_video": True}}],
        }
    }

    try:
        resp = await request.app.state.es.search(index="posts", query=knn_q, size=payload.size)
    except Exception as exc:
        logger.exception("Elasticsearch similar search failed")
        raise HTTPException(status_code=502, detail="Elasticsearch request failed") from exc

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
        l12 = None
        emb_obj = src.get("embeddings") or {}
        if isinstance(emb_obj, dict):
            l12 = emb_obj.get("all_MiniLM_L12_v2")

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

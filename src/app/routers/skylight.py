import json
import logging

# The `elasticsearch` package exposes several specific exceptions; catch
# client errors as general exceptions here to avoid import-time issues.
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from ..security import verify_api_key
from pydantic import BaseModel
from ..models import CandidatePost
from ..lib.embeddings import encode_float32_b64, decode_float32_b64
from ..lib.elasticsearch import unwrap_es_response

router = APIRouter(tags=["skylight"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


class SkylightSearchResponse(BaseModel):
    """Search response returning a list of post results."""
    results: list[CandidatePost]


class SkylightSimilarRequest(BaseModel):
    at_uris: list[str] | None = None
    embeddings: list[str] | None = None
    size: int = 10


def posts_response_to_results(resp) -> list[CandidatePost]:
    """Convert an Elasticsearch response from the posts index to a list of CandidatePost objects.

    Handles ObjectApiResponse unwrapping, extracts hits, encodes embeddings as base64,
    and constructs CandidatePost objects.  The ES ``_score`` is forwarded when present.
    """
    data = unwrap_es_response(resp)
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
            CandidatePost(
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
                score=hit.get("_score"),
                generator_name=None,
            )
        )

    return results


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

    results = posts_response_to_results(resp)
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

        hits_data = unwrap_es_response(hits_resp)

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

    results = posts_response_to_results(resp)
    return SkylightSearchResponse(results=results)

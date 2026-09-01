"""POST /llm-query-vectors/fit - fit and store a MiniLM query vector for a
prompt (ingex#482).

The pipeline lives in lib/llm_query_vector_fit.py; this router validates the
request, runs it, writes the vector to Firestore (documents.LlmQueryVectorDocument,
`users/{user}/llm_query_vectors/{prompt_key}`) and returns the fit's statistics
so the caller can see that the job ran and how well.
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..lib.firestore import upsert_llm_query_vector
from ..lib.llm_query_vector_fit import (
    FitError,
    PoolTooSmallError,
    fit_query_vector,
    prompt_key,
)
from ..security import RequireAdminApiKey

logger = logging.getLogger(__name__)

router = APIRouter(tags=["llm-query-vectors"])

# A prompt is a sentence or two ("less complaining", "science news that will
# make me feel happy"). The cap keeps the expansion and the 80 scoring calls,
# which each embed the prompt, at their measured token counts.
MAX_PROMPT_CHARS = 2000


class QueryVectorFitRequest(BaseModel):
    user_did: str = Field(
        ...,
        pattern=r"^did:",
        description="AT Protocol DID of the user the vector belongs to (did:plc:...)",
    )
    prompt: str = Field(
        ...,
        min_length=1,
        max_length=MAX_PROMPT_CHARS,
        description="Free-text description of what the user wants to see",
    )


class QueryVectorFitResponse(BaseModel):
    prompt_key: str = Field(..., description="Firestore document id of the stored vector")
    user_did: str
    keywords: list[str] = Field(..., description="Search terms the prompt was expanded to")
    n_pool: int = Field(..., description="Deduplicated posts matching the keywords")
    n_keyword_posts: int = Field(..., description="Keyword posts in the fitting sample")
    n_random_posts: int = Field(
        ..., description="Random posts in the sample, scored 1 without the model"
    )
    n_scored: int = Field(..., description="Keyword posts the model scored before the deadline")
    n_cancelled: int = Field(..., description="Scoring calls cancelled at the deadline")
    n_failed: int = Field(..., description="Scoring calls that errored or returned no score")
    train_r2: float | None = Field(
        ..., description="Fit R^2 on its own sample; null if scores were constant"
    )
    duration_s: float
    cost_usd: float = Field(..., description="Model spend for this fit at list price")


@router.post(
    "/llm-query-vectors/fit",
    response_model=QueryVectorFitResponse,
    responses={
        422: {
            "description": "Invalid request, or too few posts match the prompt to fit a vector"
        },
        502: {"description": "Upstream Elasticsearch or model request failed; nothing stored"},
        503: {"description": "Firestore unavailable"},
    },
)
async def fit_llm_query_vector(
    body: QueryVectorFitRequest,
    request: Request,
    _key: RequireAdminApiKey,
) -> QueryVectorFitResponse:
    """Expand the prompt to keywords, sample and score posts, fit a query
    vector, store it under the user. Synchronous: ~10 s and ~$0.13 per call.
    Re-fitting the same prompt for the same user overwrites its vector."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    prompt = body.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt must not be blank")

    try:
        result = await fit_query_vector(request.app.state.es, prompt)
    except PoolTooSmallError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except FitError as exc:
        logger.warning("llm_qv_fit failed user_did=%s error=%s", body.user_did, exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("llm_qv_fit upstream failure", extra={"user_did": body.user_did})
        raise HTTPException(
            status_code=502, detail="Elasticsearch or model request failed"
        ) from exc

    key = prompt_key(prompt)
    await upsert_llm_query_vector(db, body.user_did, key, result.query_vector, prompt)
    logger.info(
        "llm_qv_fit stored user_did=%s prompt_key=%s n_pool=%d n_keyword=%d n_random=%d "
        "n_scored=%d n_cancelled=%d n_failed=%d train_r2=%s duration_s=%.1f cost_usd=%.3f "
        "tokens_in=%d tokens_out=%d",
        body.user_did, key, result.n_pool, result.n_keyword_posts, result.n_random_posts,
        result.n_scored, result.n_cancelled, result.n_failed,
        "-" if result.train_r2 is None else f"{result.train_r2:.3f}",
        result.duration_s, result.cost_usd, result.input_tokens, result.output_tokens,
    )
    return QueryVectorFitResponse(
        prompt_key=key,
        user_did=body.user_did,
        keywords=result.keywords,
        n_pool=result.n_pool,
        n_keyword_posts=result.n_keyword_posts,
        n_random_posts=result.n_random_posts,
        n_scored=result.n_scored,
        n_cancelled=result.n_cancelled,
        n_failed=result.n_failed,
        train_r2=result.train_r2,
        duration_s=round(result.duration_s, 2),
        cost_usd=round(result.cost_usd, 4),
    )

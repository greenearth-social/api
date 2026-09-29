"""Fit and read the signed-in user's MiniLM prompt vector (ingex#482, api#492).

POST /api/feeds/llm-query-vectors/fit     fit a prompt and store its vector
GET  /api/feeds/llm-query-vectors/current the newest fitted prompt, 204 if none

The pipeline lives in lib/llm_query_vector_fit.py; this router validates the
request, runs it, writes the vector to Firestore (documents.LlmQueryVectorDocument,
a new document under `users/{user}/llm_query_vectors/` per fit) and returns the
fit's statistics so the caller can see that the job ran and how well. The
llm_query_vector candidate generator (ingex#484) reads the user's most recently
updated document, so the newest fit is the one that serves.
"""

import asyncio
import logging
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from ..lib.firebase_auth import FirebaseUser
from ..lib.firestore import add_llm_query_vector, get_latest_llm_query_vector
from ..lib.llm_query_vector_fit import FitError, PoolTooSmallError, fit_query_vector
from ..lib.posthog_client import get_posthog_client, llm_cg_enabled

logger = logging.getLogger(__name__)

router = APIRouter(tags=["llm-query-vectors"])

# A prompt is a sentence or two ("less complaining", "science news that will
# make me feel happy"). The cap keeps the expansion and the 80 scoring calls,
# which each embed the prompt, at their measured token counts.
MAX_PROMPT_CHARS = 2000
# Hard stop for one fit. Normal fits take ~10 s; the scoring step's own
# deadline is soft (below MIN_SCORED_FRACTION it keeps waiting for in-flight
# calls, each of which can retry for minutes), so without this a slow model
# day could run past Cloud Run's 60 s and finish, and store, after the caller
# already got a 504.
FIT_TIMEOUT_S = 25.0


async def require_llm_cg_user(user_doc_id: FirebaseUser) -> str:
    """The signed-in user's DID, or 403 while the ``llm-cg`` flag is off for them.

    The feature is in beta: only the PostHog internal cohort may fit prompts,
    which is what costs money. The frontend hides the UI for everyone else.
    """
    user_did = f"did:plc:{user_doc_id}"
    enabled = await asyncio.to_thread(llm_cg_enabled, get_posthog_client(), user_did)
    if not enabled:
        raise HTTPException(
            status_code=403, detail="The prompt source is not enabled for this account"
        )
    return user_did


LlmCgUser = Annotated[str, Depends(require_llm_cg_user)]


class QueryVectorFitRequest(BaseModel):
    prompt: str = Field(
        ...,
        min_length=1,
        max_length=MAX_PROMPT_CHARS,
        description="Free-text description of what the user wants to see",
    )


class QueryVectorFitResponse(BaseModel):
    vector_id: str = Field(..., description="Firestore document id of the stored vector")
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
    cost_usd: float = Field(
        ...,
        description=(
            "Model spend for this fit at list price. Reported tokens, plus an "
            "estimate for calls cancelled at the deadline (billed, but they report "
            "no usage): each charged the mean input and the longest reply of the "
            "calls that completed. Leans high; measured 3-8% above the reported "
            "tokens' cost when 2-6 of 80 calls are cancelled."
        ),
    )


@router.post(
    "/api/feeds/llm-query-vectors/fit",
    response_model=QueryVectorFitResponse,
    responses={
        403: {"description": "The llm-cg feature flag is off for this user"},
        422: {
            "description": "Invalid request, or too few posts match the prompt to fit a vector"
        },
        502: {"description": "Upstream Elasticsearch or model request failed; nothing stored"},
        503: {"description": "Firestore unavailable"},
        504: {"description": "The fit did not finish within FIT_TIMEOUT_S; nothing stored"},
    },
)
async def fit_llm_query_vector(
    body: QueryVectorFitRequest,
    request: Request,
    user_did: LlmCgUser,
) -> QueryVectorFitResponse:
    """Expand the prompt to keywords, sample and score posts, fit a query
    vector, store it under the user. Synchronous: ~10 s and ~$0.13 per call.
    Every fit stores a new document; the feed uses the user's newest one.
    The user is whoever the Firebase token belongs to, and must have the
    ``llm-cg`` flag."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    prompt = body.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt must not be blank")

    try:
        async with asyncio.timeout(FIT_TIMEOUT_S):
            result = await fit_query_vector(request.app.state.es, prompt)
    except TimeoutError as exc:
        logger.warning("llm_qv_fit timed out user_did=%s after %.0fs", user_did, FIT_TIMEOUT_S)
        raise HTTPException(
            status_code=504,
            detail=f"fit did not finish within {FIT_TIMEOUT_S:.0f}s; nothing stored, try again",
        ) from exc
    except PoolTooSmallError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except FitError as exc:
        logger.warning("llm_qv_fit failed user_did=%s error=%s", user_did, exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("llm_qv_fit upstream failure", extra={"user_did": user_did})
        raise HTTPException(
            status_code=502, detail="Elasticsearch or model request failed"
        ) from exc

    stored = await add_llm_query_vector(db, user_did, result.query_vector, prompt)
    key = stored.prompt_key
    logger.info(
        "llm_qv_fit stored user_did=%s vector_id=%s n_pool=%d n_keyword=%d n_random=%d "
        "n_scored=%d n_cancelled=%d n_failed=%d train_r2=%s duration_s=%.1f cost_usd=%.3f "
        "tokens_in=%d tokens_out=%d",
        user_did, key, result.n_pool, result.n_keyword_posts, result.n_random_posts,
        result.n_scored, result.n_cancelled, result.n_failed,
        "-" if result.train_r2 is None else f"{result.train_r2:.3f}",
        result.duration_s, result.cost_usd, result.input_tokens, result.output_tokens,
    )
    return QueryVectorFitResponse(
        vector_id=key,
        user_did=user_did,
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


class CurrentPromptResponse(BaseModel):
    prompt_key: str = Field(..., description="Firestore document id of the serving vector")
    prompt: str = Field(..., description="Prompt text the vector was fitted to")
    created_at: datetime = Field(..., description="When it was fitted")


@router.get(
    "/api/feeds/llm-query-vectors/current",
    response_model=CurrentPromptResponse,
    responses={
        204: {"description": "No prompt fitted yet"},
        403: {"description": "The llm-cg feature flag is off for this user"},
        503: {"description": "Firestore unavailable"},
    },
)
async def current_llm_prompt(
    request: Request,
    user_did: LlmCgUser,
) -> CurrentPromptResponse | Response:
    """The prompt behind the vector the feed currently serves for this user,
    which is the most recently updated one. The vector itself is not returned."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    stored = await get_latest_llm_query_vector(db, user_did)
    if stored is None:
        return Response(status_code=204)
    return CurrentPromptResponse(
        prompt_key=stored.prompt_key, prompt=stored.prompt, created_at=stored.created_at
    )

"""Fit and read the signed-in user's MiniLM prompt vector (ingex#482, api#492).

POST /api/feeds/llm-query-vectors/fit     fit a prompt and store its vector
GET  /api/feeds/llm-query-vectors/current the active fitted prompt, 204 if none
DELETE /api/feeds/llm-query-vectors/current remove activation and redistribute its weight

The pipeline lives in lib/llm_query_vector_fit.py; this router validates the
request, runs it, writes the vector to Firestore (documents.LlmQueryVectorDocument,
a new document under `users/{user}/llm_query_vectors/` per fit) and returns the
fit's statistics so the caller can see that the job ran and how well. The
llm_query_vector candidate generator reads the explicit active key, falling back
to the newest document for users who have not yet used prompt activation state.
"""

import asyncio
import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..lib.firebase_auth import FirebaseUser
from ..lib.firestore_rate_limiter import FirestoreRateLimiter, QuotaScope
from ..lib.llm_prompt_state import (
    PromptChangedError,
    get_current_llm_query_vector,
    get_llm_prompt_revision,
    remove_llm_prompt,
    save_fitted_llm_prompt,
)
from ..lib.llm_query_vector_fit import (
    FitError,
    PoolTooSmallError,
    PromptNotAcceptedError,
    fit_query_vector,
)
from ..lib.metrics import get_metric_collector
from ..lib.posthog_client import get_posthog_client, llm_cg_enabled
from ..lib.request_context import get_request_id
from ..models_feed_transparency import FeedPreferences

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
FIT_RATE_LIMITER = FirestoreRateLimiter(
    name="llm_query_vector_fit",
    user_limit=12,
    user_window=timedelta(hours=6),
    global_daily_limit=1000,
)


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


class QueryVectorRateLimitDetail(BaseModel):
    code: Literal["llm_query_vector_rate_limited"] = "llm_query_vector_rate_limited"
    scopes: list[QuotaScope]
    message: str
    retry_at: datetime = Field(
        ...,
        description="Earliest retry time in UTC based on current usage",
    )
    retry_after_seconds: int = Field(..., ge=0, description="Seconds until retry_at, rounded up")


class QueryVectorRateLimitResponse(BaseModel):
    detail: QueryVectorRateLimitDetail


class QueryVectorPromptErrorDetail(BaseModel):
    code: Literal["llm_query_vector_pool_too_small", "llm_query_vector_prompt_not_accepted"]
    message: str


class QueryVectorPromptErrorResponse(BaseModel):
    detail: QueryVectorPromptErrorDetail | list[dict[str, object]] | str


@router.post(
    "/api/feeds/llm-query-vectors/fit",
    response_model=QueryVectorFitResponse,
    responses={
        403: {"description": "The llm-cg feature flag is off for this user"},
        409: {"description": "A newer prompt change superseded this fit; nothing stored"},
        422: {
            "model": QueryVectorPromptErrorResponse,
            "description": "Invalid request, too few matching posts, or an explicit prompt refusal",
        },
        429: {
            "model": QueryVectorRateLimitResponse,
            "description": "The rolling user quota or UTC daily global quota is full",
            "headers": {
                "Retry-After": {
                    "description": "Seconds until the earliest retry, rounded up",
                    "schema": {"type": "integer"},
                }
            },
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
    Each successful fit stores and activates a new vector document.
    The user is whoever the Firebase token belongs to, and must have the
    ``llm-cg`` flag."""
    prompt = body.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt must not be blank")

    # Only eligible, valid requests count as attempts, including quota rejections.
    collector = get_metric_collector()
    if collector is not None:
        collector.record("llm_query_vector.fit.attempted_count", 1)

    # Default to failure until the response is ready. Record exactly one outcome
    # outside the retryable quota transaction, including storage errors/cancellation.
    outcome_metric = "llm_query_vector.fit.failed_exc_count"
    outcome_labels = {"reason": "quota_store"}
    try:
        db = getattr(request.app.state, "firestore", None)
        if db is None:
            raise HTTPException(status_code=503, detail="Firestore unavailable")

        # Capture activation before quota/model work so a removal supersedes this fit.
        outcome_labels = {"reason": "prompt_state"}
        revision = await get_llm_prompt_revision(db, user_did)
        outcome_labels = {"reason": "quota_store"}
        try:
            quota = await FIT_RATE_LIMITER.reserve(db, user_did)
        except Exception as exc:
            logger.exception("llm_qv_fit quota unavailable", extra={"user_did": user_did})
            raise HTTPException(
                status_code=503,
                detail="Unable to verify or reserve query-vector quota. Please try again.",
            ) from exc

        if quota.retry_at is not None:
            retry_after_seconds = max(
                0, math.ceil((quota.retry_at - datetime.now(UTC)).total_seconds())
            )
            if quota.exhausted_scopes == ("user",):
                message = (
                    "You have reached your query-vector generation limit for the past six hours."
                )
            elif quota.exhausted_scopes == ("global",):
                message = "The daily query-vector generation limit has been reached."
            else:
                message = (
                    "Your six-hour limit and the daily query-vector generation limit are full."
                )
            detail = QueryVectorRateLimitDetail(
                scopes=list(quota.exhausted_scopes),
                message=f"{message} Please try again later.",
                retry_at=quota.retry_at,
                retry_after_seconds=retry_after_seconds,
            )
            outcome_metric = "llm_query_vector.fit.failed_limited_count"
            outcome_labels = {
                "scope": "both" if len(quota.exhausted_scopes) == 2 else quota.exhausted_scopes[0]
            }
            raise HTTPException(
                status_code=429,
                detail=detail.model_dump(mode="json"),
                headers={"Retry-After": str(retry_after_seconds)},
            )

        # At this point, this fit has already counted against the quotas. That is,
        # the reservation is already committed. Retries of its transaction cannot
        # duplicate metrics, and later fit failures intentionally do not refund quota.
        if collector is not None:
            for scope in quota.reached_scopes:
                collector.record("llm_query_vector.fit.cap_reached_count", 1, scope=scope)

        outcome_labels = {"reason": "upstream"}
        try:
            async with asyncio.timeout(FIT_TIMEOUT_S):
                result = await fit_query_vector(request.app.state.es, prompt)
        except TimeoutError as exc:
            outcome_labels = {"reason": "timeout"}
            logger.warning("llm_qv_fit timed out user_did=%s after %.0fs", user_did, FIT_TIMEOUT_S)
            raise HTTPException(
                status_code=504,
                detail=f"fit did not finish within {FIT_TIMEOUT_S:.0f}s; nothing stored, try again",
            ) from exc
        except PoolTooSmallError as exc:
            outcome_labels = {"reason": "pool_too_small"}
            detail = QueryVectorPromptErrorDetail(
                code="llm_query_vector_pool_too_small",
                message=(
                    "We couldn’t find enough relevant posts for this prompt. "
                    "Try a broader topic or different wording."
                ),
            )
            raise HTTPException(status_code=422, detail=detail.model_dump()) from exc
        except PromptNotAcceptedError as exc:
            outcome_labels = {"reason": "prompt_not_accepted"}
            detail = QueryVectorPromptErrorDetail(
                code="llm_query_vector_prompt_not_accepted",
                message="Prompt not accepted. Please rephrase your request and try again.",
            )
            raise HTTPException(status_code=422, detail=detail.model_dump()) from exc
        except FitError as exc:
            logger.warning("llm_qv_fit failed user_did=%s error=%s", user_did, exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("llm_qv_fit upstream failure", extra={"user_did": user_did})
            raise HTTPException(
                status_code=502, detail="Elasticsearch or model request failed"
            ) from exc

        outcome_labels = {"reason": "storage"}
        stored = await save_fitted_llm_prompt(
            db, user_did, result.query_vector, prompt, expected_revision=revision
        )
        key = stored.prompt_key
        logger.info(
            "llm_qv_fit stored user_did=%s vector_id=%s n_pool=%d n_keyword=%d n_random=%d "
            "n_scored=%d n_cancelled=%d n_failed=%d train_r2=%s duration_s=%.1f cost_usd=%.3f "
            "tokens_in=%d tokens_out=%d",
            user_did,
            key,
            result.n_pool,
            result.n_keyword_posts,
            result.n_random_posts,
            result.n_scored,
            result.n_cancelled,
            result.n_failed,
            "-" if result.train_r2 is None else f"{result.train_r2:.3f}",
            result.duration_s,
            result.cost_usd,
            result.input_tokens,
            result.output_tokens,
        )
        response = QueryVectorFitResponse(
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
        outcome_metric = "llm_query_vector.fit.success_count"
        outcome_labels = {}
        return response
    except asyncio.CancelledError:
        outcome_labels = {"reason": "cancelled"}
        raise
    except PromptChangedError as exc:
        outcome_labels = {"reason": "prompt_changed"}
        logger.warning(
            "llm_qv_fit superseded error_type=PromptChangedError request_id=%s", get_request_id()
        )
        raise _prompt_changed() from exc
    except Exception as exc:
        cause = exc.__cause__ or exc
        status = exc.status_code if isinstance(exc, HTTPException) else 500
        logger.warning(
            "llm_qv_fit failure reason=%s error_type=%s status=%s request_id=%s "
            "provider_request_id=%s user_did=%s",
            "rate_limited" if status == 429 else outcome_labels.get("reason", "unknown"),
            type(cause).__name__,
            status,
            get_request_id(),
            getattr(cause, "request_id", None),
            user_did,
        )
        raise
    finally:
        if collector is not None:
            collector.record(outcome_metric, 1, **outcome_labels)


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
    """The active prompt behind the vector the feed currently serves for this user."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    stored = await get_current_llm_query_vector(db, user_did)
    if stored is None:
        return Response(status_code=204)
    return CurrentPromptResponse(
        prompt_key=stored.prompt_key, prompt=stored.prompt, created_at=stored.created_at
    )


def _prompt_changed() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": "llm_query_vector_prompt_changed",
            "message": "Your prompt changed in another session. Please try again.",
        },
    )


@router.delete(
    "/api/feeds/llm-query-vectors/current",
    response_model=FeedPreferences,
    responses={
        403: {"description": "The llm-cg feature flag is off for this user"},
        409: {"description": "The active prompt differs from the requested prompt key"},
        503: {"description": "Prompt removal could not be persisted"},
    },
)
async def remove_current_llm_prompt(
    request: Request,
    user_did: LlmCgUser,
    prompt_key: Annotated[str, Query(min_length=1, max_length=1500)],
) -> FeedPreferences:
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    try:
        preferences = await remove_llm_prompt(db, user_did, prompt_key)
    except PromptChangedError as exc:
        logger.warning(
            "llm_qv_remove superseded error_type=PromptChangedError request_id=%s", get_request_id()
        )
        raise _prompt_changed() from exc
    except Exception as exc:
        logger.exception(
            "llm_qv_remove failed error_type=%s request_id=%s",
            type(exc).__name__,
            get_request_id(),
        )
        raise HTTPException(status_code=503, detail="Unable to remove prompt") from exc
    logger.info("llm_qv_remove completed request_id=%s user_did=%s", get_request_id(), user_did)
    return FeedPreferences.model_validate(preferences.model_dump(exclude_none=True))

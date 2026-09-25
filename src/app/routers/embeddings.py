"""Authenticated export of actual-history user embeddings for offline jobs."""

import asyncio
import logging
import time

import httpx
from elastic_transport import ConnectionTimeout
from elasticsearch import ApiError
from fastapi import APIRouter, Depends, HTTPException, Request

from ..lib.inference import (
    InferenceModelMetadataError,
    InferenceResponseFormatError,
    get_inference_settings,
    predict_user_embedding,
)
from ..lib.request_context import get_request_id
from ..lib.user_history_cache import fetch_user_history_features
from ..models import UserEmbeddingRequest, UserEmbeddingResponse
from ..security import verify_api_key

logger = logging.getLogger(__name__)
# Router-level authentication runs before any history or inference work.
router = APIRouter(tags=["embeddings"], dependencies=[Depends(verify_api_key)])

# Leave time to return a structured 504 before the producer's 60-second HTTP timeout.
REQUEST_TIMEOUT_SECONDS = 55.0


def _failure(
    code: str,
    *,
    started: float,
    stage: str,
    exception: Exception,
    status_code: int = 502,
) -> HTTPException:
    # The stable code is consumed by the offline script. Request ID and stage help
    # correlate server logs without returning raw exceptions in the HTTP response.
    logger.warning(
        "User embedding export failed code=%s stage=%s exception_class=%s "
        "upstream_status=%s duration_ms=%.1f request_id=%s",
        code,
        stage,
        type(exception).__name__,
        getattr(exception, "status_code", None),
        (time.monotonic() - started) * 1000,
        get_request_id(),
    )
    return HTTPException(status_code=status_code, detail={"code": code})


@router.post(
    "/embeddings/user",
    response_model=UserEmbeddingResponse,
    response_model_exclude_none=True,
    responses={
        502: {"description": "Upstream failure or invalid model metadata"},
        504: {"description": "History, inference, or overall request deadline expired"},
    },
)
async def user_embedding(request: Request, payload: UserEmbeddingRequest) -> UserEmbeddingResponse:
    """Return one actual-history prediction without retrieval or empty-history fallback."""
    started = time.monotonic()
    try:
        inference_base_url, inference_api_key = get_inference_settings()
    except RuntimeError as exc:
        raise _failure(
            "inference_not_configured", started=started, stage="configuration", exception=exc
        ) from None

    stage = "history"
    try:
        # One deadline covers cached/loaded history and inference together,
        # rather than granting each operation a fresh 55-second budget.
        async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
            history_started = time.monotonic()
            # Reuse the production loader and cache so offline vectors reflect the
            # same latest-like preparation as feed requests, including reply history.
            history = await fetch_user_history_features(request.app.state.es, payload.user_did)
            history_ms = (time.monotonic() - history_started) * 1000
            prediction_started = time.monotonic()
            stage = "inference"
            # The helper returns skip reasons for unusable history without inference.
            # Otherwise its prediction supplies the actual user/post model pair;
            # no readiness lookup or candidate search participates in this export.
            result = await predict_user_embedding(
                history,
                base_url=inference_base_url,
                api_key=inference_api_key,
            )
            prediction_ms = (time.monotonic() - prediction_started) * 1000
    except (TimeoutError, httpx.TimeoutException, ConnectionTimeout) as exc:
        raise _failure(
            "upstream_timeout", status_code=504, started=started, stage=stage, exception=exc
        ) from None
    except InferenceModelMetadataError as exc:
        raise _failure(
            "model_metadata_missing", started=started, stage=stage, exception=exc
        ) from None
    except InferenceResponseFormatError as exc:
        raise _failure(
            "invalid_inference_response", started=started, stage=stage, exception=exc
        ) from None
    except ApiError as exc:
        # ES authentication/configuration errors are actionable without retrying
        # every user; other upstream failures keep the generic retryable code.
        status = exc.status_code
        if status in (401, 403):
            code = "upstream_authentication_error"
        elif status in (400, 404, 405, 422) or 300 <= status < 400:
            code = "upstream_configuration_error"
        else:
            code = "upstream_error"
        raise _failure(code, started=started, stage=stage, exception=exc) from None
    except Exception as exc:
        # Raw transport exceptions may include URLs, credentials, or response
        # bodies. Log only a stable code and the existing correlation ID.
        raise _failure("upstream_error", started=started, stage=stage, exception=exc) from None

    logger.info(
        "User embedding export status=%s reason=%s likes=%d embedded=%d "
        "user_model_uuid=%s post_model_uuid=%s dimension=%s "
        "history_ms=%.1f inference_ms=%.1f duration_ms=%.1f request_id=%s",
        "skipped" if result.reason else "ok",
        result.reason,
        result.history_like_count,
        result.history_embedding_count,
        result.user_model_uuid,
        result.post_model_uuid,
        len(result.embedding) if result.embedding is not None else None,
        history_ms,
        prediction_ms,
        (time.monotonic() - started) * 1000,
        get_request_id(),
    )
    return UserEmbeddingResponse(
        # None fields are omitted by the route's response configuration, so skipped
        # users have no vector or model identity that a producer could mistake for data.
        user_did=payload.user_did,
        status="skipped" if result.reason else "ok",
        history_like_count=result.history_like_count,
        history_embedding_count=result.history_embedding_count,
        embedding=result.embedding,
        user_model_uuid=result.user_model_uuid,
        post_model_uuid=result.post_model_uuid,
        dimension=len(result.embedding) if result.embedding is not None else None,
        reason=result.reason,
    )

"""Compute two-tower user embeddings independently of candidate search."""

import logging
from typing import Literal

import httpx
from elastic_transport import ConnectionTimeout
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..lib.inference import (
    InferenceResponseFormatError,
    MissingUserHistoryReason,
    compute_user_embedding_result,
    get_inference_settings,
)
from ..security import verify_api_key

router = APIRouter(tags=["embeddings"], dependencies=[Depends(verify_api_key)])
logger = logging.getLogger(__name__)


class UserEmbeddingRequest(BaseModel):
    user_did: str = Field(min_length=1, description="AT Protocol DID of the user")


class UserEmbeddingResponse(BaseModel):
    user_did: str
    status: Literal["ok", "skipped"]
    embedding: list[float] | None
    model_uuid: str | None
    dimension: int | None
    history_like_count: int = Field(ge=0, description="Likes in the recent model-input history")
    history_embedding_count: int = Field(ge=0, description="History items with usable embeddings")
    reason: MissingUserHistoryReason | None


@router.post(
    "/embeddings/user",
    response_model=UserEmbeddingResponse,
    responses={
        502: {"description": "History or inference unavailable, unconfigured, or malformed"},
        504: {"description": "History or inference timed out"},
    },
)
async def user_embedding(
    request: Request, payload: UserEmbeddingRequest
) -> UserEmbeddingResponse:
    """Use actual recent likes to compute one user vector without a post search.

    A successful response identifies the exact user-tower model used. Missing
    history is an explicit skip; upstream failures are errors. The history
    counts describe the model's recent input window, not all indexed likes.
    """
    try:
        inference_base_url, inference_api_key = get_inference_settings()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "inference_not_configured",
                "message": "User-tower inference is not configured",
            },
        ) from exc

    try:
        result = await compute_user_embedding_result(
            payload.user_did, request.app.state.es, inference_base_url, inference_api_key
        )
    except (TimeoutError, httpx.TimeoutException, ConnectionTimeout) as exc:
        logger.warning("User embedding upstream timed out", exc_info=True)
        raise HTTPException(
            status_code=504,
            detail={"code": "upstream_timeout", "message": "User embedding upstream timed out"},
        ) from exc
    except InferenceResponseFormatError as exc:
        logger.warning("Invalid user-tower prediction", exc_info=True)
        raise HTTPException(
            status_code=502,
            detail={
                "code": "invalid_inference_response",
                "message": "User-tower returned an invalid prediction",
            },
        ) from exc
    except Exception as exc:
        logger.exception("User embedding upstream failed")
        raise HTTPException(
            status_code=502,
            detail={"code": "upstream_error", "message": "User embedding upstream failed"},
        ) from exc

    return UserEmbeddingResponse(
        user_did=payload.user_did,
        status="skipped" if result.reason is not None else "ok",
        embedding=result.embedding,
        model_uuid=result.model_uuid,
        dimension=len(result.embedding) if result.embedding is not None else None,
        history_like_count=result.history_like_count,
        history_embedding_count=result.history_embedding_count,
        reason=result.reason,
    )

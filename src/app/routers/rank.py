"""Rank router – exposes ranking models via HTTP.

GET /rank/models
    List available ranking models.

POST /rank/predict
    Rank a list of candidates and return ordered AT URIs plus ranking metadata.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from ..lib.rankers import (
    RankModelNotFoundError,
    RankerError,
    RankerExecutionError,
    list_rankers,
    run_predict,
)
from ..models import RankPredictRequest, RankPredictResult
from ..security import verify_api_key

router = APIRouter(tags=["rank"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class RankModelListResponse(BaseModel):
    """Lists available ranking models."""

    rankers: list[str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/rank/models", response_model=RankModelListResponse)
async def rank_list_models() -> RankModelListResponse:
    """Return the ranking models currently exposed by the API."""
    return RankModelListResponse(rankers=list_rankers())


@router.post(
    "/rank/predict",
    response_model=RankPredictResult,
    responses={
        400: {"description": "Invalid request (e.g. unsupported model configuration)"},
        404: {"description": "Requested ranking model not found"},
        502: {"description": "Upstream ranker service failed"},
    },
)
async def rank_predict(
    request: Request,
    payload: RankPredictRequest,
) -> RankPredictResult:
    """Score and order candidate posts by combining one or more rank models.

    Each model in `models` is run in parallel; its raw scores are normalized
    into [0, 1] using the model's theoretical score bounds, then combined
    into a single score per candidate via a weighted average using each
    model's relative `weight`.  The response lists candidates in descending
    combined-score order with 1-based `rank` positions.  Pass the output of
    `/candidates/generate` directly as the request body.
    """
    try:
        result = await run_predict(payload, request.app.state.es)
    except RankModelNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RankerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RankerExecutionError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return RankPredictResult(rankings=result.rankings)

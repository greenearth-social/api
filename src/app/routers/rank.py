"""Rank router – exposes ranking models via HTTP.

GET /rank/models
    List available ranking models.

POST /rank/predict
    Rank a list of candidates and return ordered AT URIs plus ranking metadata.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..lib.rank import RankModelNotFoundError, RankerError, list_models, run_predict
from ..models import RankModel, RankedCandidate, RankPredictRequest
from ..security import verify_api_key

router = APIRouter(tags=["rank"], dependencies=[Depends(verify_api_key)])


class RankModelListResponse(BaseModel):
    """Lists available ranking models."""

    models: list[RankModel]


class RankPredictResponse(BaseModel):
    """Response body for a ranking request."""

    model: str
    ranked_at_uris: list[str]
    rankings: list[RankedCandidate]


@router.get("/rank/models", response_model=RankModelListResponse)
async def rank_list_models() -> RankModelListResponse:
    """Return the ranking models currently exposed by the API."""
    return RankModelListResponse(models=list_models())


@router.post("/rank/predict", response_model=RankPredictResponse)
async def rank_predict(payload: RankPredictRequest) -> RankPredictResponse:
    """Rank the supplied candidate posts."""
    try:
        result = await run_predict(payload)
    except RankModelNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RankerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return RankPredictResponse(
        model=result.model,
        ranked_at_uris=result.ranked_at_uris,
        rankings=result.rankings,
    )

"""Candidates router – exposes candidate generators via HTTP.

GET /candidates/generators
    List available generators.

POST /candidates/generate
    Run one or more named generators and return de-duplicated candidates.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..models import CandidatePost
from ..lib.candidates import (
    CandidateGenerateRequest,
    GeneratorError,
    GeneratorNotFoundError,
    list_generators,
    run_generate,
)
from ..security import verify_api_key

router = APIRouter(tags=["candidates"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class CandidateGenerateResponse(BaseModel):
    """Response body returning de-duplicated candidates from all generators."""

    candidates: list[CandidatePost]


class GeneratorListResponse(BaseModel):
    """Lists available generator names."""

    generators: list[str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/candidates/generators", response_model=GeneratorListResponse)
async def candidates_list_generators() -> GeneratorListResponse:
    """Return the names of all registered candidate generators."""
    return GeneratorListResponse(generators=list_generators())


@router.post("/candidates/generate", response_model=CandidateGenerateResponse)
async def candidates_generate(
    request: Request,
    payload: CandidateGenerateRequest,
) -> CandidateGenerateResponse:
    """Run one or more named generators and return de-duplicated candidates.

    When multiple generators are specified, candidates from each are
    interleaved according to their proportional weights and then
    de-duplicated (first occurrence wins).
    """
    try:
        result = await run_generate(payload, request.app.state.es)
    except GeneratorNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except GeneratorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return CandidateGenerateResponse(candidates=result.candidates)

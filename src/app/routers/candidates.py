"""Candidates router – exposes candidate generators via HTTP.

GET /candidates/generators
    List available generators.

POST /candidates/generate
    Run one or more named generators and return de-duplicated candidates.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..models import CandidateGenerateRequest, CandidateGenerateResult
from ..lib.candidates import (
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

class GeneratorListResponse(BaseModel):
    """Lists available generator names."""

    generators: list[str] = Field(
        default_factory=list,
        description="Names of all registered candidate generators (use in `GeneratorSpec.name`).",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/candidates/generators", response_model=GeneratorListResponse)
async def candidates_list_generators() -> GeneratorListResponse:
    """Return the names of all registered candidate generators."""
    return GeneratorListResponse(generators=list_generators())


@router.post(
    "/candidates/generate",
    response_model=CandidateGenerateResult,
    responses={
        404: {"description": "A named generator was not found"},
        502: {"description": "Upstream generator service failed"},
    },
)
async def candidates_generate(
    request: Request,
    payload: CandidateGenerateRequest,
) -> CandidateGenerateResult:
    """Run one or more named generators and return de-duplicated candidates.

    When multiple generators are specified, candidates from each are
    interleaved according to their proportional weights and then
    de-duplicated (first occurrence wins).

    Set `video_only: true` to restrict results to posts that contain video.
    Use `infill` to name a fallback generator that fills remaining slots when
    primary generators return fewer candidates than `num_candidates`.
    Pass previously shown AT URIs in `exclude_uris` to avoid repeating posts
    across pagination pages.
    """
    try:
        result = await run_generate(payload, request.app.state.es)
    except GeneratorNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except GeneratorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return CandidateGenerateResult(candidates=result.candidates)

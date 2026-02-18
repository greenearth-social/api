"""Candidates router – exposes candidate generators via HTTP.

GET /candidates/generators
    List available generators.

POST /candidates/generate
    Run a named generator and return candidates.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..lib.candidates import CandidateResult, get_generator, list_generators
from ..security import verify_api_key

router = APIRouter(tags=["candidates"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CandidateGenerateRequest(BaseModel):
    """Request body for the generate endpoint."""

    generator_name: str = Field(..., description="Name of the candidate generator to invoke")
    user_did: str = Field(..., description="AT Protocol DID of the user")
    num_candidates: int = Field(100, ge=1, le=1000, description="Max candidates to return")


class CandidateGenerateResponse(BaseModel):
    """Response body wrapping a CandidateResult."""

    result: CandidateResult


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
    """Run a named candidate generator and return the results."""
    gen = get_generator(payload.generator_name)
    if gen is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown generator: {payload.generator_name}",
        )

    es = request.app.state.es

    try:
        result = await gen.generate(
            es=es,
            user_did=payload.user_did,
            num_candidates=payload.num_candidates,
        )
    except Exception as exc:
        logger.exception("Candidate generator '%s' failed", payload.generator_name)
        raise HTTPException(
            status_code=502,
            detail=f"Generator '{payload.generator_name}' failed",
        ) from exc

    return CandidateGenerateResponse(result=result)

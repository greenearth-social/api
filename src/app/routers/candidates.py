"""Candidates router – exposes candidate generators via HTTP.

GET /candidates/generators
    List available generators.

POST /candidates/generate
    Run one or more named generators and return de-duplicated candidates.
"""

import logging
import math

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..models import CandidatePost
from ..lib.candidates import CandidateResult, get_generator, list_generators
from ..security import verify_api_key

router = APIRouter(tags=["candidates"], dependencies=[Depends(verify_api_key)])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class GeneratorSpec(BaseModel):
    """Specifies a generator and the proportion of candidates it should supply."""

    name: str = Field(..., description="Name of the candidate generator")
    weight: float = Field(
        1.0, gt=0, description="Relative weight — proportional share of total candidates"
    )


class CandidateGenerateRequest(BaseModel):
    """Request body for the generate endpoint."""

    generators: list[GeneratorSpec] = Field(
        ...,
        min_length=1,
        description="List of generators with relative weights",
    )
    user_did: str = Field(..., description="AT Protocol DID of the user")
    num_candidates: int = Field(100, ge=1, le=1000, description="Total candidates to return")
    video_only: bool = Field(False, description="When true, only return posts containing video")
    infill: str | None = Field(
        None,
        description=(
            "Generator used to fill remaining slots when the primary "
            "generators return fewer candidates than requested. "
            "If omitted, no infill is performed."
        ),
    )


class CandidateGenerateResponse(BaseModel):
    """Response body returning de-duplicated candidates from all generators."""

    candidates: list[CandidatePost]


class GeneratorListResponse(BaseModel):
    """Lists available generator names."""

    generators: list[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _allocate_counts(specs: list[GeneratorSpec], total: int) -> list[int]:
    """Distribute *total* candidates across specs proportionally to their weights.

    Uses largest-remainder allocation to avoid rounding errors.
    """
    weight_sum = sum(s.weight for s in specs)
    raw = [(s.weight / weight_sum) * total for s in specs]
    floors = [math.floor(r) for r in raw]
    remainders = [r - f for r, f in zip(raw, floors)]
    leftover = total - sum(floors)
    # Award the leftover slots to the specs with the largest fractional part
    for idx in sorted(range(len(specs)), key=lambda i: -remainders[i]):
        if leftover <= 0:
            break
        floors[idx] += 1
        leftover -= 1
    return floors


def _dedup_candidates(candidates: list[CandidatePost]) -> list[CandidatePost]:
    """Remove duplicate posts (by at_uri), keeping the first occurrence."""
    seen: set[str | None] = set()
    deduped: list[CandidatePost] = []
    for c in candidates:
        if c.at_uri in seen:
            continue
        seen.add(c.at_uri)
        deduped.append(c)
    return deduped


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
    counts = _allocate_counts(payload.generators, payload.num_candidates)

    es = request.app.state.es
    all_candidates: list[CandidatePost] = []

    for spec, count in zip(payload.generators, counts):
        if count <= 0:
            continue

        gen = get_generator(spec.name)
        if gen is None:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown generator: {spec.name}",
            )

        try:
            result = await gen.generate(
                es=es,
                user_did=payload.user_did,
                num_candidates=count,
                video_only=payload.video_only,
            )
        except Exception as exc:
            logger.exception("Candidate generator '%s' failed", spec.name)
            raise HTTPException(
                status_code=502,
                detail=f"Generator '{spec.name}' failed",
            ) from exc

        all_candidates.extend(result.candidates)

    deduped = _dedup_candidates(all_candidates)

    # ---- Infill: top up if we still need more candidates ----
    shortfall = payload.num_candidates - len(deduped)
    if shortfall > 0 and payload.infill is not None:
        infill_gen = get_generator(payload.infill)
        if infill_gen is None:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown infill generator: {payload.infill}",
            )

        try:
            # Ask for extra to compensate for likely dedup losses
            infill_result = await infill_gen.generate(
                es=es,
                user_did=payload.user_did,
                num_candidates=shortfall * 2,
                video_only=payload.video_only,
            )
        except Exception as exc:
            logger.exception("Infill generator '%s' failed", payload.infill)
            raise HTTPException(
                status_code=502,
                detail=f"Infill generator '{payload.infill}' failed",
            ) from exc

        deduped = _dedup_candidates(deduped + infill_result.candidates)

    return CandidateGenerateResponse(candidates=deduped[:payload.num_candidates])

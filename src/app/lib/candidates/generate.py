"""Shared candidate generation pipeline.

Given a ``CandidateGenerateRequest``, runs the specified generators with
proportional allocation, de-duplicates results, and optionally infills with
a fallback generator.  This module is used by both the ``/candidates``
REST API and the XRPC feed-skeleton endpoint.
"""

import logging
import math

from ...models import (
    CandidateGenerateRequest,
    CandidateGenerateResult,
    CandidatePost,
    GeneratorSpec,
)
from .base import CandidateResult, get_generator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def allocate_counts(specs: list[GeneratorSpec], total: int) -> list[int]:
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


def dedup_candidates(candidates: list[CandidatePost]) -> list[CandidatePost]:
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
# Errors
# ---------------------------------------------------------------------------

class GeneratorNotFoundError(Exception):
    """Raised when a requested generator name is not in the registry."""

    def __init__(self, name: str, *, is_infill: bool = False):
        self.name = name
        self.is_infill = is_infill
        kind = "Infill generator" if is_infill else "Generator"
        super().__init__(f"{kind} not found: {name}")


class GeneratorError(Exception):
    """Raised when a generator's ``generate()`` call fails."""

    def __init__(self, name: str, cause: Exception, *, is_infill: bool = False):
        self.name = name
        self.is_infill = is_infill
        super().__init__(f"Generator '{name}' failed: {cause}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_generate(
    request: CandidateGenerateRequest,
    es,
    *,
    swallow_errors: bool = False,
) -> CandidateGenerateResult:
    """Execute a candidate-generation pipeline described by *request*.

    Parameters
    ----------
    request:
        The generation configuration.
    es:
        An ``AsyncElasticsearch`` client.
    swallow_errors:
        If ``True``, generator failures are logged but do not raise.
        Missing generators still raise ``GeneratorNotFoundError``.
        This is useful for feed-skeleton endpoints that should return
        partial results rather than 5xx.
    """
    counts = allocate_counts(request.generators, request.num_candidates)

    all_candidates: list[CandidatePost] = []

    for spec, count in zip(request.generators, counts):
        if count <= 0:
            continue

        gen = get_generator(spec.name)
        if gen is None:
            raise GeneratorNotFoundError(spec.name)

        try:
            result = await gen.generate(
                es=es,
                user_did=request.user_did,
                num_candidates=count,
                video_only=request.video_only,
            )
        except Exception as exc:
            logger.exception("Candidate generator '%s' failed", spec.name)
            if swallow_errors:
                continue
            raise GeneratorError(spec.name, exc) from exc

        all_candidates.extend(result.candidates)

    deduped = dedup_candidates(all_candidates)

    # ---- Infill: top up if we still need more candidates ----
    shortfall = request.num_candidates - len(deduped)
    if shortfall > 0 and request.infill is not None:
        infill_gen = get_generator(request.infill)
        if infill_gen is None:
            raise GeneratorNotFoundError(request.infill, is_infill=True)

        try:
            # Ask for extra to compensate for likely dedup losses
            infill_result = await infill_gen.generate(
                es=es,
                user_did=request.user_did,
                num_candidates=shortfall * 2,
                video_only=request.video_only,
            )
        except Exception as exc:
            logger.exception("Infill generator '%s' failed", request.infill)
            if swallow_errors:
                infill_result = CandidateResult(generator_name=request.infill, candidates=[])
            else:
                raise GeneratorError(request.infill, exc, is_infill=True) from exc

        deduped = dedup_candidates(deduped + infill_result.candidates)

    return CandidateGenerateResult(candidates=deduped[:request.num_candidates])

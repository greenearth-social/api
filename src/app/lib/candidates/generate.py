"""Shared candidate generation pipeline.

Given a ``CandidateGenerateRequest``, runs the specified generators with
proportional allocation, de-duplicates results, and optionally infills with
a fallback generator.  This module is used by both the ``/candidates``
REST API and the XRPC feed-skeleton endpoint.
"""

import asyncio
import logging
import math
import os

from ...models import (
    CandidateGenerateRequest,
    CandidateGenerateResult,
    CandidatePost,
    GeneratorSpec,
)
from ..feed_debug import current_recorder
from ..metrics import get_metric_collector
from ..pipeline_context import DegradationEvent, DegradationStage, current_pipeline_context
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult, get_generator

logger = logging.getLogger(__name__)


try:
    _GENERATOR_TIMEOUT_SEC: float = float(os.environ.get("GE_CANDIDATE_GENERATOR_TIMEOUT_SEC", "4"))
except ValueError:
    _GENERATOR_TIMEOUT_SEC = 4.0


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
) -> CandidateGenerateResult:
    """Execute a candidate-generation pipeline described by *request*.

    Soft-fail behavior is driven by whether a PipelineContext is installed
    (via pipeline_context_scope) on the current task:

    - No context → generator failures raise GeneratorError (hard fail).
    - Context installed, fail_fast=False → failures are logged, recorded as
      DegradationEvent, and the generator contributes an empty result.
    - Context installed, fail_fast=True → failures re-raise after recording.

    Missing generators always raise GeneratorNotFoundError regardless of context.
    """
    counts = allocate_counts(request.generators, request.num_candidates)

    # Resolve generators up front so missing-name errors raise deterministically
    # before any network work begins.
    active: list[tuple[GeneratorSpec, int, CandidateGenerator]] = []
    for spec, count in zip(request.generators, counts):
        if count <= 0:
            continue
        gen = get_generator(spec.name)
        if gen is None:
            raise GeneratorNotFoundError(spec.name)
        active.append((spec, count, gen))

    async def _run_one(
        spec: GeneratorSpec, count: int, gen: CandidateGenerator
    ) -> list[CandidateResult]:
        try:
            async with timed(
                logger,
                "candidates.generate.duration_ms",
                record_metric=True,
                metric_attrs={"generator_name": spec.name},
                count=count,
            ):
                results = [
                    await asyncio.wait_for(
                        gen.generate(
                            es=es,
                            user_did=request.user_did,
                            num_candidates=count,
                            video_only=request.video_only,
                            exclude_uris=request.exclude_uris or None,
                            max_age_hours=request.max_age_hours,
                        ),
                        timeout=_GENERATOR_TIMEOUT_SEC,
                    )
                ]
            if mc := get_metric_collector():
                mc.record(
                    "candidates.generate.success_count",
                    1,
                    generator_name=spec.name,
                    is_infill="false",
                )
                # Share of the requested allocation actually returned, so a
                # generator that quietly under-fills for some users is visible
                # without reading the feed slate.
                returned = sum(len(result.candidates) for result in results)
                mc.record(
                    "candidates.generate.fill_share",
                    min(returned / count, 1.0),
                    generator_name=spec.name,
                    is_infill="false",
                )
            normalized: list[CandidateResult] = []
            for result in results:
                if not result.candidates and result.status == "success":
                    reason = result.reason or "source_returned_no_candidates"
                    logger.warning(
                        "Candidate generator returned no candidates",
                        extra={
                            "generator_name": spec.name,
                            "user_did": request.user_did,
                            "requested_count": count,
                            "max_age_hours": request.max_age_hours,
                            "exclude_count": len(request.exclude_uris or []),
                            "reason": reason,
                        },
                    )
                    normalized.append(
                        result.model_copy(update={"status": "empty", "reason": reason})
                    )
                else:
                    normalized.append(result)
            return normalized
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                logger.warning(
                    "Candidate generator '%s' timed out after %.1fs",
                    spec.name,
                    _GENERATOR_TIMEOUT_SEC,
                )
                outcome = "timeout"
            else:
                logger.exception("Candidate generator '%s' failed", spec.name)
                outcome = "error"
            if mc := get_metric_collector():
                mc.record(
                    "candidates.generate.failure_count",
                    1,
                    generator_name=spec.name,
                    outcome=outcome,
                    is_infill="false",
                )
            ctx = current_pipeline_context()
            if ctx is not None:
                ctx.record(
                    DegradationEvent(
                        stage=DegradationStage.CANDIDATE_GEN,
                        component=spec.name,
                        cause=exc,
                    )
                )
                return [
                    CandidateResult(
                        generator_name=spec.name,
                        candidates=[],
                        status=outcome,
                        reason="generator_timeout" if outcome == "timeout" else "generator_error",
                    )
                ]
            raise GeneratorError(spec.name, exc) from exc

    result_groups = await asyncio.gather(
        *(_run_one(spec, count, gen) for spec, count, gen in active)
    )
    results = [result for group in result_groups for result in group]

    rec = current_recorder()

    all_candidates: list[CandidatePost] = []
    for result in results:
        if rec is not None:
            rec.record_generator_output(result)
        all_candidates.extend(result.candidates)

    deduped = dedup_candidates(all_candidates)

    # ---- Infill: top up if we still need more candidates ----
    shortfall = request.num_candidates - len(deduped)
    if shortfall > 0 and request.infill is not None:
        infill_gen = get_generator(request.infill)
        if infill_gen is None:
            raise GeneratorNotFoundError(request.infill, is_infill=True)

        infill_exclude_uris: list[str] = []
        for uri in request.exclude_uris or []:
            if uri not in infill_exclude_uris:
                infill_exclude_uris.append(uri)
        for c in deduped:
            if c.at_uri and c.at_uri not in infill_exclude_uris:
                infill_exclude_uris.append(c.at_uri)

        try:
            # Infill receives the same freshness window as primary generation;
            # it may compensate for deduplication but must never widen eligibility.
            infill_result = await asyncio.wait_for(
                infill_gen.generate(
                    es=es,
                    user_did=request.user_did,
                    num_candidates=shortfall * 2,
                    video_only=request.video_only,
                    exclude_uris=infill_exclude_uris or None,
                    max_age_hours=request.max_age_hours,
                ),
                timeout=_GENERATOR_TIMEOUT_SEC,
            )
            if mc := get_metric_collector():
                mc.record(
                    "candidates.generate.success_count",
                    1,
                    generator_name=request.infill,
                    is_infill="true",
                )
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                logger.warning(
                    "Infill generator '%s' timed out after %.1fs",
                    request.infill,
                    _GENERATOR_TIMEOUT_SEC,
                )
                outcome = "timeout"
            else:
                logger.exception("Infill generator '%s' failed", request.infill)
                outcome = "error"
            if mc := get_metric_collector():
                mc.record(
                    "candidates.generate.failure_count",
                    1,
                    generator_name=request.infill,
                    outcome=outcome,
                    is_infill="true",
                )
            ctx = current_pipeline_context()
            if ctx is not None:
                ctx.record(
                    DegradationEvent(
                        stage=DegradationStage.CANDIDATE_GEN,
                        component=f"{request.infill}:infill",
                        cause=exc,
                    )
                )
                infill_result = CandidateResult(
                    generator_name=request.infill,
                    candidates=[],
                    status=outcome,
                    reason="generator_timeout" if outcome == "timeout" else "generator_error",
                )
            else:
                raise GeneratorError(request.infill, exc, is_infill=True) from exc

        infill_result = infill_result.model_copy(update={"mode": "infill"})
        if rec is not None:
            rec.record_generator_output(infill_result)
        deduped = dedup_candidates(deduped + infill_result.candidates)

    final = deduped[: request.num_candidates]
    if rec is not None:
        rec.record_final_candidates(final)
    return CandidateGenerateResult(candidates=final)

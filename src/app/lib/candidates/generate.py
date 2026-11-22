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
from collections.abc import Mapping

from ...models import (
    CandidateGenerateRequest,
    CandidateGenerateResult,
    CandidatePost,
    GeneratorSpec,
)
from ..elasticsearch import (
    fetch_post_embeddings_and_politics_scores,
    fetch_post_perspective_scores,
)
from ..embeddings import encode_float32_b64
from ..feed_debug import current_recorder
from ..metrics import get_metric_collector
from ..pipeline_context import DegradationEvent, DegradationStage, current_pipeline_context
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult, get_generator
from .post_features_cache import PostFeatures, get_post_features_cache

logger = logging.getLogger(__name__)


try:
    _GENERATOR_TIMEOUT_SEC: float = float(os.environ.get("GE_CANDIDATE_GENERATOR_TIMEOUT_SEC", "4"))
except ValueError:
    _GENERATOR_TIMEOUT_SEC = 4.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_generator(
    name: str, extra_generators: Mapping[str, CandidateGenerator] | None
) -> CandidateGenerator | None:
    """Request-scoped generators shadow the registry; unknown names are ``None``."""
    if extra_generators is not None and name in extra_generators:
        return extra_generators[name]
    return get_generator(name)


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


class DetailHydrationError(Exception):
    """Raised when the extra data for ``detail="complete"`` could not be fetched."""


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


try:
    _EMBED_HYDRATION_TIMEOUT_SEC: float = float(
        os.environ.get("GE_EMBED_HYDRATION_TIMEOUT_SEC", "1.5")
    )
except ValueError:
    _EMBED_HYDRATION_TIMEOUT_SEC = 1.5

def _needs_hydration(candidate: CandidatePost) -> bool:
    return bool(candidate.at_uri) and (
        not candidate.minilm_l12_embedding or candidate.politics_score is None
    )


def _with_features(candidate: CandidatePost, features: PostFeatures) -> CandidatePost:
    """Fill a candidate's missing embedding and politics score from *features*."""
    update: dict = {}
    if not candidate.minilm_l12_embedding and features.embedding:
        update["minilm_l12_embedding"] = features.embedding
    if candidate.politics_score is None and features.politics_score is not None:
        update["politics_score"] = features.politics_score
    return candidate.model_copy(update=update) if update else candidate


async def hydrate_posts(es, candidates: list[CandidatePost]) -> list[CandidatePost]:
    """Fill missing L12 embeddings and politics scores, from cache or one ES call.

    Both features are fixed once a post is indexed, so they are served from the
    instance's :mod:`post_features_cache` when an earlier render fetched them,
    and only the remainder is fetched from ``posts_recent``. Posts the query
    does not return (no embedding) are cached as such too.
    """
    pending = [c for c in candidates if _needs_hydration(c)]
    if not pending:
        return candidates

    cache = get_post_features_cache()
    known = cache.get_many([c.at_uri for c in pending if c.at_uri])
    missing = list(dict.fromkeys(c.at_uri for c in pending if c.at_uri and c.at_uri not in known))

    if missing:
        try:
            async with timed(logger, "hydrate_posts", n_missing=len(missing)):
                hydration_results = await asyncio.wait_for(
                    fetch_post_embeddings_and_politics_scores(es, missing, index="posts_recent"),
                    timeout=_EMBED_HYDRATION_TIMEOUT_SEC,
                )
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                logger.warning(
                    "Post hydration timed out after %.1fs; continuing without",
                    _EMBED_HYDRATION_TIMEOUT_SEC,
                )
            else:
                logger.exception("Post hydration failed; continuing without")

            ctx = current_pipeline_context()
            if ctx is not None:
                ctx.record(
                    DegradationEvent(
                        stage=DegradationStage.EMBED_HYDRATION,
                        component="fetch_post_embeddings_and_politics_scores",
                        cause=exc,
                    )
                )
        else:
            for uri, vec, politics_score in hydration_results:
                try:
                    embedding: str | None = encode_float32_b64(vec)
                except Exception:
                    embedding = None
                known[uri] = PostFeatures(embedding, politics_score)
            for uri in missing:
                # Not returned: the post has no embedding (or no embedding source text).
                known.setdefault(uri, PostFeatures(None, None))
            for uri in missing:
                cache.put(uri, known[uri])

    if not known:
        return candidates
    return [
        _with_features(c, known[c.at_uri]) if c.at_uri in known and _needs_hydration(c) else c
        for c in candidates
    ]


async def hydrate_complete_details(
    es, candidates: list[CandidatePost]
) -> list[CandidatePost]:
    """Add the fields that ``detail="complete"`` returns beyond the essentials.

    Raises rather than degrading: a caller that asked for complete data can't
    tell a failed lookup from a post that simply has none.
    """
    uris = [c.at_uri for c in candidates if c.at_uri]
    try:
        scores = await asyncio.wait_for(
            fetch_post_perspective_scores(es, uris),
            timeout=_EMBED_HYDRATION_TIMEOUT_SEC,
        )
    except Exception as exc:
        raise DetailHydrationError(f"Fetching complete post details failed: {exc!r}") from exc
    return [
        c.model_copy(update={"perspective_scores": scores[c.at_uri]})
        if c.at_uri in scores else c
        for c in candidates
    ]


async def run_generate(
    request: CandidateGenerateRequest,
    es,
    *,
    extra_generators: Mapping[str, CandidateGenerator] | None = None,
    feed_name: str,
    hydrate_per_generator: bool = False,
) -> CandidateGenerateResult:
    """Execute a candidate-generation pipeline described by *request*.

    With ``hydrate_per_generator`` each generator's candidates get their
    embeddings and politics scores (``hydrate_posts``) in that generator's own
    task, so hydration overlaps the slower generators instead of following
    them all. The feed pipeline sets it. Without it, embeddings are returned
    only when ``request.hydrate_embeddings`` asks for them.

    ``feed_name`` labels metrics with the requested feed, or ``candidates_api``
    for direct candidate-generation requests.

    ``extra_generators`` are request-scoped generators (e.g. the slate API's
    ``external`` generator wrapping caller-supplied posts) consulted by name
    before the global registry, for both primary specs and ``infill``.

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
        gen = _resolve_generator(spec.name, extra_generators)
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
                metric_attrs={
                    "generator_name": spec.name,
                    "feed_name": feed_name
                },
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
            if hydrate_per_generator and results[0].candidates:
                results = [
                    results[0].model_copy(
                        update={"candidates": await hydrate_posts(es, results[0].candidates)}
                    )
                ]
            if results[0].status == "not_run":
                if mc := get_metric_collector():
                    mc.record(
                        "candidates.generate.skipped_count",
                        1,
                        generator_name=spec.name,
                        is_infill="false",
                        feed_name=feed_name,
                        reason=results[0].reason or "",
                    )
                # Preserve diagnostics and infill without treating a skipped
                # retrieval as a successful call with zero fill.
                return results
            if mc := get_metric_collector():
                mc.record(
                    "candidates.generate.success_count",
                    1,
                    generator_name=spec.name,
                    is_infill="false",
                    feed_name=feed_name,
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
                    feed_name=feed_name,
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
            if isinstance(exc, TimeoutError):
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
                    feed_name=feed_name,
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
        infill_gen = _resolve_generator(request.infill, extra_generators)
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
                if infill_result.status == "not_run":
                    mc.record(
                        "candidates.generate.skipped_count",
                        1,
                        generator_name=request.infill,
                        is_infill="true",
                        feed_name=feed_name,
                        reason=infill_result.reason or "",
                    )
                else:
                    mc.record(
                        "candidates.generate.success_count",
                        1,
                        generator_name=request.infill,
                        is_infill="true",
                        feed_name=feed_name,
                    )
        except Exception as exc:
            if isinstance(exc, TimeoutError):
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
                    feed_name=feed_name,
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
        if hydrate_per_generator and infill_result.candidates:
            infill_result = infill_result.model_copy(
                update={"candidates": await hydrate_posts(es, infill_result.candidates)}
            )
        if rec is not None:
            rec.record_generator_output(infill_result)
        deduped = dedup_candidates(deduped + infill_result.candidates)

    final = deduped[:request.num_candidates]
    if getattr(request, 'hydrate_embeddings', False):
        final = await hydrate_posts(es, final)
    elif not hydrate_per_generator:
        # Some generators read embeddings in their own query; a caller that did
        # not ask for them gets the same response as before that change.
        final = [
            c.model_copy(update={"minilm_l12_embedding": None}) if c.minilm_l12_embedding else c
            for c in final
        ]
    if request.detail == "complete":
        final = await hydrate_complete_details(es, final)
    if rec is not None:
        rec.record_final_candidates(final)
    return CandidateGenerateResult(candidates=final)

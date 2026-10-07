"""The ranking pipeline, from a generation request to a trimmed slate.

Shared by the Bluesky feed generator (``getFeedSkeleton``, settings previews)
and the slate API (``POST /slate/generate``). The router owns everything
around the pipeline — identity, preferences, pins, pagination, the cache —
and hands this module a ``CandidateGenerateRequest`` plus a ``SlateConfig``
(ranking template and the post-ranking cutoffs).

Stages, in order: generate → hydrate embeddings → rank (optional) →
``min_rank_score`` cut → MMR diversification (optional) → ``min_mmr_score``
cut → ``max_render_share`` cap. The ``FeedDebugRecorder`` and
``PipelineContext`` installed by :func:`run_pipeline_capturing` observe every
stage so a snapshot of the run can be stored or returned to the caller.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import NamedTuple

from fastapi import HTTPException

from ..documents import FeedSnapshotDocument
from ..models import CandidateGenerateRequest, SlateConfig
from .candidates import run_generate
from .candidates.base import CandidateGenerator
from .candidates.generate import hydrate_posts
from .diversify import mmr_rerank
from .feed_debug import FeedDebugRecorder, current_recorder, feed_debug_scope
from .metrics import get_metric_collector
from .pipeline_context import (
    DegradationEvent,
    DegradationStage,
    PipelineContext,
    current_pipeline_context,
    pipeline_context_scope,
)
from .rankers import run_predict
from .release import api_release_sha
from .request_cache import request_cache_scope
from .telemetry import timed

logger = logging.getLogger(__name__)

FEED_SNAPSHOT_RETENTION_SECONDS = 24 * 60 * 60  # 24 hours

# When the slate cutoffs reject every retrieved candidate, serve the
# pre-cutoff ordering rather than an empty slate. See issue #248.
EMPTY_SLATE_FAIL_OPEN = True


class PipelineResult(NamedTuple):
    """Output of one ranking-pipeline run."""

    uris: list[str]  # final render list, after all cutoffs
    # Candidates cut for scoring below the slate's min_rank_score; recorded as
    # discarded so future generation stops re-fetching and re-ranking them.
    low_score_uris: list[str]


def record_cutoff(feed_name: str, reason: str, uris: list[str]) -> None:
    """Emit the slate-cutoff metric and debug-record the removed URIs."""
    if not uris:
        return
    collector = get_metric_collector()
    if collector:
        collector.record("feed.slate.cutoff_count", len(uris), feed_name=feed_name, reason=reason)
    rec = current_recorder()
    if rec is not None:
        rec.record_cutoff(reason, uris)


async def run_ranking_pipeline(
    slate: SlateConfig,
    gen_request: CandidateGenerateRequest,
    es,
    *,
    feed_name: str,
    extra_generators: Mapping[str, CandidateGenerator] | None = None,
) -> PipelineResult:
    """Generate candidates, optionally rank them, then diversify with MMR.

    After ranking/diversification the slate is cut down by the configured
    quality gates (``min_rank_score``, ``min_mmr_score``, ``max_render_share``);
    posts cut for low rank score are surfaced so the caller can persist them as
    discarded.

    ``extra_generators`` are request-scoped generators resolved by name ahead
    of the registry (see ``run_generate``).

    Runs inside a per-request cache scope so that identical ES queries
    issued by different stages (e.g. ``fetch_recent_liked_post_uris`` in
    both the two-tower generator and the heavy ranker) collapse to a
    single round-trip.
    """
    rec = current_recorder()
    if rec is not None:
        rec.set_generate_request(gen_request)
        rec.diversify = slate.diversify
        if slate.rank_request_template is not None:
            rec.ranker_model = ", ".join(
                spec.name for spec in slate.rank_request_template.models
            )

    ctx = current_pipeline_context()

    async with request_cache_scope():
        async with timed(
            logger,
            "run_generate",
            num_candidates=gen_request.num_candidates,
            n_generators=len(gen_request.generators),
        ):
            result = await run_generate(gen_request, es, extra_generators=extra_generators)
        candidates = result.candidates

        n_retrieved = len(candidates)
        collector = get_metric_collector()
        if collector:
            # Candidate-starvation signals: how full the retrieval came back,
            # and how large the exclusion list driving it has grown.
            if gen_request.num_candidates > 0:
                collector.record(
                    "candidates.generate.retrieved_share",
                    n_retrieved / gen_request.num_candidates,
                    feed_name=feed_name,
                )
            collector.record(
                "feed.slate.exclusion_size",
                len(gen_request.exclude_uris or []),
                feed_name=feed_name,
            )
        if rec is not None:
            rec.record_n_retrieved(n_retrieved)

        if not candidates:
            return PipelineResult([], [])

        # Generators fetch lightweight candidates. Backfill embeddings and topic
        # scores in one batched ES call after deduping to the working set.
        candidates = await hydrate_posts(es, candidates)

        low_score_uris: list[str] = []
        if slate.rank_request_template is not None:
            candidates = [c for c in candidates if c.minilm_l12_embedding]
            if not candidates:
                return PipelineResult([], [])

            rank_req = slate.rank_request_template.model_copy(
                update={"candidates": candidates, "user_did": gen_request.user_did}
            )
            try:
                async with timed(
                    logger,
                    "run_predict",
                    n_candidates=len(candidates),
                    n_models=len(rank_req.models),
                ):
                    rank_result = await run_predict(rank_req, es)
                if rec is not None:
                    rec.record_ranking(rank_result)
                # Reorder CandidatePosts by model rank and stamp rank_score onto each
                # so MMR uses the model's relevance scores, not the generator scores.
                by_uri = {c.at_uri: c for c in candidates if c.at_uri}
                ordered = [
                    by_uri[r.at_uri].model_copy(update={"score": r.rank_score})
                    for r in rank_result.rankings
                    if r.at_uri in by_uri
                ]
            except Exception as exc:
                logger.exception("Ranking stage failed; falling back to unranked ordering")
                if ctx is not None:
                    component = getattr(exc, "name", type(exc).__name__)
                    ctx.record(
                        DegradationEvent(
                            stage=DegradationStage.RANK,
                            component=component,
                            cause=exc,
                        )
                    )
                    # ctx.record re-raises when fail_fast=True, so this is the
                    # fail-open path.
                    ordered = sorted(candidates, key=lambda c: c.score or 0.0, reverse=True)
                else:
                    raise
        else:
            ordered = sorted(candidates, key=lambda c: c.score or 0.0, reverse=True)

        # Kept for the fail-open fallback below: the best posts we retrieved,
        # before any quality gate fired.
        pre_cut_uris = [c.at_uri for c in ordered if c.at_uri]

        if slate.rank_request_template is not None and slate.min_rank_score is not None:
            # ordered is sorted desc by the combined score, so everything from
            # the first sub-floor candidate on is below the floor.
            cut_idx = next(
                (i for i, c in enumerate(ordered) if (c.score or 0.0) < slate.min_rank_score),
                len(ordered),
            )
            low_score_uris = [c.at_uri for c in ordered[cut_idx:] if c.at_uri]
            ordered = ordered[:cut_idx]
            record_cutoff(feed_name, "rank_score", low_score_uris)

        if rec is not None:
            rec.record_order_after_rank([c.at_uri for c in ordered if c.at_uri])

        if slate.diversify:
            if collector:
                collector.record("feed.mmr.input_size", len(ordered), feed_name=feed_name)
            async with timed(
                logger,
                "feed.mmr.duration_ms",
                record_metric=True,
                metric_attrs={"feed_name": feed_name},
                n_candidates=len(ordered),
            ):
                picks = mmr_rerank(ordered)
            final = [c for c, _ in picks]
            if slate.min_mmr_score is not None:
                # Pick scores are not monotone (penalties decay with position),
                # so cutting at the first sub-floor pick is a policy: stop the
                # slate as soon as quality drops below the bar.
                cut_idx = next(
                    (i for i, (_, s) in enumerate(picks) if s < slate.min_mmr_score),
                    len(picks),
                )
                record_cutoff(
                    feed_name, "mmr_score", [c.at_uri for c in final[cut_idx:] if c.at_uri]
                )
                final = final[:cut_idx]
        else:
            final = ordered

        if slate.max_render_share is not None:
            max_keep = max(1, math.floor(slate.max_render_share * n_retrieved))
            if len(final) > max_keep:
                record_cutoff(feed_name, "share", [c.at_uri for c in final[max_keep:] if c.at_uri])
                final = final[:max_keep]

        final_uris = [c.at_uri for c in final if c.at_uri]

        if collector and n_retrieved > 0:
            collector.record(
                "feed.slate.kept_share",
                len(final_uris) / n_retrieved,
                feed_name=feed_name,
            )

        if not final_uris and pre_cut_uris:
            # The quality gates rejected everything we retrieved.
            if collector:
                collector.record("feed.slate.empty_after_cutoff_count", 1, feed_name=feed_name)
            if EMPTY_SLATE_FAIL_OPEN:
                logger.warning(
                    "Slate cutoffs emptied feed '%s' (%d candidates retrieved); failing open",
                    feed_name,
                    n_retrieved,
                )
                final_uris = pre_cut_uris

        if rec is not None:
            rec.record_final_order(final_uris)

    return PipelineResult(final_uris, low_score_uris)


@dataclass(frozen=True)
class PipelineCapture:
    """One captured pipeline run: the snapshot plus what the router may persist."""

    snapshot: FeedSnapshotDocument
    low_score_uris: list[str]
    # The recorder that observed the run, so a caller can also build the full
    # debug document for debug-flagged users.
    recorder: FeedDebugRecorder
    degradations: list[DegradationEvent]


async def run_pipeline_capturing(
    es,
    slate: SlateConfig,
    gen_request: CandidateGenerateRequest,
    *,
    feed_name: str,
    request_id: str,
    regenerated: bool,
    extra_generators: Mapping[str, CandidateGenerator] | None = None,
) -> PipelineCapture:
    """Run the ranking pipeline under a recorder and a pipeline context.

    The recorder is always installed since the snapshot is built for every
    run; ``run_ranking_pipeline``'s own return value carries the URIs cut for
    low rank score so the caller can persist them as discarded.

    A PipelineContext is also installed for every run so degradation events
    and the feed.render.degraded_count metric are always tracked. fail_fast=False
    for now; PostHog per-user flag (issue 279) will pass it in when implemented.
    """
    recorder = FeedDebugRecorder(feed_name=feed_name, regenerated=regenerated)
    generated_at = datetime.now(UTC)
    ctx = PipelineContext(feed_name=feed_name)

    with feed_debug_scope(recorder), pipeline_context_scope(ctx):
        pipeline_result = await run_ranking_pipeline(
            slate, gen_request, es, feed_name=feed_name, extra_generators=extra_generators
        )

    # Emit once only after the pipeline has returned successfully. A render can
    # accumulate several degradation events, so attribute it to the first
    # failure: this preserves the counter's "degraded renders" meaning (and its
    # ratio denominator) while making the primary stage/component actionable.
    # Later events remain in the PipelineContext for debug capture. Early-return
    # fallbacks (for example, every generator yielding no candidates) still pass
    # through here, unlike an emitter at the bottom of run_ranking_pipeline.
    if ctx.degradations and not ctx.fail_fast:
        if collector := get_metric_collector():
            primary = ctx.degradations[0]
            collector.record(
                "feed.render.degraded_count",
                1,
                feed_name=ctx.feed_name,
                stage=primary.stage.value,
                component=primary.component,
            )

    expires_at = generated_at + timedelta(seconds=FEED_SNAPSHOT_RETENTION_SECONDS)
    snapshot = recorder.build_pipeline_metadata(
        request_id=request_id,
        generated_at=generated_at,
        expires_at=expires_at,
        api_release_sha=api_release_sha(),
    )
    return PipelineCapture(
        snapshot=snapshot,
        low_score_uris=pipeline_result.low_score_uris,
        recorder=recorder,
        degradations=list(ctx.degradations),
    )


def feed_request_timeout_sec() -> float:
    """Internal deadline for the feed pipeline, read fresh per call so it can
    be overridden per-request in tests. Set below the Bluesky AppView's 10s
    abort on getFeedSkeleton calls (see #291) so a downstream hang (ES,
    ranker) surfaces as a logged 504 instead of losing the race against the
    client's own timeout with nothing recorded.
    """
    return float(os.environ.get("GE_FEED_REQUEST_TIMEOUT_SEC", "9"))


async def run_pipeline_capturing_with_timeout(
    es,
    slate: SlateConfig,
    gen_request: CandidateGenerateRequest,
    *,
    feed_name: str,
    request_id: str,
    regenerated: bool,
    extra_generators: Mapping[str, CandidateGenerator] | None = None,
) -> PipelineCapture:
    """Enforce ``GE_FEED_REQUEST_TIMEOUT_SEC`` around ``run_pipeline_capturing``."""
    try:
        return await asyncio.wait_for(
            run_pipeline_capturing(
                es,
                slate,
                gen_request,
                feed_name=feed_name,
                request_id=request_id,
                regenerated=regenerated,
                extra_generators=extra_generators,
            ),
            timeout=feed_request_timeout_sec(),
        )
    except TimeoutError:
        logger.error(
            "Feed pipeline exceeded internal timeout (%.0fs) for feed '%s'",
            feed_request_timeout_sec(),
            feed_name,
        )
        raise HTTPException(status_code=504, detail="Feed generation timed out") from None


def record_render_metrics(
    feed_name_from: Callable[[dict[str, object]], str],
) -> Callable[[Callable[..., Awaitable[object]]], Callable[..., Awaitable[object]]]:
    """Record one success/failure counter around a slate-rendering endpoint.

    ``feed_name_from`` derives the ``feed_name`` label from the handler's
    keyword arguments, since each endpoint names its feed differently (an AT
    URI for getFeedSkeleton, a request-body field for the slate API).
    """

    def decorate(endpoint: Callable[..., Awaitable[object]]) -> Callable[..., Awaitable[object]]:
        @wraps(endpoint)
        async def wrapped(*args: object, **kwargs: object) -> object:
            feed_name = feed_name_from(kwargs)
            outcome = "success"
            try:
                return await endpoint(*args, **kwargs)
            except HTTPException as exc:
                outcome = str(exc.status_code)
                raise
            except Exception:
                outcome = "500"
                raise
            finally:
                if collector := get_metric_collector():
                    if outcome == "success":
                        collector.record(
                            "feed.render.success_count",
                            1,
                            feed_name=feed_name,
                        )
                    else:
                        collector.record(
                            "feed.render.failure_count",
                            1,
                            feed_name=feed_name,
                            status_code=outcome,
                        )

        return wrapped

    return decorate

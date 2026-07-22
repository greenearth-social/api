"""XRPC endpoints for AT Protocol Feed Generator.

Implements the two endpoints required by the AT Protocol Feed Generator spec:

  GET /xrpc/app.bsky.feed.describeFeedGenerator
      Declares the feeds this server provides.

  GET /xrpc/app.bsky.feed.getFeedSkeleton
      Returns a feed skeleton (ordered list of AT URIs) for a given feed.

See: https://docs.bsky.app/docs/starter-templates/custom-feeds
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import math
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from threading import Lock
from typing import NamedTuple

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..documents import (
    FeedCacheDocument,
    FeedSnapshotDocument,
    InteractionDocument,
    PipelineItemMeta,
)
from ..feeds import FEEDS, SOCIAL_RADIUS_PRESETS
from ..lib.atproto_auth import verify_auth_header
from ..lib.candidates import run_generate
from ..lib.config import fail_fast
from ..lib.diversify import mmr_rerank
from ..lib.elasticsearch import fetch_post_embeddings
from ..lib.embeddings import encode_float32_b64
from ..lib.feed_cache import DEFAULT_TTL_SECONDS, FeedCache
from ..lib.feed_context import FeedContextPayload, decode_feed_context, encode_feed_context
from ..lib.feed_debug import FeedDebugRecorder, current_recorder, feed_debug_scope
from ..lib.firestore import (
    FEED_DEBUG_RETENTION_DAYS,
    get_recent_discarded_uris,
    get_recent_seen_uris,
    get_user,
    merge_feed_snapshot,
    record_discarded_posts,
    record_interaction,
    record_seen_posts,
    upsert_feed_activity,
    upsert_user,
    write_feed_debug,
)
from ..lib.metrics import get_metric_collector
from ..lib.posthog_client import get_posthog_client, track_interaction, track_session
from ..lib.rankers import run_predict
from ..lib.request_cache import request_cache_scope
from ..lib.telemetry import timed
from ..models import CandidateGenerateRequest, CandidatePost, FeedConfig, FeedCursor

logger = logging.getLogger(__name__)

router = APIRouter(tags=["xrpc"])


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FEED_SNAPSHOT_RETENTION_SECONDS = 900  # 15 minutes
INITIAL_REQUEST_REUSE_SECONDS = 5


@dataclass
class _InitialRequestEntry:
    created_at: float
    future: Future[FeedSkeletonResponse]


_initial_request_lock = Lock()
_initial_requests: dict[tuple[str, str, int], _InitialRequestEntry] = {}


def _claim_initial_request(
    user_did: str,
    feed_name: str,
    limit: int,
) -> tuple[bool, Future[FeedSkeletonResponse]]:
    """Elect one generator for identical initial requests within a short window."""
    now = time.monotonic()
    key = (user_did, feed_name, limit)
    with _initial_request_lock:
        expired = [
            existing_key
            for existing_key, entry in _initial_requests.items()
            if now - entry.created_at >= INITIAL_REQUEST_REUSE_SECONDS
        ]
        for existing_key in expired:
            _initial_requests.pop(existing_key, None)

        existing = _initial_requests.get(key)
        if existing is not None:
            return False, existing.future

        future: Future[FeedSkeletonResponse] = Future()
        _initial_requests[key] = _InitialRequestEntry(created_at=now, future=future)
        return True, future


def _complete_initial_request(
    user_did: str,
    feed_name: str,
    limit: int,
    future: Future[FeedSkeletonResponse],
    *,
    response: FeedSkeletonResponse | None = None,
    error: BaseException | None = None,
) -> None:
    key = (user_did, feed_name, limit)
    if error is not None:
        with _initial_request_lock:
            entry = _initial_requests.get(key)
            if entry is not None and entry.future is future:
                _initial_requests.pop(key, None)
        if not future.done():
            future.set_exception(error)
            # Mark the exception observed for the no-follower case. Awaiting
            # followers still receive the same exception from wrap_future.
            future.exception()
        return
    assert response is not None
    if not future.done():
        future.set_result(response)


def _clear_initial_request_cache() -> None:
    """Test helper for isolating process-local request reuse state."""
    with _initial_request_lock:
        _initial_requests.clear()


def _get_service_did() -> str:
    """Return the DID of this feed generator service.

    Set via the ``GE_FEED_GENERATOR_DID`` environment variable.  For local
    development behind ngrok this will be something like
    ``did:web:xxxx-xxx-xxx.ngrok-free.app``.
    """
    return os.environ.get("GE_FEED_GENERATOR_DID", "did:web:localhost")


def _get_hostname() -> str:
    """Return the public hostname, derived from the service DID."""
    did = _get_service_did()
    # did:web:<hostname> → hostname
    if did.startswith("did:web:"):
        return did[len("did:web:") :]
    return "localhost"


# ---------------------------------------------------------------------------
# Feed catalogue
# ---------------------------------------------------------------------------


def _feed_uri(feed_name: str) -> str:
    return f"at://{_get_service_did()}/app.bsky.feed.generator/{feed_name}"


async def _resolve_username(request: Request, user_did: str) -> str:
    """Resolve the caller's handle from their DID document."""
    resolver = getattr(request.app.state, "id_resolver", None)
    if resolver is None:
        logger.error("id_resolver not initialized")
        raise HTTPException(status_code=500, detail="Identity resolver unavailable")

    did_doc = await resolver.did.resolve(user_did)
    if did_doc is None:
        logger.error("Failed to resolve DID document for %s", user_did)
        raise HTTPException(status_code=500, detail="Username resolution failed")

    username = did_doc.get_handle()
    if not username:
        logger.error("No handle found in DID document for %s", user_did)
        raise HTTPException(status_code=500, detail="Username resolution failed")

    return username


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class FeedLink(BaseModel):
    uri: str = Field(..., description="AT URI of the feed")


class DescribeFeedGeneratorResponse(BaseModel):
    did: str = Field(..., description="DID of the feed generator service")
    feeds: list[FeedLink] = Field(default_factory=list)


class SkeletonItem(BaseModel):
    post: str = Field(..., description="AT URI of a post")
    feed_context: str | None = Field(
        default=None,
        serialization_alias="feedContext",
        description="Signed token echoed back by sendInteractions (max 2000 chars)",
    )


class FeedSkeletonResponse(BaseModel):
    """Response for getFeedSkeleton.

    When ``cursor`` is ``None`` it is omitted from the JSON output — the
    AT Protocol spec requires the field to be absent rather than ``null``.
    """

    model_config = {"populate_by_name": True}

    feed: list[SkeletonItem] = Field(default_factory=list)
    cursor: str | None = Field(default=None, description="Pagination cursor")


# Recognised interaction event names, stored without their
# ``app.bsky.feed.defs#`` lexicon prefix. Unknown events are still stored — this
# set is for reference and lightweight logging only.
INTERACTION_EVENTS = frozenset(
    {
        "requestLess",
        "requestMore",
        "clickthroughItem",
        "clickthroughAuthor",
        "clickthroughEmbed",
        "interactionSeen",
        "interactionLike",
        "interactionRepost",
        "interactionReply",
        "interactionQuote",
        "interactionShare",
    }
)


def _short_event(event: str | None) -> str:
    """Strip the ``app.bsky.feed.defs#`` lexicon prefix, keeping the event name.

    Falls back to the original value when stripping would leave nothing (e.g. a
    value ending in ``#``), so a non-empty event is never replaced with "".
    """
    if not event:
        return ""
    return event.rsplit("#", 1)[-1] or event


class Interaction(BaseModel):
    """A single interaction entry in a sendInteractions request."""

    model_config = {"populate_by_name": True}

    item: str | None = Field(default=None, description="AT URI of the post interacted with")
    event: str | None = Field(
        default=None, description="Interaction event type (app.bsky.feed.defs#...)"
    )
    feed_context: str | None = Field(
        default=None,
        validation_alias="feedContext",
        description="The signed token we attached to the feed item",
    )


class SendInteractionsRequest(BaseModel):
    interactions: list[Interaction] = Field(default_factory=list)


class SendInteractionsResponse(BaseModel):
    """Empty response body, per the app.bsky.feed.sendInteractions lexicon."""


# ---------------------------------------------------------------------------
# Feed pipeline
# ---------------------------------------------------------------------------


async def _hydrate_embeddings(es, candidates: list[CandidatePost]) -> list[CandidatePost]:
    """Fetch missing L12 embeddings in a single batched ES call.

    Candidate generators skip the embedding when reading from ES — the
    array is ~4-5 KB per doc and dominates response size for kNN
    searches. We refetch embeddings here, after dedup, against just
    the candidates that survived. The per-request cache means later
    callers (e.g. the two-tower ranker re-asking for the same URIs)
    pay no additional ES cost.
    """
    missing = [c.at_uri for c in candidates if c.at_uri and not c.minilm_l12_embedding]
    if not missing:
        return candidates

    try:
        async with timed(logger, "hydrate_embeddings", n_missing=len(missing)):
            pairs = await fetch_post_embeddings(es, missing, index="posts_recent")
    except Exception:
        # If the refetch fails, MMR falls back to author-only similarity
        # and the two-tower ranker has its own refetch path. Don't fail
        # the request over a hydration hiccup — unless GE_FAIL_FAST is set,
        # in which case degraded serving is disabled and we surface the error.
        logger.exception("Embedding hydration failed")
        if fail_fast():
            raise
        return candidates

    encoded: dict[str, str] = {}
    for uri, vec in pairs:
        try:
            encoded[uri] = encode_float32_b64(vec)
        except Exception:
            continue

    if not encoded:
        return candidates

    return [
        c.model_copy(update={"minilm_l12_embedding": encoded[c.at_uri]})
        if c.at_uri and not c.minilm_l12_embedding and c.at_uri in encoded
        else c
        for c in candidates
    ]


# When cutoffs empty a slate that still had candidates, serve the best pre-cutoff
# posts anyway (fail open) rather than a blank feed. Flip to False to strictly
# honor the thresholds and return an empty slate instead.
EMPTY_SLATE_FAIL_OPEN = True


class PipelineResult(NamedTuple):
    """Output of one ranking-pipeline run."""

    uris: list[str]  # final render list, after all cutoffs
    # Candidates cut for scoring below the feed's min_rank_score; recorded as
    # discarded so future generation stops re-fetching and re-ranking them.
    low_score_uris: list[str]


def _record_cutoff(feed_name: str, reason: str, uris: list[str]) -> None:
    """Emit the slate-cutoff metric and debug-record the removed URIs."""
    if not uris:
        return
    collector = get_metric_collector()
    if collector:
        collector.record(
            "feed.slate.cutoff_count", len(uris), feed_name=feed_name, reason=reason
        )
    rec = current_recorder()
    if rec is not None:
        rec.record_cutoff(reason, uris)


async def _run_ranking_pipeline(
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    es,
    *,
    feed_name: str,
) -> PipelineResult:
    """Generate candidates, optionally rank them, then diversify with MMR.

    After ranking/diversification the slate is cut down by the feed's quality
    gates (``min_rank_score``, ``min_mmr_score``, ``max_render_share``); posts
    cut for low rank score are surfaced so the caller can persist them as
    discarded.

    Runs inside a per-request cache scope so that identical ES queries
    issued by different stages (e.g. ``fetch_recent_liked_post_uris`` in
    both ``post_similarity`` and the two-tower ranker) collapse to a
    single round-trip.
    """
    rec = current_recorder()
    if rec is not None:
        rec.set_generate_request(gen_request)
        rec.diversify = feed_cfg.diversify
        if feed_cfg.rank_request_template is not None:
            rec.ranker_model = ", ".join(
                spec.name for spec in feed_cfg.rank_request_template.models
            )

    async with request_cache_scope():
        async with timed(
            logger,
            "run_generate",
            num_candidates=gen_request.num_candidates,
            n_generators=len(gen_request.generators),
        ):
            result = await run_generate(gen_request, es)
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

        # Generators fetch lightweight candidates (no embedding); ranker and
        # MMR need embeddings, so backfill in one batched ES call now that
        # the candidate set has been deduped down to the working size.
        candidates = await _hydrate_embeddings(es, candidates)

        low_score_uris: list[str] = []
        if feed_cfg.rank_request_template is not None:
            candidates = [c for c in candidates if c.minilm_l12_embedding]
            if not candidates:
                return PipelineResult([], [])

            rank_req = feed_cfg.rank_request_template.model_copy(
                update={"candidates": candidates, "user_did": gen_request.user_did}
            )
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
        else:
            ordered = sorted(candidates, key=lambda c: c.score or 0.0, reverse=True)

        # Kept for the fail-open fallback below: the best posts we retrieved,
        # before any quality gate fired.
        pre_cut_uris = [c.at_uri for c in ordered if c.at_uri]

        if feed_cfg.rank_request_template is not None and feed_cfg.min_rank_score is not None:
            # ordered is sorted desc by the combined score, so everything from
            # the first sub-floor candidate on is below the floor.
            cut_idx = next(
                (
                    i
                    for i, c in enumerate(ordered)
                    if (c.score or 0.0) < feed_cfg.min_rank_score
                ),
                len(ordered),
            )
            low_score_uris = [c.at_uri for c in ordered[cut_idx:] if c.at_uri]
            ordered = ordered[:cut_idx]
            _record_cutoff(feed_name, "rank_score", low_score_uris)

        if rec is not None:
            rec.record_order_after_rank([c.at_uri for c in ordered if c.at_uri])

        if feed_cfg.diversify:
            picks = mmr_rerank(ordered)
            final = [c for c, _ in picks]
            if feed_cfg.min_mmr_score is not None:
                # Pick scores are not monotone (penalties decay with position),
                # so cutting at the first sub-floor pick is a policy: stop the
                # slate as soon as quality drops below the bar.
                cut_idx = next(
                    (i for i, (_, s) in enumerate(picks) if s < feed_cfg.min_mmr_score),
                    len(picks),
                )
                _record_cutoff(
                    feed_name, "mmr_score", [c.at_uri for c in final[cut_idx:] if c.at_uri]
                )
                final = final[:cut_idx]
        else:
            final = ordered

        if feed_cfg.max_render_share is not None:
            max_keep = max(1, math.floor(feed_cfg.max_render_share * n_retrieved))
            if len(final) > max_keep:
                _record_cutoff(
                    feed_name, "share", [c.at_uri for c in final[max_keep:] if c.at_uri]
                )
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
                collector.record(
                    "feed.slate.empty_after_cutoff_count", 1, feed_name=feed_name
                )
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


async def _run_pipeline_capturing(
    request: Request,
    db,
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    *,
    feed_name: str,
    user_did: str,
    request_id: str,
    regenerated: bool,
    debug_enabled: bool,
    applied_social_radius: int | None = None,
) -> tuple[FeedSnapshotDocument, list[str]]:
    """Run the ranking pipeline, capturing a lightweight snapshot for every
    feed load and a full debug document for debug-flagged users.

    The snapshot is written inline (no handle resolution needed) so the
    transparency API can re-render any served feed.  The full debug document
    (for the CLI tool) is written in a background task only when
    ``debug_enabled`` is true.

    The recorder is always installed (not just when ``debug_enabled``) since
    the snapshot is built for every request; ``_run_ranking_pipeline``'s own
    return value carries the URIs cut for low rank score so the caller can
    persist them as discarded, alongside the snapshot.
    """
    recorder = FeedDebugRecorder(feed_name=feed_name, regenerated=regenerated)
    generated_at = datetime.now(timezone.utc)

    with feed_debug_scope(recorder):
        pipeline_result = await _run_ranking_pipeline(
            feed_cfg, gen_request, request.app.state.es, feed_name=feed_name
        )

    expires_at = generated_at + timedelta(seconds=FEED_SNAPSHOT_RETENTION_SECONDS)
    snapshot = recorder.build_pipeline_metadata(
        request_id=request_id,
        generated_at=generated_at,
        expires_at=expires_at,
        applied_social_radius=applied_social_radius,
    )

    # Full debug document only for debug-flagged users, in background.
    if debug_enabled:
        _spawn_background(
            _write_feed_debug(request, db, recorder, request_id, user_did, generated_at)
        )

    return snapshot, pipeline_result.low_score_uris


def _feed_request_timeout_sec() -> float:
    """Internal deadline for the feed pipeline, read fresh per call so it can
    be overridden per-request in tests. Set below the Cloud Run request
    timeout so a downstream hang (ES, ranker) surfaces as a logged 504 instead
    of the platform silently killing the connection with nothing recorded.
    """
    return float(os.environ.get("GE_FEED_REQUEST_TIMEOUT_SEC", "45"))


async def _run_pipeline_capturing_with_timeout(
    request: Request,
    db,
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    *,
    feed_name: str,
    user_did: str,
    request_id: str,
    regenerated: bool,
    debug_enabled: bool,
    applied_social_radius: int | None = None,
) -> tuple[FeedSnapshotDocument, list[str]]:
    """Enforce ``GE_FEED_REQUEST_TIMEOUT_SEC`` around ``_run_pipeline_capturing``."""
    try:
        return await asyncio.wait_for(
            _run_pipeline_capturing(
                request,
                db,
                feed_cfg,
                gen_request,
                feed_name=feed_name,
                user_did=user_did,
                request_id=request_id,
                regenerated=regenerated,
                debug_enabled=debug_enabled,
                applied_social_radius=applied_social_radius,
            ),
            timeout=_feed_request_timeout_sec(),
        )
    except TimeoutError:
        logger.error(
            "Feed pipeline exceeded internal timeout (%.0fs) for feed '%s'",
            _feed_request_timeout_sec(),
            feed_name,
        )
        raise HTTPException(status_code=504, detail="Feed generation timed out") from None


def _snapshot_page(
    snapshot: FeedSnapshotDocument,
    uris: list[str],
) -> FeedSnapshotDocument:
    """Restrict pipeline metadata to posts actually returned in one page."""
    meta_by_uri = {meta.at_uri: meta for meta in snapshot.items_meta}
    items_meta = [
        meta_by_uri.get(uri, PipelineItemMeta(at_uri=uri))
        for uri in uris
    ]
    diagnostics = [
        diagnostic.model_copy(
            update={
                "contributed_count": sum(
                    1
                    for meta in items_meta
                    if any(g.name == diagnostic.name for g in meta.generators)
                )
            }
        )
        for diagnostic in snapshot.generator_diagnostics
    ]
    return snapshot.model_copy(
        update={
            "items": uris,
            "items_meta": items_meta,
            "generator_diagnostics": diagnostics,
        }
    )


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

BATCH_MULTIPLIER = 5  # how many pages of results to fetch for each cursor session
MAX_BATCH_SIZE = 100  # minimum number of results to fetch for each cursor session


def _batch_size(limit: int) -> int:
    """How many candidates to pre-generate for a new cursor session."""
    return min(limit * BATCH_MULTIPLIER, MAX_BATCH_SIZE)


def _get_feed_cache(request: Request) -> FeedCache:
    """Return the FeedCache attached during app startup."""
    cache = getattr(request.app.state, "feed_cache", None)
    if cache is None:
        logger.error("FeedCache not initialized")
        raise HTTPException(status_code=500, detail="Feed cache unavailable")
    return cache


# ---------------------------------------------------------------------------
# feedContext helpers
# ---------------------------------------------------------------------------


def _make_feed_context(user_did: str, feed_name: str, request_id: str) -> str:
    """Build the signed feedContext token shared by every item in a response.

    ``request_id`` doubles as the feed-cache key, so the served item order can be
    recovered from the cache during its TTL window.
    """
    return encode_feed_context(
        FeedContextPayload(
            did=user_did,
            feed=feed_name,
            rid=request_id,
            iat=int(time.time()),
        )
    )


def _skeleton_items(uris: list[str], feed_context: str) -> list[SkeletonItem]:
    return [SkeletonItem(post=uri, feed_context=feed_context) for uri in uris]


# Fire-and-forget background tasks (Firestore session writes, …). Keeping a
# strong reference here prevents the event loop from garbage-collecting them
# mid-flight; the done callback removes them once they complete.
_background_tasks: set[asyncio.Task] = set()


def _spawn_background(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def _generation_exclusions(db, user_did: str, feed_cfg: FeedConfig) -> list[str]:
    """Fetch post URIs to exclude from candidate generation, de-duped.

    Combines the user's recently-seen posts (for feeds with
    ``exclude_seen_posts``) and posts previously discarded for low rank score
    (for feeds with a ``min_rank_score`` floor).  Fail-soft on each source: a
    Firestore hiccup should degrade the feature (possible repeats) rather than
    break feed serving, so errors are logged and yield an empty list.
    """

    async def _seen() -> list[str]:
        if not feed_cfg.exclude_seen_posts:
            return []
        try:
            return await get_recent_seen_uris(db, user_did)
        except Exception:
            logger.exception("Failed to fetch seen posts for user '%s'", user_did)
            return []

    async def _discarded() -> list[str]:
        if feed_cfg.min_rank_score is None:
            return []
        try:
            return await get_recent_discarded_uris(db, user_did)
        except Exception:
            logger.exception("Failed to fetch discarded posts for user '%s'", user_did)
            return []

    seen_uris, discarded_uris = await asyncio.gather(_seen(), _discarded())
    return list(dict.fromkeys(seen_uris + discarded_uris))


async def _record_discarded(db, user_did: str, post_uris: list[str]) -> None:
    """Persist low-rank-score post URIs in the background; failures are logged."""
    try:
        await record_discarded_posts(db, user_did, post_uris)
    except Exception:
        logger.exception("Failed to record discarded posts for user '%s'", user_did)


async def _record_session(request: Request, user_did: str, feed_name: str, db) -> None:
    """Resolve the caller's handle and upsert user + feed-activity docs.

    Runs as a background task so the user-facing latency of getFeedSkeleton
    isn't paying for firebase roundtrips.
    Failures are logged but do not surface to the caller.
    """
    try:
        username = await _resolve_username(request, user_did)
    except Exception:
        logger.exception("Failed to resolve username for %s in background", user_did)
        return

    now = datetime.now(timezone.utc)

    try:
        await upsert_user(db, user_did, username)
    except Exception:
        logger.exception("Failed to upsert user '%s' in Firestore", user_did)

    try:
        await upsert_feed_activity(db, user_did, feed_name)
    except Exception:
        logger.exception(
            "Failed to record feed activity for user '%s', feed '%s'", user_did, feed_name
        )

    try:
        track_session(get_posthog_client(), user_did, username, feed_name, now)
    except Exception:
        logger.exception("Failed to track PostHog session for user '%s'", user_did)


async def _resolve_handles(request: Request, dids: set[str]) -> dict[str, str]:
    """Best-effort batch DID→handle resolution; failures are skipped.

    Used for feed-debug capture only, so a DID that fails to resolve is simply
    omitted from the result rather than raising.
    """
    resolver = getattr(request.app.state, "id_resolver", None)
    if resolver is None or not dids:
        return {}

    did_list = list(dids)

    async def _one(did: str) -> str | None:
        try:
            did_doc = await resolver.did.resolve(did)
            return did_doc.get_handle() if did_doc is not None else None
        except Exception:
            return None

    handles = await asyncio.gather(*(_one(did) for did in did_list))
    return {did: handle for did, handle in zip(did_list, handles) if handle}


async def _write_feed_snapshot_background(
    db,
    user_did: str,
    request_id: str,
    snapshot,
) -> None:
    """Create or extend the lightweight feed snapshot in a background task.

    Separated from ``_run_pipeline_capturing`` so the Firestore write stays
    off the feed-serving hot path. Failures are logged, never surfaced.
    """
    if not snapshot.items:
        return
    try:
        truncated = await merge_feed_snapshot(db, user_did, request_id, snapshot)
        if truncated:
            logger.warning(
                "Feed snapshot reached item limit for user '%s', request '%s'",
                user_did,
                request_id,
                extra={"user_did": user_did, "request_id": request_id},
            )
            collector = get_metric_collector()
            if collector is not None:
                collector.record(
                    "feed.snapshot.truncated_count", 1, feed_name=snapshot.feed_name
                )
    except Exception:
        logger.exception(
            "Failed to write feed snapshot for user '%s', request '%s'",
            user_did,
            request_id,
        )


async def _write_feed_debug(
    request: Request,
    db,
    recorder: FeedDebugRecorder,
    request_id: str,
    user_did: str,
    generated_at: datetime,
) -> None:
    """Resolve author handles, assemble the debug document, and persist it.

    This coroutine does the work but does *not* spawn its own task: the caller
    (``_run_pipeline_capturing``) is responsible for running it via
    ``_spawn_background`` so handle resolution and the Firestore write stay off
    the feed-serving hot path. Failures are logged, never surfaced.
    """
    try:
        author_usernames = await _resolve_handles(request, recorder.author_dids())
        username = author_usernames.get(user_did)
        expires_at = generated_at + timedelta(days=FEED_DEBUG_RETENTION_DAYS)
        doc = recorder.build_document(
            request_id=request_id,
            username=username,
            generated_at=generated_at,
            expires_at=expires_at,
            author_usernames=author_usernames,
        )
        await write_feed_debug(db, doc)
    except Exception:
        logger.exception("Failed to write feed debug record for user '%s'", user_did)


async def _record_interactions(db, interactions: list["Interaction"]) -> None:
    """Verify each interaction's feedContext and persist the valid ones.

    The signed feedContext is the trust anchor: interactions with a missing or
    forged token are dropped (and logged) rather than written, so the public
    endpoint can't be used to poison the data. Runs as a background task.

    ``interactionSeen`` items are additionally appended to the user's seen-posts
    buckets so they can be excluded from future feed generations -- but only for
    feeds whose config has ``exclude_seen_posts`` enabled. The raw interaction is
    always stored regardless.
    """
    # Seen URIs collected per user so we can record them with a single write
    # per user after the per-interaction loop.
    seen_by_user: dict[str, list[str]] = {}

    for ix in interactions:
        payload = decode_feed_context(ix.feed_context or "")
        if payload is None:
            logger.warning("Dropping interaction with missing/invalid feedContext")
            continue

        event = _short_event(ix.event)
        if event and event not in INTERACTION_EVENTS:
            logger.warning("Recording interaction with unrecognized event: %s", event)

        feed_cfg = FEEDS.get(payload.feed)
        if (
            event == "interactionSeen"
            and ix.item
            and feed_cfg is not None
            and feed_cfg.exclude_seen_posts
        ):
            seen_by_user.setdefault(payload.did, []).append(ix.item)

        doc = InteractionDocument(
            user_did=payload.did,
            item_uri=ix.item,
            event=event,
            feed_name=payload.feed,
            request_id=payload.rid,
            feed_generated_at=datetime.fromtimestamp(payload.iat, tz=timezone.utc),
        )
        try:
            await record_interaction(db, doc)
        except Exception:
            logger.exception("Failed to record interaction for user '%s'", payload.did)

        if event:
            try:
                track_interaction(
                    get_posthog_client(),
                    payload.did,
                    event,
                    payload.feed,
                    ix.item,
                    doc.created_at,
                )
            except Exception:
                logger.exception(
                    "Failed to track PostHog interaction '%s' for user '%s'", event, payload.did
                )

    for did, uris in seen_by_user.items():
        try:
            await record_seen_posts(db, did, uris)
        except Exception:
            logger.exception("Failed to record seen posts for user '%s'", did)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _feed_name_for_metrics(feed: object) -> str:
    if not isinstance(feed, str):
        return "unknown"
    try:
        rkey = feed.split("/")[-1]
        collection = feed.split("/")[-2] if feed.count("/") >= 4 else ""
    except Exception:
        return "unknown"
    if collection != "app.bsky.feed.generator":
        return "unknown"
    if rkey in FEEDS:
        return rkey
    for name, config in FEEDS.items():
        if config.internal_rkey == rkey:
            return name
    return "unknown"


def _record_feed_render_metrics(
    endpoint: Callable[..., Awaitable[FeedSkeletonResponse]],
) -> Callable[..., Awaitable[FeedSkeletonResponse]]:
    """Record one success/failure counter around a feed render request."""

    @wraps(endpoint)
    async def wrapped(*args: object, **kwargs: object) -> FeedSkeletonResponse:
        feed_name = _feed_name_for_metrics(kwargs.get("feed"))
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


@router.get("/.well-known/did.json", response_class=JSONResponse)
async def well_known_did() -> JSONResponse:
    """Serve the DID document for ``did:web`` resolution.

    Bluesky's AppView fetches ``https://<hostname>/.well-known/did.json`` to
    discover the feed generator's service endpoint.
    """
    service_did = _get_service_did()
    hostname = _get_hostname()

    return JSONResponse(
        content={
            "@context": ["https://www.w3.org/ns/did/v1"],
            "id": service_did,
            "service": [
                {
                    "id": "#bsky_fg",
                    "type": "BskyFeedGenerator",
                    "serviceEndpoint": f"https://{hostname}",
                },
            ],
        },
        media_type="application/json",
    )


@router.get(
    "/xrpc/app.bsky.feed.describeFeedGenerator",
    response_model=DescribeFeedGeneratorResponse,
)
async def describe_feed_generator() -> DescribeFeedGeneratorResponse:
    """Declare which feeds this generator serves."""
    return DescribeFeedGeneratorResponse(
        did=_get_service_did(),
        feeds=[FeedLink(uri=_feed_uri(name)) for name in FEEDS],
    )


@router.get(
    "/xrpc/app.bsky.feed.getFeedSkeleton",
    response_model=FeedSkeletonResponse,
    response_model_exclude_none=True,
)
@_record_feed_render_metrics
async def get_feed_skeleton(
    request: Request,
    feed: str = Query(..., description="AT URI of the requested feed"),
    limit: int = Query(30, ge=1, le=100, description="Max number of posts"),
    cursor: str | None = Query(None, description="Pagination cursor"),
) -> FeedSkeletonResponse:
    """Return a feed skeleton for the requested feed.

    The ``feed`` query parameter must be the full AT URI of one of the
    feeds declared by ``describeFeedGenerator``.
    """
    # Resolve which feed was requested by extracting the rkey (feed short
    # name) from the AT URI.  The URI's authority is the *publisher* DID
    # (the account that owns the record), which differs from the service DID,
    # so we match on the rkey alone.
    feed_name: str | None = None
    try:
        # at://<did>/app.bsky.feed.generator/<rkey>
        rkey = feed.split("/")[-1]
        collection = feed.split("/")[-2] if feed.count("/") >= 4 else ""
    except Exception:
        rkey = ""
        collection = ""

    if collection == "app.bsky.feed.generator":
        if rkey in FEEDS:
            feed_name = rkey
        else:
            for key, cfg in FEEDS.items():
                if cfg.internal_rkey == rkey:
                    feed_name = key
                    break

    if feed_name is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown feed: {feed}",
        )

    feed_cfg = FEEDS[feed_name]

    # Cloud Scheduler probe bypass: if GE_PROBE_SECRET is set and the request
    # carries the matching X-Probe-Secret header, skip AT Protocol auth and
    # use the configured probe DID so the full pipeline runs and emits latency metrics.
    _probe_secret = os.environ.get("GE_PROBE_SECRET")
    is_probe = bool(_probe_secret) and hmac.compare_digest(
        request.headers.get("X-Probe-Secret", ""), _probe_secret
    )
    if is_probe:
        user_did = os.environ.get("GE_PROBE_USER_DID", "did:plc:s4tl2ajfsnstzuxtegl7r33g")
    else:
        user_did = await verify_auth_header(request, service_did=_get_service_did())

        if not user_did:
            if request.headers.get("Authorization"):
                logger.warning("Auth header present but verification failed for feed %s", feed_name)
            else:
                logger.warning("No auth header present for feed %s", feed_name)
            raise HTTPException(
                status_code=401,
                detail="Authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # Record authenticated users in Firestore for backend analytics. Runs in
    # the background since this isn't essential for serving.
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        logger.error("Firestore client not initialized")
        raise HTTPException(status_code=500, detail="Firestore unavailable")

    _spawn_background(_record_session(request, user_did, feed_name, db))

    # Per-user opt-in: capture pipeline debugging info for this feed load. This
    # costs one extra Firestore read per request; fail-soft so a hiccup degrades
    # to no-debug rather than breaking feed serving.
    debug_enabled = False
    user_doc = None
    try:
        user_doc = await get_user(db, user_did)
        debug_enabled = bool(user_doc and user_doc.debug_feeds)
    except Exception:
        logger.exception("Failed to read debug flag for user '%s'", user_did)

    # Apply social-radius preference override to your-feed generator weights.
    # The override is computed once and threaded through model_copy in both
    # generation paths so the shared module-level template is never mutated.
    generators_override: dict = {}
    applied_social_radius: int | None = None
    if feed_name == "your-feed":
        applied_social_radius = user_doc.social_radius if user_doc is not None else 3
        preset = SOCIAL_RADIUS_PRESETS.get(applied_social_radius)
        if preset is not None:
            generators_override = {"generators": preset}

    feed_cache = _get_feed_cache(request)

    async with timed(
        logger,
        "feed.render.duration_ms",
        record_metric=True,
        metric_attrs={"feed_name": feed_name},
    ):
        # ------------------------------------------------------------------
        # If the client sent a cursor, try to serve the next page from cache.
        # ------------------------------------------------------------------
        if cursor is not None:
            try:
                parsed = FeedCursor.decode(cursor)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid cursor")

            async with timed(logger, "feedcache_retrieve", cache_id=parsed.id):
                cache_doc = await feed_cache.retrieve_document(parsed.id)
            if cache_doc is not None:
                cached_uris = cache_doc.items
                if parsed.offset < len(cached_uris):
                    # Serve from the existing cached batch.
                    page = cached_uris[parsed.offset : parsed.offset + limit]
                    next_offset = parsed.offset + len(page)
                    next_cursor: str | None = None
                    if page:
                        # Always return a cursor when there are results.
                        # When next_offset reaches the end of the cache the
                        # next request will fall into the regeneration branch
                        # below, which fetches fresh candidates.
                        next_cursor = FeedCursor(id=parsed.id, offset=next_offset).encode()
                    feed_context = _make_feed_context(user_did, feed_name, parsed.id)
                    cached_snapshot = FeedSnapshotDocument(
                        request_id=parsed.id,
                        items=cached_uris,
                        feed_name=cache_doc.feed_name or feed_name,
                        generated_at=cache_doc.generated_at or datetime.now(timezone.utc),
                        expires_at=cache_doc.expires_at,
                        generator_diagnostics=cache_doc.generator_diagnostics,
                        applied_social_radius=cache_doc.applied_social_radius,
                        items_meta=cache_doc.items_meta,
                    )
                    if not is_probe:
                        await _write_feed_snapshot_background(
                            db,
                            user_did,
                            parsed.id,
                            _snapshot_page(cached_snapshot, page),
                        )
                    return FeedSkeletonResponse(
                        feed=_skeleton_items(page, feed_context),
                        cursor=next_cursor,
                    )

                # Offset is at or past the end — regenerate with exclusions.
                batch = _batch_size(limit)
                excluded = await _generation_exclusions(db, user_did, feed_cfg)
                # Dedup while preserving order; the cached batch and the
                # seen/discarded posts can overlap.
                exclude_uris = list(dict.fromkeys(cached_uris + excluded))
                gen_request = feed_cfg.gen_request_template.model_copy(
                    update={
                        "user_did": user_did,
                        "num_candidates": batch,
                        "exclude_uris": exclude_uris,
                        **generators_override,
                    }
                )

                generated_snapshot, low_score_uris = await _run_pipeline_capturing_with_timeout(
                    request,
                    db,
                    feed_cfg,
                    gen_request,
                    feed_name=feed_name,
                    user_did=user_did,
                    request_id=parsed.id,
                    regenerated=True,
                    debug_enabled=debug_enabled,
                    applied_social_radius=applied_social_radius,
                )
                if low_score_uris:
                    _spawn_background(_record_discarded(db, user_did, low_score_uris))
                new_uris = generated_snapshot.items
                if new_uris:
                    async with timed(logger, "feedcache_append", cache_id=parsed.id):
                        updated = await feed_cache.append_document(
                            parsed.id,
                            new_uris,
                            generated_snapshot.items_meta,
                        )
                    if updated is not None:
                        page = new_uris[:limit]
                        next_offset = parsed.offset + len(page)
                        # A short page still gets a cursor: the next request
                        # lands at end-of-cache and regenerates again (the
                        # ranking session restarts with fresh exclusions).
                        next_cursor = FeedCursor(id=parsed.id, offset=next_offset).encode()
                        feed_context = _make_feed_context(user_did, feed_name, parsed.id)
                        if not is_probe:
                            await _write_feed_snapshot_background(
                                db,
                                user_did,
                                parsed.id,
                                _snapshot_page(generated_snapshot, page),
                            )
                        return FeedSkeletonResponse(
                            feed=_skeleton_items(page, feed_context),
                            cursor=next_cursor,
                        )

                # Append failed or nothing new — end of feed.
                return FeedSkeletonResponse(feed=[])

            # Cache miss (expired / evicted) — fall through to generate fresh.

        # ------------------------------------------------------------------
        # No cursor or cache miss — generate a fresh batch.
        # ------------------------------------------------------------------
        reuse_future: Future[FeedSkeletonResponse] | None = None
        if cursor is None and not is_probe:
            is_leader, reuse_future = _claim_initial_request(user_did, feed_name, limit)
            if not is_leader:
                reused = await asyncio.wrap_future(reuse_future)
                return reused.model_copy(deep=True)

        try:
            batch = _batch_size(limit)
            exclude_uris = await _generation_exclusions(db, user_did, feed_cfg)
            gen_request = feed_cfg.gen_request_template.model_copy(
                update={
                    "user_did": user_did,
                    "num_candidates": batch,
                    "exclude_uris": exclude_uris,
                    **generators_override,
                }
            )

            # The request id identifies this response; when we cache a batch it doubles
            # as the cache key so the served order can be recovered from interactions.
            # Generated up front so it can key the debug record too.
            request_id = uuid.uuid4().hex

            generated_snapshot, low_score_uris = await _run_pipeline_capturing_with_timeout(
                request,
                db,
                feed_cfg,
                gen_request,
                feed_name=feed_name,
                user_did=user_did,
                request_id=request_id,
                regenerated=False,
                debug_enabled=debug_enabled,
                applied_social_radius=applied_social_radius,
            )
            if low_score_uris:
                _spawn_background(_record_discarded(db, user_did, low_score_uris))
            all_uris = generated_snapshot.items

            # Pinned posts are a Bluesky presentation concern and are deliberately
            # excluded from observability snapshots and source diagnostics.
            if feed_cfg.pinned_post_uri:
                cache_uris = [uri for uri in all_uris if uri != feed_cfg.pinned_post_uri]
                generated_page = cache_uris[: max(0, limit - 1)]
                page = [feed_cfg.pinned_post_uri, *generated_page]
                consumed = len(generated_page)
            else:
                cache_uris = all_uris
                generated_page = all_uris[:limit]
                page = generated_page
                consumed = len(generated_page)

            if not is_probe:
                await _write_feed_snapshot_background(
                    db,
                    user_did,
                    request_id,
                    _snapshot_page(generated_snapshot, generated_page),
                )

            # Store every non-empty batch — even a (possibly cutoff-shortened)
            # batch that fits in one page — so paging past it regenerates
            # instead of ending the feed. ``consumed`` (computed above) counts
            # only the generated URIs the first page actually displayed (a
            # pinned page consumes limit-1 generated URIs, not limit), or
            # later pages would skip posts.
            next_cursor = None
            if cache_uris:
                cache_meta_by_uri = {
                    meta.at_uri: meta for meta in generated_snapshot.items_meta
                }
                cache_items_meta = [
                    cache_meta_by_uri[uri]
                    for uri in cache_uris
                    if uri in cache_meta_by_uri
                ]
                async with timed(logger, "feedcache_store", cache_id=request_id):
                    await feed_cache.store_document(
                        request_id,
                        FeedCacheDocument(
                            items=cache_uris,
                            items_meta=cache_items_meta,
                            generator_diagnostics=generated_snapshot.generator_diagnostics,
                            applied_social_radius=applied_social_radius,
                            feed_name=feed_name,
                            generated_at=generated_snapshot.generated_at,
                            expires_at=datetime.now(timezone.utc)
                            + timedelta(seconds=DEFAULT_TTL_SECONDS),
                        ),
                    )
                next_cursor = FeedCursor(id=request_id, offset=consumed).encode()

            feed_context = _make_feed_context(user_did, feed_name, request_id)
            response = FeedSkeletonResponse(
                feed=_skeleton_items(page, feed_context),
                cursor=next_cursor,
            )
            if reuse_future is not None:
                _complete_initial_request(
                    user_did,
                    feed_name,
                    limit,
                    reuse_future,
                    response=response.model_copy(deep=True),
                )
            return response
        except BaseException as error:
            if reuse_future is not None:
                _complete_initial_request(
                    user_did,
                    feed_name,
                    limit,
                    reuse_future,
                    error=error,
                )
            raise


@router.post(
    "/xrpc/app.bsky.feed.sendInteractions",
    response_model=SendInteractionsResponse,
)
async def send_interactions(
    request: Request,
    body: SendInteractionsRequest,
) -> SendInteractionsResponse:
    """Receive user interaction signals forwarded by the AppView.

    This endpoint is public: the user's identity comes from the signed
    ``feedContext`` we issued in getFeedSkeleton, not from request auth. Each
    interaction is verified and persisted in the background; forged or
    unverifiable ones are dropped. Always returns an empty object per the
    lexicon.
    """
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        logger.error("Firestore client not initialized")
        raise HTTPException(status_code=500, detail="Firestore unavailable")

    _spawn_background(_record_interactions(db, body.interactions))

    return SendInteractionsResponse()

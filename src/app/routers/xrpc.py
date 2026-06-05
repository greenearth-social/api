"""XRPC endpoints for AT Protocol Feed Generator.

Implements the two endpoints required by the AT Protocol Feed Generator spec:

  GET /xrpc/app.bsky.feed.describeFeedGenerator
      Declares the feeds this server provides.

  GET /xrpc/app.bsky.feed.getFeedSkeleton
      Returns a feed skeleton (ordered list of AT URIs) for a given feed.

See: https://docs.bsky.app/docs/starter-templates/custom-feeds
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..documents import InteractionDocument
from ..lib.candidates import run_generate
from ..lib.diversify import mmr_rerank

from ..lib.elasticsearch import fetch_post_embeddings
from ..lib.embeddings import encode_float32_b64
from ..lib.feed_cache import FeedCache, DEFAULT_TTL_SECONDS
from ..lib.feed_context import FeedContextPayload, decode_feed_context, encode_feed_context
from ..lib.rankers import run_predict
from ..models import CandidateGenerateRequest, CandidatePost, FeedConfig, FeedCursor

from ..lib.atproto_auth import verify_auth_header
from ..lib.firestore import (
    get_recent_seen_uris,
    record_interaction,
    record_seen_posts,
    upsert_feed_activity,
    upsert_user,
)
from ..lib.request_cache import request_cache_scope
from ..lib.telemetry import timed
from ..feeds import FEEDS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["xrpc"])


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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
        return did[len("did:web:"):]
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
    event: str | None = Field(default=None, description="Interaction event type (app.bsky.feed.defs#...)")
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


async def _hydrate_embeddings(
    es, candidates: list[CandidatePost]
) -> list[CandidatePost]:
    """Fetch missing L12 embeddings in a single batched ES call.

    Candidate generators skip the embedding when reading from ES — the
    array is ~4-5 KB per doc and dominates response size for kNN
    searches. We refetch embeddings here, after dedup, against just
    the candidates that survived. The per-request cache means later
    callers (e.g. the two-tower ranker re-asking for the same URIs)
    pay no additional ES cost.
    """
    missing = [
        c.at_uri for c in candidates if c.at_uri and not c.minilm_l12_embedding
    ]
    if not missing:
        return candidates

    try:
        async with timed(logger, "hydrate_embeddings", n_missing=len(missing)):
            pairs = await fetch_post_embeddings(es, missing)
    except Exception:
        # If the refetch fails, MMR falls back to author-only similarity
        # and the two-tower ranker has its own refetch path. Don't fail
        # the request over a hydration hiccup.
        logger.exception("Embedding hydration failed; continuing without")
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


async def _run_ranking_pipeline(
    feed_cfg: FeedConfig,
    gen_request: CandidateGenerateRequest,
    es,
) -> list[str]:
    """Generate candidates, optionally rank them, then diversify with MMR.

    Runs inside a per-request cache scope so that identical ES queries
    issued by different stages (e.g. ``fetch_recent_liked_post_uris`` in
    both ``post_similarity`` and the two-tower ranker) collapse to a
    single round-trip.
    """
    async with request_cache_scope():
        async with timed(
            logger,
            "run_generate",
            num_candidates=gen_request.num_candidates,
            n_generators=len(gen_request.generators),
        ):
            result = await run_generate(gen_request, es, swallow_errors=True)
        candidates = result.candidates

        if not candidates:
            return []

        # Generators fetch lightweight candidates (no embedding); ranker and
        # MMR need embeddings, so backfill in one batched ES call now that
        # the candidate set has been deduped down to the working size.
        candidates = await _hydrate_embeddings(es, candidates)

        if feed_cfg.rank_request_template is not None:
            rank_req = feed_cfg.rank_request_template.model_copy(
                update={"candidates": candidates, "user_did": gen_request.user_did}
            )
            async with timed(
                logger,
                "run_predict",
                n_candidates=len(candidates),
                model=rank_req.model,
            ):
                rank_result = await run_predict(rank_req, es)
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

        final = mmr_rerank(ordered) if feed_cfg.diversify else ordered
        return [c.at_uri for c in final if c.at_uri]


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


async def _seen_exclusions(db, user_did: str) -> list[str]:
    """Fetch the user's recently-seen post URIs to exclude from generation.

    Fail-soft: a Firestore hiccup should degrade the feature (possible repeats)
    rather than break feed serving, so errors are logged and yield an empty list.
    """
    try:
        return await get_recent_seen_uris(db, user_did)
    except Exception:
        logger.exception("Failed to fetch seen posts for user '%s'", user_did)
        return []


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


async def _record_interactions(db, interactions: list["Interaction"]) -> None:
    """Verify each interaction's feedContext and persist the valid ones.

    The signed feedContext is the trust anchor: interactions with a missing or
    forged token are dropped (and logged) rather than written, so the public
    endpoint can't be used to poison the data. Runs as a background task.

    ``interactionSeen`` items are additionally appended to the user's seen-posts
    buckets so they can be excluded from future feed generations.
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

        if event == "interactionSeen" and ix.item:
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

    for did, uris in seen_by_user.items():
        try:
            await record_seen_posts(db, did, uris)
        except Exception:
            logger.exception("Failed to record seen posts for user '%s'", did)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

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

    # Authenticate the requesting user via the AT Protocol inter-service JWT.
    # A valid DID is required for this feed endpoint.
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
                cached_uris = await feed_cache.retrieve(parsed.id)
            if cached_uris is not None:
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
                    return FeedSkeletonResponse(
                        feed=_skeleton_items(page, feed_context),
                        cursor=next_cursor,
                    )

                # Offset is at or past the end — regenerate with exclusions.
                batch = _batch_size(limit)
                seen_uris = await _seen_exclusions(db, user_did)
                # Dedup while preserving order; the cached batch and seen posts
                # can overlap.
                exclude_uris = list(dict.fromkeys(cached_uris + seen_uris))
                gen_request = feed_cfg.gen_request_template.model_copy(
                    update={
                        "user_did": user_did,
                        "num_candidates": batch,
                        "exclude_uris": exclude_uris,
                    }
                )

                new_uris = await _run_ranking_pipeline(feed_cfg, gen_request, request.app.state.es)
                if new_uris:
                    async with timed(logger, "feedcache_append", cache_id=parsed.id):
                        updated = await feed_cache.append(parsed.id, new_uris)
                    if updated is not None:
                        page = new_uris[:limit]
                        next_offset = parsed.offset + len(page)
                        next_cursor = None
                        if len(page) == limit:
                            next_cursor = FeedCursor(id=parsed.id, offset=next_offset).encode()
                        feed_context = _make_feed_context(user_did, feed_name, parsed.id)
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
        batch = _batch_size(limit)
        seen_uris = await _seen_exclusions(db, user_did)
        gen_request = feed_cfg.gen_request_template.model_copy(
            update={"user_did": user_did, "num_candidates": batch, "exclude_uris": seen_uris}
        )

        all_uris = await _run_ranking_pipeline(feed_cfg, gen_request, request.app.state.es)

        # First page to return immediately.
        page = all_uris[:limit]

        # Store the full batch and issue a cursor only when there are more pages.
        # The request id identifies this response; when we cache a batch it doubles
        # as the cache key so the served order can be recovered from interactions.
        next_cursor = None
        if len(all_uris) > limit:
            request_id = uuid.uuid4().hex
            async with timed(logger, "feedcache_store", cache_id=request_id):
                await feed_cache.store(request_id, all_uris)
            next_cursor = FeedCursor(id=request_id, offset=limit).encode()
        else:
            request_id = uuid.uuid4().hex

        feed_context = _make_feed_context(user_did, feed_name, request_id)
        return FeedSkeletonResponse(
            feed=_skeleton_items(page, feed_context),
            cursor=next_cursor,
        )


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

"""XRPC endpoints for AT Protocol Feed Generator.

Implements the two endpoints required by the AT Protocol Feed Generator spec:

  GET /xrpc/app.bsky.feed.describeFeedGenerator
      Declares the feeds this server provides.

  GET /xrpc/app.bsky.feed.getFeedSkeleton
      Returns a feed skeleton (ordered list of AT URIs) for a given feed.

See: https://docs.bsky.app/docs/starter-templates/custom-feeds
"""

import logging
import os
import uuid

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..lib.candidates import run_generate
from ..lib.feed_cache import FeedCache, FirestoreFeedCache, DEFAULT_TTL_SECONDS
from ..models import CandidateGenerateRequest, FeedCursor, GeneratorSpec
from ..lib.atproto_auth import verify_auth_header
from ..lib.firestore import upsert_user
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


class FeedSkeletonResponse(BaseModel):
    """Response for getFeedSkeleton.

    When ``cursor`` is ``None`` it is omitted from the JSON output — the
    AT Protocol spec requires the field to be absent rather than ``null``.
    """
    model_config = {"populate_by_name": True}

    feed: list[SkeletonItem] = Field(default_factory=list)
    cursor: str | None = Field(default=None, description="Pagination cursor")


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

BATCH_MULTIPLIER = 5
MAX_BATCH_SIZE = 200


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

    if collection == "app.bsky.feed.generator" and rkey in FEEDS:
        feed_name = rkey

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

    username = await _resolve_username(request, user_did)

    # Record authenticated users in Firestore for backend analytics/state.
    # Firestore persistence is required and failures are treated as fatal.
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        logger.error("Firestore client not initialized")
        raise HTTPException(status_code=500, detail="Firestore unavailable")

    try:
        await upsert_user(db, user_did, username)
    except Exception:
        logger.exception("Failed to upsert user '%s' in Firestore", user_did)
        raise HTTPException(status_code=500, detail="Firestore write failed")

    feed_cache = _get_feed_cache(request)

    # ------------------------------------------------------------------
    # If the client sent a cursor, try to serve the next page from cache.
    # ------------------------------------------------------------------
    if cursor is not None:
        try:
            parsed = FeedCursor.decode(cursor)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor")

        cached_uris = await feed_cache.retrieve(parsed.id)
        if cached_uris is not None:
            page = cached_uris[parsed.offset : parsed.offset + limit]
            next_offset = parsed.offset + len(page)
            next_cursor: str | None = None
            if next_offset < len(cached_uris) and page:
                next_cursor = FeedCursor(id=parsed.id, offset=next_offset).encode()
            return FeedSkeletonResponse(
                feed=[SkeletonItem(post=uri) for uri in page],
                cursor=next_cursor,
            )
        # Cache miss (expired / evicted) — fall through to generate fresh.

    # ------------------------------------------------------------------
    # No cursor or cache miss — generate a fresh batch.
    # ------------------------------------------------------------------
    batch = _batch_size(limit)
    gen_request = feed_cfg.gen_request_template.model_copy(
        update={"user_did": user_did, "num_candidates": batch}
    )

    result = await run_generate(
        gen_request, request.app.state.es, swallow_errors=True
    )

    all_uris = [c.at_uri for c in result.candidates if c.at_uri]

    # First page to return immediately.
    page = all_uris[:limit]

    # Store the full batch and issue a cursor only when there are more pages.
    next_cursor = None
    if len(all_uris) > limit:
        cache_key = uuid.uuid4().hex
        await feed_cache.store(cache_key, all_uris)
        next_cursor = FeedCursor(id=cache_key, offset=limit).encode()

    return FeedSkeletonResponse(
        feed=[SkeletonItem(post=uri) for uri in page],
        cursor=next_cursor,
    )

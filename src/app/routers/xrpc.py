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

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..lib.candidates import (
    CandidateGenerateRequest,
    GeneratorSpec,
    run_generate,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["xrpc"])


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_service_did() -> str:
    """Return the DID of this feed generator service.

    Set via the ``FEED_GENERATOR_DID`` environment variable.  For local
    development behind ngrok this will be something like
    ``did:web:xxxx-xxx-xxx.ngrok-free.app``.
    """
    return os.environ.get("FEED_GENERATOR_DID", "did:web:localhost")


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

# Each entry maps a short feed name to the generator specs used to produce it.
# The full feed URI is  at://<service_did>/app.bsky.feed.generator/<name>
FEEDS: dict[str, dict] = {
    "greenearth-dev": {
        "display_name": "GreenEarth Dev",
        "description": "Development feed — post-similarity candidates with popularity infill.",
        "primary_generator": "post_similarity",
        "infill_generator": "popularity",
        "default_limit": 30,
    },
}


def _feed_uri(feed_name: str) -> str:
    return f"at://{_get_service_did()}/app.bsky.feed.generator/{feed_name}"


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
    cursor: str | None = Field(default=None, description="Pagination cursor (not yet implemented)")


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

    # Build a CandidateGenerateRequest from the feed config so we share the
    # same generation / infill / dedup pipeline as /candidates/generate.
    gen_request = CandidateGenerateRequest(
        generators=[GeneratorSpec(name=feed_cfg["primary_generator"], weight=1.0)],
        # Feed generators don't receive a user DID from the Bluesky AppView
        # for unauthenticated feeds.  We use a placeholder; post_similarity
        # will fall through to the infill when there are no likes.
        user_did="",
        num_candidates=limit,
        infill=feed_cfg.get("infill_generator"),
        video_only=False,
    )

    result = await run_generate(
        gen_request, request.app.state.es, swallow_errors=True
    )

    skeleton = [
        SkeletonItem(post=c.at_uri)
        for c in result.candidates
        if c.at_uri
    ]

    return FeedSkeletonResponse(feed=skeleton)

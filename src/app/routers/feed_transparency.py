"""Feed-transparency API endpoints.

GET  /api/feeds              — list recent feed snapshots (summary)
GET  /api/feeds/{request_id} — full detail with pipeline metadata + hydrated posts
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Request, status
from google.cloud.firestore import AsyncClient

from ..documents import FeedSnapshotDocument
from ..lib.firebase_auth import FirebaseUser
from ..lib.firestore import (
    get_feed_snapshot,
    get_newer_feed_snapshot_uris,
    get_recent_feed_snapshots,
    get_user,
    set_user_preferences,
)
from ..lib.post_hydration import hydrate_posts
from ..models_feed_transparency import (
    AuthorView,
    DiversificationView,
    EngagementView,
    FeedDetailResponse,
    FeedItemView,
    FeedListResponse,
    FeedSummary,
    GeneratorView,
    MediaView,
    ModelScoreView,
    Preferences,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["feed-transparency"], prefix="/api/feeds")

CACHE_WINDOW_MINUTES = 15
TARGET_FEED_NAME = "your-feed"
DEFAULT_LIST_LIMIT = 20

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _at_uri_to_bsky_url(at_uri: str, handle: str | None = None) -> str | None:
    match = re.match(r"^at://([^/]+)/app\.bsky\.feed\.post/([^/]+)$", at_uri)
    if match is None:
        return None
    did, post_id = match.groups()
    identifier = handle or did
    return f"https://bsky.app/profile/{identifier}/post/{post_id}"


def _build_items(snapshot: FeedSnapshotDocument, hydrated: dict[str, dict]) -> list[FeedItemView]:
    """Build ``FeedItemView`` list from a ``FeedSnapshotDocument`` + hydrated post data.

    ``PipelineItemMeta`` is already per-URI with all pipeline fields joined, so no
    cross-stage merging is needed here.
    """
    items: list[FeedItemView] = []
    for meta in snapshot.items_meta:
        hyd = hydrated.get(meta.at_uri, {})
        author = hyd.get("author", {})
        media = hyd.get("media", {})
        engagement = hyd.get("engagement", {})

        items.append(
            FeedItemView(
                at_uri=meta.at_uri,
                rank=meta.rank,
                rank_score=meta.rank_score,
                after_rank_position=meta.after_rank_position,
                author=AuthorView(
                    handle=author.get("handle"),
                    display_name=author.get("display_name"),
                    avatar_url=author.get("avatar_url"),
                ),
                created_at=hyd.get("created_at"),
                content=hyd.get("content"),
                generators=[GeneratorView(name=g.name, score=g.score) for g in meta.generators],
                model_scores=[
                    ModelScoreView(name=s.name, weight=s.weight, score=s.score)
                    for s in meta.model_scores
                ],
                diversification=DiversificationView(
                    relevance=meta.diversification.relevance,
                    score=meta.diversification.score,
                    author_penalty=meta.diversification.author_penalty,
                    content_penalty=meta.diversification.content_penalty,
                )
                if meta.diversification
                else None,
                media=MediaView(**media) if media else None,
                engagement=EngagementView(**engagement) if engagement else None,
                post_url=_at_uri_to_bsky_url(meta.at_uri, author.get("handle")),
            )
        )
    return items


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=FeedListResponse)
async def list_feeds(
    request: Request,
    user_doc_id: FirebaseUser,
) -> FeedListResponse:
    """Return recent feed snapshots within the cache window."""
    db: AsyncClient = request.app.state.firestore
    cutoff = datetime.now(UTC) - timedelta(minutes=CACHE_WINDOW_MINUTES)

    docs = await get_recent_feed_snapshots(
        db, user_doc_id, feed_name=TARGET_FEED_NAME, cutoff=cutoff, limit=DEFAULT_LIST_LIMIT
    )

    seen_uris: set[str] = set()
    summaries: list[FeedSummary] = []
    for doc in docs:
        if set(doc.items).issubset(seen_uris):
            continue
        seen_uris.update(doc.items)
        summaries.append(
            FeedSummary(
                request_id=doc.request_id,
                generated_at=doc.generated_at,
                feed_name=doc.feed_name,
            )
        )

    return FeedListResponse(feeds=summaries)


# ---------------------------------------------------------------------------
# GET / PUT /api/feeds/preferences  (must precede /{request_id})
# ---------------------------------------------------------------------------


@router.get("/preferences", response_model=Preferences)
async def get_preferences(
    request: Request,
    user_doc_id: FirebaseUser,
) -> Preferences:
    """Return the current preferences for the authenticated user."""
    db: AsyncClient = request.app.state.firestore
    user_doc = await get_user(db, f"did:plc:{user_doc_id}")
    if user_doc is None:
        return Preferences()
    return Preferences(
        social_radius=user_doc.social_radius,
        freshness=user_doc.freshness,
        politics=user_doc.politics,
        purpose=user_doc.purpose,
    )


@router.put("/preferences", response_model=Preferences)
async def put_preferences(
    request: Request,
    body: Preferences,
    user_doc_id: FirebaseUser,
) -> Preferences:
    """Update the preferences for the authenticated user."""
    db: AsyncClient = request.app.state.firestore
    await set_user_preferences(
        db,
        f"did:plc:{user_doc_id}",
        social_radius=body.social_radius,
        freshness=body.freshness,
        politics=body.politics,
        purpose=body.purpose,
    )
    return body


@router.get("/{request_id}", response_model=FeedDetailResponse)
async def get_feed_detail(
    request: Request,
    request_id: str,
    user_doc_id: FirebaseUser,
) -> FeedDetailResponse:
    """Return full feed-debug detail with hydrated post data for one feed load."""
    db: AsyncClient = request.app.state.firestore

    snapshot = await get_feed_snapshot(db, user_doc_id, request_id)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Feed snapshot not found",
        )

    newer_uris = await get_newer_feed_snapshot_uris(
        db,
        user_doc_id,
        feed_name=snapshot.feed_name,
        newer_than=snapshot.generated_at,
    )
    if newer_uris:
        snapshot = snapshot.model_copy(
            update={
                "items": [u for u in snapshot.items if u not in newer_uris],
                "items_meta": [m for m in snapshot.items_meta if m.at_uri not in newer_uris],
            }
        )

    hydrated = await hydrate_posts(db, snapshot.items)
    items = _build_items(snapshot, hydrated)

    return FeedDetailResponse(
        request_id=request_id,
        generated_at=snapshot.generated_at,
        items=items,
    )

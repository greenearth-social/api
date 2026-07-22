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

from ..documents import FeedSnapshotDocument, PipelineItemMeta
from ..lib.firebase_auth import FirebaseUser
from ..lib.firestore import (
    get_feed_snapshot,
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
    GeneratorDiagnosticView,
    MediaView,
    ModelScoreView,
    Preferences,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["feed-transparency"], prefix="/api/feeds")

CACHE_WINDOW_MINUTES = 15
TARGET_FEED_NAME = "your-feed"
DEFAULT_LIST_LIMIT = 20
PUBLIC_MODERATION_LABELS = frozenset(
    {
        "porn",
        "sexual",
        "nudity",
        "graphic-media",
        "graphic_media",
    }
)

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


def _is_publicly_filtered(hydrated_post: dict) -> bool:
    moderation = hydrated_post.get("moderation") or {}
    labels = [
        *moderation.get("post_labels", []),
        *moderation.get("author_labels", []),
    ]
    return any(str(label).strip().lower() in PUBLIC_MODERATION_LABELS for label in labels)


def _has_usable_hydration(hydrated_post: dict) -> bool:
    return bool(hydrated_post) and any(
        (
            hydrated_post.get("content") is not None,
            hydrated_post.get("created_at") is not None,
            (hydrated_post.get("author") or {}).get("handle") is not None,
        )
    )


def _build_items(
    snapshot: FeedSnapshotDocument,
    hydrated: dict[str, dict],
) -> tuple[list[FeedItemView], int, int]:
    """Build ``FeedItemView`` list from a ``FeedSnapshotDocument`` + hydrated post data.

    ``PipelineItemMeta`` is already per-URI with all pipeline fields joined, so no
    cross-stage merging is needed here.
    """
    items: list[FeedItemView] = []
    publicly_filtered_count = 0
    unavailable_count = 0
    meta_by_uri = {meta.at_uri: meta for meta in snapshot.items_meta}
    for at_uri in snapshot.items:
        meta = meta_by_uri.get(at_uri, PipelineItemMeta(at_uri=at_uri))
        hyd = hydrated.get(meta.at_uri, {})
        # The AppView applies user-specific moderation after this generator
        # returns skeleton URIs, so that exact state is not available here.
        # Missing hydration does reliably indicate a deleted or unavailable
        # post, which should remain in the stored audit but not render as a
        # recurring blank observability row.
        if not _has_usable_hydration(hyd):
            unavailable_count += 1
            continue
        if _is_publicly_filtered(hyd):
            publicly_filtered_count += 1
            continue
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
    return items, publicly_filtered_count, unavailable_count


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

    summaries: list[FeedSummary] = []
    for doc in docs:
        summaries.append(
            FeedSummary(
                request_id=doc.request_id,
                generated_at=doc.generated_at,
                feed_name=doc.feed_name,
                applied_social_radius=doc.applied_social_radius,
                generator_diagnostics=[
                    GeneratorDiagnosticView(**diagnostic.model_dump())
                    for diagnostic in doc.generator_diagnostics
                ],
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

    hydrated = await hydrate_posts(db, snapshot.items)
    items, publicly_filtered_count, unavailable_count = _build_items(snapshot, hydrated)

    return FeedDetailResponse(
        request_id=request_id,
        generated_at=snapshot.generated_at,
        items=items,
        stored_item_count=len(snapshot.items),
        displayed_item_count=len(items),
        publicly_filtered_count=publicly_filtered_count,
        unavailable_count=unavailable_count,
    )

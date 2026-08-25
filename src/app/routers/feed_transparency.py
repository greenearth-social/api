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

from ..documents import FeedPreferencesDocument, FeedSnapshotDocument, PipelineItemMeta
from ..feeds import FEEDS, canonical_feed_name
from ..lib.feed_cache import DEFAULT_TTL_SECONDS
from ..lib.feed_preferences import resolve_feed_preferences
from ..lib.firebase_auth import FirebaseUser
from ..lib.firestore import (
    accept_feed_preview,
    delete_most_recent_seen_bucket,
    get_feed_snapshot,
    get_recent_feed_snapshots,
    get_user,
    patch_user_feed_preferences,
)
from ..lib.post_hydration import hydrate_posts
from ..lib.request_context import set_traffic
from ..models_feed_transparency import (
    AcceptedFeedPreviewResponse,
    AcceptFeedPreviewRequest,
    AuthorView,
    DiversificationView,
    EngagementView,
    FeedDetailResponse,
    FeedItemView,
    FeedListResponse,
    FeedPreferences,
    FeedPreviewResponse,
    FeedSummary,
    GeneratorDiagnosticView,
    GeneratorView,
    MediaView,
    ModelScoreView,
    PreferencesResponse,
)
from .xrpc import generate_feed_preview

logger = logging.getLogger(__name__)

router = APIRouter(tags=["feed-transparency"], prefix="/api/feeds")

CACHE_WINDOW_HOURS = 24
DEFAULT_LIST_LIMIT = 100
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
    cutoff = datetime.now(UTC) - timedelta(hours=CACHE_WINDOW_HOURS)

    # Query all recent loads without a feed-name index, then expose only the
    # configured public pages below. In-memory canonicalization also keeps
    # legacy snapshots written with a stage/internal published rkey visible.
    docs = await get_recent_feed_snapshots(db, user_doc_id, cutoff=cutoff, limit=DEFAULT_LIST_LIMIT)

    summaries: list[FeedSummary] = []
    seen_snapshots: set[tuple[str, tuple[str, ...]]] = set()
    for doc in docs:
        resolved_feed_name = canonical_feed_name(doc.feed_name)
        if resolved_feed_name is None or not FEEDS[resolved_feed_name].public:
            continue
        # Treat the complete ordered post sequence as the snapshot identity.
        # Documents are newest-first, so skipping a repeated key retains the
        # newest load while preserving snapshots with different posts or order.
        snapshot_key = (resolved_feed_name, tuple(doc.items))
        if snapshot_key in seen_snapshots:
            continue
        seen_snapshots.add(snapshot_key)

        summaries.append(
            FeedSummary(
                request_id=doc.request_id,
                generated_at=doc.generated_at,
                feed_name=resolved_feed_name,
                api_release_sha=doc.api_release_sha,
                applied_social_radius=doc.applied_social_radius,
                generator_diagnostics=[
                    GeneratorDiagnosticView(**diagnostic.model_dump())
                    for diagnostic in doc.generator_diagnostics
                ],
            )
        )

    return FeedListResponse(feeds=summaries)


# ---------------------------------------------------------------------------
# GET/PATCH /api/feeds/preferences  (must precede /{request_id})
# ---------------------------------------------------------------------------


@router.get(
    "/preferences",
    response_model=PreferencesResponse,
    response_model_exclude_none=True,
)
async def get_preferences(
    request: Request,
    user_doc_id: FirebaseUser,
) -> PreferencesResponse:
    """Return configured controls and values for every public feed."""
    db: AsyncClient = request.app.state.firestore
    user_doc = await get_user(db, f"did:plc:{user_doc_id}")
    return PreferencesResponse(
        feeds={
            feed_name: FeedPreferences.model_validate(
                resolve_feed_preferences(user_doc, feed_name).model_dump(exclude_none=True)
            )
            for feed_name, feed in FEEDS.items()
            if feed.public and feed.controls
        }
    )


@router.patch(
    "/preferences/{feed_name}",
    response_model=FeedPreferences,
    response_model_exclude_none=True,
)
async def patch_preferences(
    request: Request,
    feed_name: str,
    body: FeedPreferences,
    user_doc_id: FirebaseUser,
) -> FeedPreferences:
    """Update only the supplied controls for one configured public feed.

    Also clears today's seen-posts bucket so the user sees fresh candidates
    after adjusting their feed controls. The delete is idempotent.
    """
    feed = FEEDS.get(feed_name)
    if feed is None or not feed.public or not feed.controls:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown feed")

    supplied = body.model_fields_set
    if not supplied:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one control is required",
        )
    if any(getattr(body, control) is None for control in supplied):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Control values cannot be null",
        )
    unsupported = supplied.difference(feed.controls)
    if unsupported:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported controls for {feed_name}: {', '.join(sorted(unsupported))}",
        )

    db: AsyncClient = request.app.state.firestore
    user_did = f"did:plc:{user_doc_id}"
    patch = FeedPreferencesDocument.model_validate(body.model_dump(exclude_none=True))
    updated = await patch_user_feed_preferences(
        db,
        user_did,
        feed_name,
        patch,
    )
    await delete_most_recent_seen_bucket(db, user_did)
    return FeedPreferences.model_validate(updated.model_dump(exclude_none=True))


@router.post("/{feed_name}/preview", response_model=FeedPreviewResponse)
async def create_feed_preview(
    request: Request,
    feed_name: str,
    body: FeedPreferences,
    user_doc_id: FirebaseUser,
) -> FeedPreviewResponse:
    """Generate a hypothetical feed from unsaved settings and cache it briefly."""
    feed = FEEDS.get(feed_name)
    if feed is None or not feed.public or not feed.controls:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown feed")

    supplied = body.model_fields_set
    if any(getattr(body, control) is None for control in supplied):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Control values cannot be null",
        )
    unsupported = supplied.difference(feed.controls)
    if unsupported:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported controls for {feed_name}: {', '.join(sorted(unsupported))}",
        )

    set_traffic("preview")
    patch = FeedPreferencesDocument.model_validate(body.model_dump(exclude_none=True))
    snapshot = await generate_feed_preview(
        request,
        f"did:plc:{user_doc_id}",
        feed_name,
        patch,
    )
    return FeedPreviewResponse(
        request_id=snapshot.request_id,
        feed_name=feed_name,
        generated_at=snapshot.generated_at,
        expires_at=snapshot.expires_at,
    )


@router.get("/previews/{request_id}", response_model=FeedDetailResponse)
async def get_feed_preview(
    request: Request,
    request_id: str,
    user_doc_id: FirebaseUser,
) -> FeedDetailResponse:
    """Hydrate an owned, unexpired settings-preview cache entry."""
    cache = getattr(request.app.state, "feed_cache", None)
    if cache is None:
        raise HTTPException(status_code=500, detail="Feed cache unavailable")

    cache_doc = await cache.retrieve_document(request_id)
    user_did = f"did:plc:{user_doc_id}"
    if (
        cache_doc is None
        or cache_doc.mode != "preview"
        or cache_doc.user_did != user_did
        or cache_doc.feed_name is None
        or cache_doc.generated_at is None
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Feed preview not found")

    snapshot = FeedSnapshotDocument(
        request_id=request_id,
        items=cache_doc.items,
        feed_name=cache_doc.feed_name,
        generated_at=cache_doc.generated_at,
        api_release_sha=cache_doc.api_release_sha,
        expires_at=cache_doc.expires_at,
        generator_diagnostics=cache_doc.generator_diagnostics,
        applied_social_radius=cache_doc.applied_social_radius,
        items_meta=cache_doc.items_meta,
    )
    db: AsyncClient = request.app.state.firestore
    hydrated = await hydrate_posts(db, snapshot.items)
    items, publicly_filtered_count, unavailable_count = _build_items(snapshot, hydrated)
    return FeedDetailResponse(
        request_id=request_id,
        generated_at=snapshot.generated_at,
        api_release_sha=snapshot.api_release_sha,
        items=items,
        stored_item_count=len(snapshot.items),
        displayed_item_count=len(items),
        publicly_filtered_count=publicly_filtered_count,
        unavailable_count=unavailable_count,
    )


@router.post(
    "/{feed_name}/previews/{request_id}/accept",
    response_model=AcceptedFeedPreviewResponse,
    response_model_exclude_none=True,
)
async def accept_preview(
    request: Request,
    feed_name: str,
    request_id: str,
    body: AcceptFeedPreviewRequest,
    user_doc_id: FirebaseUser,
) -> AcceptedFeedPreviewResponse:
    """Persist a preview's settings and stage its visible slate for one feed session."""

    feed = FEEDS.get(feed_name)
    if feed is None or not feed.public or not feed.controls:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown feed")

    supplied = body.preferences.model_fields_set
    if not supplied:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one control is required",
        )
    if any(getattr(body.preferences, control) is None for control in supplied):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Control values cannot be null",
        )
    unsupported = supplied.difference(feed.controls)
    if unsupported:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported controls for {feed_name}: {', '.join(sorted(unsupported))}",
        )

    cache = getattr(request.app.state, "feed_cache", None)
    if cache is None:
        raise HTTPException(status_code=500, detail="Feed cache unavailable")

    user_did = f"did:plc:{user_doc_id}"
    cache_doc = await cache.retrieve_document(request_id)
    if (
        cache_doc is None
        or cache_doc.mode != "preview"
        or cache_doc.user_did != user_did
        or cache_doc.feed_name != feed_name
        or cache_doc.generated_at is None
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Feed preview not found")

    patch = FeedPreferencesDocument.model_validate(body.preferences.model_dump(exclude_none=True))
    cached_patch = (
        cache_doc.preference_patch.model_dump(exclude_none=True)
        if cache_doc.preference_patch is not None
        else None
    )
    if cached_patch != patch.model_dump(exclude_none=True):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Feed preview does not match these settings",
        )

    snapshot = FeedSnapshotDocument(
        request_id=request_id,
        items=cache_doc.items,
        feed_name=feed_name,
        generated_at=cache_doc.generated_at,
        api_release_sha=cache_doc.api_release_sha,
        expires_at=cache_doc.expires_at,
        generator_diagnostics=cache_doc.generator_diagnostics,
        applied_social_radius=cache_doc.applied_social_radius,
        items_meta=cache_doc.items_meta,
    )
    db: AsyncClient = request.app.state.firestore
    hydrated = await hydrate_posts(db, snapshot.items)
    visible_items, _, _ = _build_items(snapshot, hydrated)
    visible_uris = [item.at_uri for item in visible_items]
    if body.displayed_item_uris != visible_uris:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Feed preview contents changed; generate it again",
        )

    accepted = await accept_feed_preview(
        db,
        user_did,
        feed_name,
        request_id,
        patch,
        visible_uris,
        ttl_seconds=DEFAULT_TTL_SECONDS,
    )
    if accepted is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Feed preview not found")
    preferences, accepted_until = accepted
    return AcceptedFeedPreviewResponse(
        request_id=request_id,
        preferences=FeedPreferences.model_validate(preferences.model_dump(exclude_none=True)),
        accepted_until=accepted_until,
    )


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
        api_release_sha=snapshot.api_release_sha,
        items=items,
        stored_item_count=len(snapshot.items),
        displayed_item_count=len(items),
        publicly_filtered_count=publicly_filtered_count,
        unavailable_count=unavailable_count,
    )

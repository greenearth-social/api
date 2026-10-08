"""Slate router – the whole ranking pipeline in one call.

POST /slate/generate
    Generate, rank, diversify and trim a slate for a user from a request-body
    configuration, with cursor pagination over the result.

Partners who host part of the pipeline themselves (typically candidate
generation) previously chained ``/candidates/generate`` → ``/rank/predict`` →
``/diversify``, paying a network round-trip per stage. This endpoint is the
same pipeline that serves our own Bluesky feeds, configured per request
instead of from ``feeds.py``, in one call (issue #542).
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..documents import FeedCacheDocument, GeneratorDiagnostic, PipelineItemMeta
from ..lib.candidates import get_generator
from ..lib.candidates.supplied import SuppliedCandidatesGenerator
from ..lib.feed_cache import DEFAULT_TTL_SECONDS
from ..lib.pipeline import record_render_metrics, run_pipeline_capturing_with_timeout
from ..lib.posthog_client import get_posthog_client, track_slate_generated
from ..lib.rankers import get_ranker
from ..lib.request_context import set_traffic
from ..lib.telemetry import timed
from ..models import (
    EXTERNAL_GENERATOR_NAME,
    CandidateGenerateRequest,
    FeedCursor,
    RankPredictRequest,
    SlateConfig,
    SlateGenerateRequest,
)
from ..security import RequireApiKey
from .xrpc import (
    _get_feed_cache,
    _make_feed_context,
    _record_discarded,
    _spawn_background,
    user_exclusions,
)

router = APIRouter(tags=["slate"])

logger = logging.getLogger(__name__)

CURSOR_EXPIRED = "cursor_expired"
CURSOR_EXHAUSTED = "cursor_exhausted"


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SlateItem(PipelineItemMeta):
    """One ranked post with its pipeline metadata and interaction token."""

    feed_context: str = Field(
        ...,
        description="Signed token identifying the user, feed and request this item was "
        "served in. Pass it back with any interaction reported for the item, exactly "
        "like `feedContext` on a Bluesky feed skeleton.",
    )


class DegradationMeta(BaseModel):
    """A pipeline component that failed and was skipped during this run."""

    stage: str = Field(..., description="Pipeline stage: candidate_gen, embed_hydration, rank…")
    component: str = Field(..., description="Generator, ranker or helper that failed")


class SlateGenerateResponse(BaseModel):
    """One page of a generated slate."""

    request_id: str = Field(
        ..., description="Identifies the generation run; the cursor session and feed_context "
        "tokens are keyed on it."
    )
    feed_name: str
    items: list[SlateItem] = Field(default_factory=list, description="Posts in slate order")
    cursor: str | None = Field(
        None,
        description="Cursor for the next page, or null when this is the last page of the "
        "session. Start a new session (a request without a cursor) to get more posts.",
    )
    generator_diagnostics: list[GeneratorDiagnostic] = Field(
        default_factory=list,
        description="Per-generator outcome for the run: what was requested, returned, and "
        "how many of this page's items each generator contributed.",
    )
    degraded: list[DegradationMeta] = Field(
        default_factory=list,
        description="Components that failed during generation. The slate was still built "
        "from whatever succeeded; empty on a clean run.",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _page_items(
    items_meta: list[PipelineItemMeta], uris: list[str], feed_context: str
) -> list[SlateItem]:
    meta_by_uri = {meta.at_uri: meta for meta in items_meta}
    return [
        SlateItem(
            **meta_by_uri.get(uri, PipelineItemMeta(at_uri=uri)).model_dump(),
            feed_context=feed_context,
        )
        for uri in uris
    ]


def _page_diagnostics(
    diagnostics: list[GeneratorDiagnostic], items: list[SlateItem]
) -> list[GeneratorDiagnostic]:
    """Restrict ``contributed_count`` to the items on this page."""
    return [
        diagnostic.model_copy(
            update={
                "contributed_count": sum(
                    1
                    for item in items
                    if any(g.name == diagnostic.name for g in item.generators)
                )
            }
        )
        for diagnostic in diagnostics
    ]


def _validate_component_names(body: SlateGenerateRequest) -> None:
    """Reject unknown generator/ranker names up front.

    Inside the pipeline an unknown ranker degrades to an unranked slate rather
    than failing, which is right for a published feed and wrong for a caller
    who mistyped a name.
    """
    for spec in body.generators:
        if spec.name != EXTERNAL_GENERATOR_NAME and get_generator(spec.name) is None:
            raise HTTPException(status_code=404, detail=f"Unknown generator: '{spec.name}'")
    if (
        body.infill is not None
        and body.infill != EXTERNAL_GENERATOR_NAME
        and get_generator(body.infill) is None
    ):
        raise HTTPException(status_code=404, detail=f"Unknown infill generator: '{body.infill}'")
    for model in body.rankers or []:
        if get_ranker(model.name) is None:
            raise HTTPException(status_code=404, detail=f"Unknown ranker: '{model.name}'")


def _gone(code: str, message: str) -> HTTPException:
    return HTTPException(status_code=410, detail={"code": code, "message": message})


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post(
    "/slate/generate",
    response_model=SlateGenerateResponse,
    responses={
        400: {"description": "Invalid cursor (malformed, or issued to another key, user or feed)"},
        404: {"description": "A named generator or ranker was not found"},
        410: {
            "description": "The cursor session is over: `detail.code` is `cursor_expired` "
            "(the session timed out) or `cursor_exhausted` (every item was served). Start "
            "a new session by sending the request again without a cursor."
        },
        504: {"description": "Generation exceeded the server's internal deadline"},
    },
)
@record_render_metrics(lambda kwargs: kwargs["body"].feed_name)  # type: ignore[arg-type]
async def slate_generate(
    request: Request,
    body: SlateGenerateRequest,
    key_id: RequireApiKey,
) -> SlateGenerateResponse:
    """Run the full ranking pipeline for a user and return the first page.

    **Generation.** Each entry in `generators` names a registered generator
    (see `/candidates/generators`) and its share of `num_candidates`. Name
    `external` to mix in the posts you retrieved yourself, listed in
    `external_candidates`: only the URI and an optional score are taken from
    you — text, author, embeddings and topic scores are loaded from our index,
    and a URI we don't have is dropped (see `generator_diagnostics`). Posts the
    user has already been shown through us, and posts previously discarded for
    a low rank score, are excluded on top of your `exclude_uris`.

    **Ranking and the slate.** `rankers` are combined exactly as in
    `/rank/predict`; omit them for a slate ordered by generator score. Then
    `min_rank_score` trims the tail, MMR diversification runs unless
    `diversify` is false, `min_mmr_score` cuts the slate at the first weak pick,
    and `max_render_share` caps how much of what was retrieved is served.

    **Paging.** The whole slate is generated once and cached for ten minutes;
    `limit` items come back per page with a `cursor` for the next. A cursor
    request carries only `feed_name`, `user_did`, `limit` and `cursor` — no
    pipeline configuration, since nothing is regenerated. The last page has
    `cursor: null`; a cursor used after that, or after the session expired,
    gets **410** with `detail.code` set, which means: start a new session.

    **Interactions.** Every item carries `feed_context`, the token to report
    back with interactions for that item.

    `feed_name` is yours to choose. It labels this feed in our metrics and
    analytics and is bound into the cursor session and `feed_context` tokens.
    """
    set_traffic("real")

    db = getattr(request.app.state, "firestore", None)
    if db is None:
        logger.error("Firestore client not initialized")
        raise HTTPException(status_code=500, detail="Firestore unavailable")
    feed_cache = _get_feed_cache(request)

    async with timed(
        logger,
        "feed.render.duration_ms",
        record_metric=True,
        metric_attrs={"feed_name": body.feed_name},
    ):
        if body.cursor is not None:
            response = await _serve_cursor_page(body, key_id, feed_cache)
        else:
            response = await _generate_session(request, body, key_id, db, feed_cache)

    try:
        track_slate_generated(
            get_posthog_client(),
            user_did=body.user_did,
            feed_name=body.feed_name,
            api_key_id=key_id,
            requested_limit=body.limit,
            has_cursor=body.cursor is not None,
            generators=[spec.name for spec in body.generators],
            rankers=[model.name for model in body.rankers] if body.rankers is not None else None,
            external_candidate_count=len(body.external_candidates),
            item_count=len(response.items),
            timestamp=datetime.now(UTC),
        )
    except Exception:
        logger.exception("Failed to track slateGenerated for feed '%s'", body.feed_name)

    return response


async def _serve_cursor_page(
    body: SlateGenerateRequest, key_id: str, feed_cache
) -> SlateGenerateResponse:
    assert body.cursor is not None
    try:
        parsed = FeedCursor.decode(body.cursor)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    cache_doc = await feed_cache.retrieve_document(parsed.id)
    if cache_doc is None:
        raise _gone(CURSOR_EXPIRED, "This slate session has expired; request a new slate")
    if (
        cache_doc.mode != "api"
        or cache_doc.api_key_id != key_id
        or cache_doc.user_did != body.user_did
        or cache_doc.feed_name != body.feed_name
    ):
        # Don't say which check failed: the cursor was minted for someone else.
        logger.warning(
            "Rejected slate cursor outside its originating key/user/feed context",
            extra={"request_id": parsed.id, "feed_name": body.feed_name},
        )
        raise HTTPException(status_code=400, detail="Invalid cursor")
    if parsed.offset >= len(cache_doc.items):
        raise _gone(
            CURSOR_EXHAUSTED, "Every item in this slate has been served; request a new slate"
        )

    page = cache_doc.items[parsed.offset : parsed.offset + body.limit]
    next_offset = parsed.offset + len(page)
    next_cursor = (
        FeedCursor(id=parsed.id, offset=next_offset).encode()
        if next_offset < len(cache_doc.items)
        else None
    )
    items = _page_items(
        cache_doc.items_meta, page, _make_feed_context(body.user_did, body.feed_name, parsed.id)
    )
    return SlateGenerateResponse(
        request_id=parsed.id,
        feed_name=body.feed_name,
        items=items,
        cursor=next_cursor,
        generator_diagnostics=_page_diagnostics(cache_doc.generator_diagnostics, items),
    )


async def _generate_session(
    request: Request, body: SlateGenerateRequest, key_id: str, db, feed_cache
) -> SlateGenerateResponse:
    _validate_component_names(body)

    excluded = await user_exclusions(
        db, body.user_did, include_seen=True, include_discarded=True
    )
    gen_request = CandidateGenerateRequest(
        generators=body.generators,
        user_did=body.user_did,
        num_candidates=body.num_candidates,
        video_only=body.video_only,
        max_age_hours=body.max_age_hours,
        exclude_uris=list(dict.fromkeys([*body.exclude_uris, *excluded])),
        infill=body.infill,
    )
    rank_template = (
        RankPredictRequest(
            candidates=[],
            models=body.rankers,
            user_did=body.user_did,
            politics=body.politics,
        )
        if body.rankers is not None
        else None
    )
    slate = SlateConfig(
        rank_request_template=rank_template,
        diversify=body.diversify,
        min_rank_score=body.min_rank_score,
        min_mmr_score=body.min_mmr_score,
        max_render_share=body.max_render_share,
    )
    extra_generators = (
        {EXTERNAL_GENERATOR_NAME: SuppliedCandidatesGenerator(body.external_candidates)}
        if body.external_candidates
        else None
    )

    # Identifies this run: the cache key for the cursor session and the rid in
    # every feed_context token, so served order can be recovered later.
    request_id = uuid.uuid4().hex
    capture = await run_pipeline_capturing_with_timeout(
        request.app.state.es,
        slate,
        gen_request,
        feed_name=body.feed_name,
        request_id=request_id,
        regenerated=False,
        extra_generators=extra_generators,
    )
    if capture.low_score_uris:
        _spawn_background(_record_discarded(db, body.user_did, capture.low_score_uris))

    snapshot = capture.snapshot
    all_uris = snapshot.items
    page = all_uris[: body.limit]
    next_cursor: str | None = None
    if all_uris:
        await feed_cache.store_document(
            request_id,
            FeedCacheDocument(
                items=all_uris,
                items_meta=snapshot.items_meta,
                generator_diagnostics=snapshot.generator_diagnostics,
                user_did=body.user_did,
                feed_name=body.feed_name,
                generated_at=snapshot.generated_at,
                api_release_sha=snapshot.api_release_sha,
                expires_at=datetime.now(UTC) + timedelta(seconds=DEFAULT_TTL_SECONDS),
                mode="api",
                api_key_id=key_id,
            ),
        )
        if len(page) < len(all_uris):
            next_cursor = FeedCursor(id=request_id, offset=len(page)).encode()

    items = _page_items(
        snapshot.items_meta, page, _make_feed_context(body.user_did, body.feed_name, request_id)
    )
    return SlateGenerateResponse(
        request_id=request_id,
        feed_name=body.feed_name,
        items=items,
        cursor=next_cursor,
        generator_diagnostics=_page_diagnostics(snapshot.generator_diagnostics, items),
        degraded=[
            DegradationMeta(stage=event.stage.value, component=event.component)
            for event in capture.degradations
        ],
    )

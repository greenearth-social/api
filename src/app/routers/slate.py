"""Slate router – the whole ranking pipeline in one call.

POST /slate/generate
    Generate, rank, diversify and trim a slate for a user from a request-body
    configuration, with cursor pagination over the result.

    With ``preview: true``, generate a settings preview instead: no side
    effects, the user's last feed load when the settings match it, and the
    preview is what the next feed load with those settings is served.

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
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ..documents import (
    FeedCacheDocument,
    GeneratorDiagnostic,
    PipelineItemMeta,
    StoredSlateDocument,
)
from ..lib.candidates import get_generator
from ..lib.candidates.supplied import SuppliedCandidatesGenerator
from ..lib.feed_cache import DEFAULT_TTL_SECONDS
from ..lib.firestore import (
    claim_api_preview,
    delete_api_slate,
    get_api_slate,
    save_api_slate,
)
from ..lib.metrics import get_metric_collector
from ..lib.posthog_client import get_posthog_client, track_slate_generated
from ..lib.rankers import get_ranker
from ..lib.request_context import set_traffic
from ..lib.slate_pipeline import record_render_metrics, run_pipeline_capturing_with_timeout
from ..lib.telemetry import timed
from ..models import (
    EXTERNAL_GENERATOR_NAME,
    CandidateGenerateRequest,
    FeedCursor,
    RankPredictRequest,
    SlateConfig,
    SlateGenerateRequest,
)
from ..security import require_api_scope
from .xrpc import (
    _get_feed_cache,
    _make_feed_context,
    _record_discarded,
    _spawn_background,
    user_exclusions,
)

router = APIRouter(tags=["slate"])

# A slate is generated on behalf of a named user: it reads their seen and
# discarded history and writes discards and their last load and preview. That
# is the same access as the user-preferences service endpoints, so it needs the
# same explicitly granted scope, not the default every key has.
require_user_config_scope = require_api_scope("user-config")
UserConfigApiKey = Annotated[str, Depends(require_user_config_scope)]

logger = logging.getLogger(__name__)

CURSOR_EXPIRED = "cursor_expired"
CURSOR_EXHAUSTED = "cursor_exhausted"

# How the session behind a response was created.
ServedFrom = Literal["generated", "last_load", "preview"]


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
    served_from: ServedFrom = Field(
        default="generated",
        description="How this slate was produced. `generated`: the pipeline ran for this "
        "request. `last_load` (previews only): the settings match the user's last feed "
        "load, so that load is shown. `preview` (feed loads only): the settings match the "
        "user's most recent preview, so the feed opens on that preview. Cursor pages "
        "report how their session started.",
    )
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


def _owned_session(
    cache_doc: FeedCacheDocument | None, key_id: str, user_did: str, feed_name: str
) -> bool:
    """Whether a stored session was created through this API for this key, user and feed."""
    return (
        cache_doc is not None
        and cache_doc.mode in {"api", "preview"}
        and cache_doc.api_key_id == key_id
        and cache_doc.user_did == user_did
        and cache_doc.feed_name == feed_name
    )


def _session_page(
    body: SlateGenerateRequest,
    request_id: str,
    cache_doc: FeedCacheDocument,
    offset: int,
) -> SlateGenerateResponse:
    """One page of a stored session, with a cursor while items remain."""
    page = cache_doc.items[offset : offset + body.limit]
    next_offset = offset + len(page)
    next_cursor = (
        FeedCursor(id=request_id, offset=next_offset).encode()
        if next_offset < len(cache_doc.items)
        else None
    )
    items = _page_items(
        cache_doc.items_meta, page, _make_feed_context(body.user_did, body.feed_name, request_id)
    )
    return SlateGenerateResponse(
        request_id=request_id,
        feed_name=body.feed_name,
        served_from=cache_doc.served_from or "generated",
        items=items,
        cursor=next_cursor,
        generator_diagnostics=_page_diagnostics(cache_doc.generator_diagnostics, items),
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post(
    "/slate/generate",
    response_model=SlateGenerateResponse,
    responses={
        400: {"description": "Invalid cursor (malformed, or issued to another key, user or feed)"},
        401: {"description": "Missing or invalid API key"},
        403: {"description": "API key lacks the `user-config` scope"},
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
    key_id: UserConfigApiKey,
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

    **Settings previews.** Set `preview: true` while the user edits their
    settings. A preview changes nothing about the user's history: no
    discards are recorded, and posts they've already seen stay eligible. If the
    preview's settings match the user's last feed load, that load is shown
    (`served_from: "last_load"`), so opening settings shows the feed as it is.
    Otherwise a slate is generated and kept as the most recent preview. The
    user's next feed load whose settings match it opens on exactly that
    preview (`served_from: "preview"`), once; with any other settings it
    generates as usual. "Settings" means the configuration minus
    `external_candidates`, `exclude_uris`, `limit`, `cursor` and `preview`.

    **Interactions.** Every item carries `feed_context`, the token to report
    back with interactions for that item.

    **Authentication.** Requires an API key granted the `user-config` scope:
    the slate is generated on behalf of `user_did`, using and updating their
    history.

    `feed_name` is yours to choose. It labels this feed in our metrics and
    analytics and is bound into the cursor session and `feed_context` tokens.
    """
    set_traffic("preview" if body.preview else "real")

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
        elif body.preview:
            response = await _preview(request, body, key_id, db, feed_cache)
        else:
            response = await _feed_load(request, body, key_id, db, feed_cache)

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
            served_from=response.served_from,
            preview=body.preview,
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
    if not _owned_session(cache_doc, key_id, body.user_did, body.feed_name):
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

    return _session_page(body, parsed.id, cache_doc, parsed.offset)


async def _feed_load(
    request: Request, body: SlateGenerateRequest, key_id: str, db, feed_cache
) -> SlateGenerateResponse:
    """A fresh feed load: the user's last preview if its settings match, else generate."""
    _validate_component_names(body)
    fingerprint = body.settings_fingerprint()

    served = await _serve_saved_preview(body, key_id, db, feed_cache, fingerprint)
    if served is not None:
        response, cache_doc = served
    else:
        response, cache_doc = await _generate_and_store(
            request, body, key_id, db, feed_cache, fingerprint, preview=False
        )
    # Remembered even when empty: a preview of unchanged settings must show the
    # feed the user actually has, not an older non-empty load.
    # Off the load's latency: only the next preview reads it.
    _spawn_background(
        _remember_last_load(body, key_id, db, response.request_id, fingerprint, cache_doc)
    )
    return response


async def _preview(
    request: Request, body: SlateGenerateRequest, key_id: str, db, feed_cache
) -> SlateGenerateResponse:
    """A settings preview: the last feed load if the settings match, else generate."""
    _validate_component_names(body)
    fingerprint = body.settings_fingerprint()

    try:
        last_load = await get_api_slate(
            db, body.user_did, api_key_id=key_id, feed_name=body.feed_name, kind="last_load"
        )
    except Exception:
        logger.exception("Failed to read last feed load for feed '%s'", body.feed_name)
        last_load = None

    if last_load is not None and last_load.settings_fingerprint == fingerprint:
        # Unchanged settings: show the feed as the user last saw it. Nothing is
        # kept as a preview, so if they leave settings without changing
        # anything their next feed load generates as usual.
        await _forget_preview(body, key_id, db)
        request_id = uuid.uuid4().hex
        cache_doc = last_load.slate.model_copy(
            update={
                "mode": "preview",
                "served_from": "last_load",
                "expires_at": datetime.now(UTC) + timedelta(seconds=DEFAULT_TTL_SECONDS),
            }
        )
        if cache_doc.items:
            await feed_cache.store_document(request_id, cache_doc)
        return _session_page(body, request_id, cache_doc, 0)

    response, cache_doc = await _generate_and_store(
        request, body, key_id, db, feed_cache, fingerprint, preview=True
    )
    if not cache_doc.items:
        # An empty preview is not worth handing to the feed: let it generate.
        await _forget_preview(body, key_id, db)
    else:
        try:
            await _remember(
                body, key_id, db, "preview", response.request_id, fingerprint, cache_doc
            )
        except Exception:
            logger.exception("Failed to save preview for feed '%s'", body.feed_name)
    return response


async def _serve_saved_preview(
    body: SlateGenerateRequest, key_id: str, db, feed_cache, fingerprint: str
) -> tuple[SlateGenerateResponse, FeedCacheDocument] | None:
    """Serve the user's most recent preview if its settings match this load's.

    Fail-soft: a Firestore error means the load generates, as it would with no
    preview saved.
    """
    request_id: str | None
    outcome: str
    try:
        request_id, outcome = await claim_api_preview(
            db,
            body.user_did,
            api_key_id=key_id,
            feed_name=body.feed_name,
            settings_fingerprint=fingerprint,
            cache_ttl_seconds=DEFAULT_TTL_SECONDS,
        )
    except Exception:
        logger.exception("Failed to claim preview for feed '%s'", body.feed_name)
        request_id, outcome = None, "error"

    cache_doc = None
    if request_id is not None:
        cache_doc = await feed_cache.retrieve_document(request_id)
        if not _owned_session(cache_doc, key_id, body.user_did, body.feed_name):
            cache_doc, outcome = None, "missing"

    if outcome != "none" and (collector := get_metric_collector()):
        collector.record(
            "feed.slate_preview.claim_count", 1, feed_name=body.feed_name, outcome=outcome
        )
    if cache_doc is None or request_id is None:
        return None
    return _session_page(body, request_id, cache_doc, 0), cache_doc


async def _generate_and_store(
    request: Request,
    body: SlateGenerateRequest,
    key_id: str,
    db,
    feed_cache,
    fingerprint: str,
    *,
    preview: bool,
) -> tuple[SlateGenerateResponse, FeedCacheDocument]:
    """Run the pipeline and store the result as a paging session.

    A preview mirrors our own Settings preview: posts the user has seen stay
    eligible (only low-score discards are excluded), and nothing is recorded.
    Returns the response and the session document. An empty slate's document
    is not stored for paging, but is still returned so a feed load can be
    remembered as empty.
    """
    excluded = await user_exclusions(
        db, body.user_did, include_seen=not preview, include_discarded=True
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
        author_penalty_setting=body.author_penalty,
        topic_penalty_setting=body.topic_penalty,
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
    if capture.low_score_uris and not preview:
        _spawn_background(_record_discarded(db, body.user_did, capture.low_score_uris))

    snapshot = capture.snapshot
    cache_doc = FeedCacheDocument(
        items=snapshot.items,
        items_meta=snapshot.items_meta,
        generator_diagnostics=snapshot.generator_diagnostics,
        user_did=body.user_did,
        feed_name=body.feed_name,
        generated_at=snapshot.generated_at,
        api_release_sha=snapshot.api_release_sha,
        expires_at=datetime.now(UTC) + timedelta(seconds=DEFAULT_TTL_SECONDS),
        mode="preview" if preview else "api",
        api_key_id=key_id,
        served_from="generated",
        preference_fingerprint=fingerprint,
    )
    if cache_doc.items:
        # Only a non-empty slate has pages to cursor through.
        await feed_cache.store_document(request_id, cache_doc)
    response = _session_page(body, request_id, cache_doc, 0)
    response.degraded = [
        DegradationMeta(stage=event.stage.value, component=event.component)
        for event in capture.degradations
    ]
    return response, cache_doc


async def _remember(
    body: SlateGenerateRequest,
    key_id: str,
    db,
    kind: Literal["last_load", "preview"],
    request_id: str,
    fingerprint: str,
    cache_doc: FeedCacheDocument,
) -> None:
    """Keep a slate beyond its paging session, as the last load or the preview."""
    await save_api_slate(
        db,
        body.user_did,
        api_key_id=key_id,
        feed_name=body.feed_name,
        kind=kind,
        stored=StoredSlateDocument(
            request_id=request_id,
            settings_fingerprint=fingerprint,
            slate=cache_doc,
            stored_at=datetime.now(UTC),
        ),
    )


async def _remember_last_load(
    body: SlateGenerateRequest,
    key_id: str,
    db,
    request_id: str,
    fingerprint: str,
    cache_doc: FeedCacheDocument,
) -> None:
    """Background: keep this feed load for the next preview. Fail-soft."""
    try:
        await _remember(body, key_id, db, "last_load", request_id, fingerprint, cache_doc)
    except Exception:
        logger.exception("Failed to save last feed load for feed '%s'", body.feed_name)


async def _forget_preview(body: SlateGenerateRequest, key_id: str, db) -> None:
    try:
        await delete_api_slate(
            db, body.user_did, api_key_id=key_id, feed_name=body.feed_name, kind="preview"
        )
    except Exception:
        logger.exception("Failed to clear preview for feed '%s'", body.feed_name)

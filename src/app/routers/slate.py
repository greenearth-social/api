"""Slate router – the whole ranking pipeline in one call.

POST /slate/generate
    Generate, rank, diversify and trim a slate for a user from a request-body
    configuration, with cursor pagination over the result.

POST /slate/accept
    Stage a slate the user was shown (e.g. while editing settings) to be served
    by the next fresh /slate/generate with the same settings.

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
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..documents import FeedCacheDocument, GeneratorDiagnostic, PipelineItemMeta
from ..lib.candidates import get_generator
from ..lib.candidates.supplied import SuppliedCandidatesGenerator
from ..lib.feed_cache import DEFAULT_TTL_SECONDS
from ..lib.firestore import claim_api_slate, stage_api_slate
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
    SlateAcceptRequest,
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

# How the session behind a response was created.
ServedFrom = Literal["generated", "accepted_slate"]


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
        description="`generated` when this slate was produced by running the pipeline; "
        "`accepted_slate` when it is a slate previously accepted through /slate/accept "
        "(e.g. the last one the user saw in settings), served instead of generating. "
        "Cursor pages report how their session started.",
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


class SlateAcceptResponse(BaseModel):
    """Confirmation that a slate is staged for the user's next fresh load."""

    request_id: str
    accepted_item_count: int = Field(..., description="Items staged, from displayed_item_uris")


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
        and cache_doc.mode in {"api", "accepted"}
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
        served_from="accepted_slate" if cache_doc.mode == "accepted" else "generated",
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

    **Accepted slates.** A fresh request normally runs the pipeline. If a
    slate was staged with `/slate/accept` for this user and `feed_name` with
    the same settings, that slate is served instead, once, and the response
    says `served_from: "accepted_slate"`.

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
            served_from=response.served_from,
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


async def _generate_session(
    request: Request, body: SlateGenerateRequest, key_id: str, db, feed_cache
) -> SlateGenerateResponse:
    _validate_component_names(body)
    fingerprint = body.settings_fingerprint()

    accepted = await _claim_accepted_slate(body, key_id, db, feed_cache, fingerprint)
    if accepted is not None:
        return accepted

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
                preference_fingerprint=fingerprint,
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
        served_from="generated",
        items=items,
        cursor=next_cursor,
        generator_diagnostics=_page_diagnostics(snapshot.generator_diagnostics, items),
        degraded=[
            DegradationMeta(stage=event.stage.value, component=event.component)
            for event in capture.degradations
        ],
    )


async def _claim_accepted_slate(
    body: SlateGenerateRequest, key_id: str, db, feed_cache, fingerprint: str
) -> SlateGenerateResponse | None:
    """Serve the slate staged by /slate/accept, if one matches these settings.

    Fail-soft: a Firestore error means the request generates normally, as it
    would with nothing staged.
    """
    request_id: str | None
    outcome: str
    try:
        request_id, outcome = await claim_api_slate(
            db,
            body.user_did,
            api_key_id=key_id,
            feed_name=body.feed_name,
            settings_fingerprint=fingerprint,
            cache_ttl_seconds=DEFAULT_TTL_SECONDS,
        )
    except Exception:
        logger.exception("Failed to claim accepted slate for feed '%s'", body.feed_name)
        request_id, outcome = None, "error"

    cache_doc = None
    if request_id is not None:
        cache_doc = await feed_cache.retrieve_document(request_id)
        if not _owned_session(cache_doc, key_id, body.user_did, body.feed_name):
            cache_doc, outcome = None, "missing"

    if outcome != "none" and (collector := get_metric_collector()):
        collector.record(
            "feed.accepted_slate.claim_count", 1, feed_name=body.feed_name, outcome=outcome
        )
    if cache_doc is None or request_id is None:
        return None
    return _session_page(body, request_id, cache_doc, 0)


@router.post(
    "/slate/accept",
    response_model=SlateAcceptResponse,
    responses={
        404: {"description": "No such slate session for this key, user and feed (or it expired)"},
        422: {"description": "displayed_item_uris are not items of the slate, in slate order"},
    },
)
async def slate_accept(
    request: Request,
    body: SlateAcceptRequest,
    key_id: RequireApiKey,
) -> SlateAcceptResponse:
    """Stage a slate the user was shown, to be served by their next fresh load.

    Built for a settings UI. As the user edits their configuration, each change
    is a normal `/slate/generate` call that shows the resulting slate. When they
    finish, accept the slate they ended on with the items they were actually
    shown. The next `/slate/generate` without a cursor, for the same user and
    `feed_name`, with the **same settings**, then opens on exactly those items
    instead of generating a new slate. Its response says so with
    `served_from: "accepted_slate"`.

    "Settings" means the request's configuration minus `external_candidates`,
    `exclude_uris`, `limit` and `cursor`. If the next request's settings differ,
    the staged slate is dropped and a slate is generated as usual. A staged
    slate is served once; accepting again replaces it. The slate must still be
    within its ten-minute session.
    """
    set_traffic("real")
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        logger.error("Firestore client not initialized")
        raise HTTPException(status_code=500, detail="Firestore unavailable")
    feed_cache = _get_feed_cache(request)

    cache_doc = await feed_cache.retrieve_document(body.request_id)
    if (
        not _owned_session(cache_doc, key_id, body.user_did, body.feed_name)
        or cache_doc is None
        or cache_doc.preference_fingerprint is None
    ):
        raise HTTPException(status_code=404, detail="Slate session not found")

    displayed = body.displayed_item_uris
    position = 0
    for at_uri in displayed:
        try:
            position = cache_doc.items.index(at_uri, position) + 1
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="displayed_item_uris must be items of this slate, in slate order",
            ) from None

    meta_by_uri = {meta.at_uri: meta for meta in cache_doc.items_meta}
    staged = cache_doc.model_copy(
        update={
            "items": displayed,
            "items_meta": [meta_by_uri[uri] for uri in displayed if uri in meta_by_uri],
            "mode": "accepted",
        }
    )
    await stage_api_slate(
        db,
        body.user_did,
        api_key_id=key_id,
        feed_name=body.feed_name,
        request_id=body.request_id,
        slate=staged,
    )
    return SlateAcceptResponse(
        request_id=body.request_id, accepted_item_count=len(displayed)
    )

from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, urlunparse

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from ..lib.firestore import create_redirect, delete_redirect, get_redirect, update_redirect
from ..lib.posthog_client import get_posthog_client, track_redirect
from ..security import RequireApiKey

router = APIRouter(tags=["redirect"])


def _build_destination(base_url: str, utm_params: dict[str, str]) -> str:
    """Append utm_params to base_url, preserving any existing query params."""
    parsed: ParseResult = urlparse(base_url)
    existing = parse_qs(parsed.query, keep_blank_values=False)
    existing.update({k: [v] for k, v in utm_params.items()})
    new_query = urlencode({k: v[0] for k, v in existing.items()})
    return urlunparse(parsed._replace(query=new_query))


def _extract_utm(
    utm_source: str | None,
    utm_medium: str | None,
    utm_campaign: str | None,
    utm_content: str | None,
    utm_term: str | None,
) -> dict[str, str]:
    return {
        k: v
        for k, v in {
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign,
            "utm_content": utm_content,
            "utm_term": utm_term,
        }.items()
        if v
    }


# ---------------------------------------------------------------------------
# Public redirect endpoints
# ---------------------------------------------------------------------------


@router.get("/r/{slug}", status_code=302, include_in_schema=True)
async def redirect_slug(
    request: Request,
    slug: str,
    utm_source: str | None = Query(None),
    utm_medium: str | None = Query(None),
    utm_campaign: str | None = Query(None),
    utm_content: str | None = Query(None),
    utm_term: str | None = Query(None),
) -> RedirectResponse:
    """302-redirect to the URL registered for *slug* with UTM params appended."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")

    record = await get_redirect(db, slug)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Slug '{slug}' not found")

    utm_params = _extract_utm(utm_source, utm_medium, utm_campaign, utm_content, utm_term)
    destination = _build_destination(record.url, utm_params)
    track_redirect(get_posthog_client(), slug, record.url, utm_params)
    return RedirectResponse(url=destination, status_code=302)


@router.get("/r", status_code=302, include_in_schema=True)
async def redirect_to(
    to: str | None = Query(None, description="Destination URL (must be https://)"),
    utm_source: str | None = Query(None),
    utm_medium: str | None = Query(None),
    utm_campaign: str | None = Query(None),
    utm_content: str | None = Query(None),
    utm_term: str | None = Query(None),
) -> RedirectResponse:
    """302-redirect to *to* with any provided UTM params appended."""
    if not to:
        raise HTTPException(status_code=400, detail="to is required")
    parsed: ParseResult = urlparse(to)
    if parsed.scheme != "https" or not parsed.netloc:
        raise HTTPException(status_code=400, detail="to must be a valid https:// URL")

    utm_params = _extract_utm(utm_source, utm_medium, utm_campaign, utm_content, utm_term)
    destination = _build_destination(to, utm_params)
    track_redirect(get_posthog_client(), "direct", to, utm_params)
    return RedirectResponse(url=destination, status_code=302)


# ---------------------------------------------------------------------------
# Admin CRUD endpoints (require API key)
# ---------------------------------------------------------------------------


class RedirectCreateRequest(BaseModel):
    slug: str
    url: str


class RedirectUpdateRequest(BaseModel):
    url: str


class RedirectResponse_(BaseModel):
    slug: str
    url: str


@router.post("/admin/redirects", status_code=201)
async def admin_create_redirect(
    body: RedirectCreateRequest,
    request: Request,
    _key: RequireApiKey,
) -> RedirectResponse_:
    """Create a new slug → URL mapping."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    parsed = urlparse(body.url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise HTTPException(status_code=400, detail="url must be a valid https:// URL")
    try:
        record = await create_redirect(db, body.slug, body.url)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    return RedirectResponse_(slug=record.slug, url=record.url)


@router.put("/admin/redirects/{slug}", status_code=200)
async def admin_update_redirect(
    slug: str,
    body: RedirectUpdateRequest,
    request: Request,
    _key: RequireApiKey,
) -> RedirectResponse_:
    """Update the destination URL for an existing slug."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    parsed = urlparse(body.url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise HTTPException(status_code=400, detail="url must be a valid https:// URL")
    record = await update_redirect(db, slug, body.url)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Slug '{slug}' not found")
    return RedirectResponse_(slug=record.slug, url=record.url)


@router.delete("/admin/redirects/{slug}", status_code=204)
async def admin_delete_redirect(
    slug: str,
    request: Request,
    _key: RequireApiKey,
) -> None:
    """Delete a slug → URL mapping."""
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    deleted = await delete_redirect(db, slug)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Slug '{slug}' not found")

from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, urlunparse

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse

from ..lib.posthog_client import get_posthog_client, track_redirect

router = APIRouter(tags=["redirect"])

_UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")


@router.get("/r", status_code=302, include_in_schema=True)
async def redirect(
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

    utm_values = {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "utm_content": utm_content,
        "utm_term": utm_term,
    }
    extra = {k: v for k, v in utm_values.items() if v}

    existing = parse_qs(parsed.query, keep_blank_values=False)
    existing.update({k: [v] for k, v in extra.items()})
    new_query = urlencode({k: v[0] for k, v in existing.items()})

    destination = urlunparse(parsed._replace(query=new_query))
    track_redirect(get_posthog_client(), to, extra)
    return RedirectResponse(url=destination, status_code=302)

"""Revoke a user's stored atproto OAuth grant via the private frontend function."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

OAUTH_REVOCATIONS_COLLECTION = "oauth_revocations"
OUTCOMES = frozenset({"revoked", "already_revoked", "no_session", "failed"})
REQUEST_TIMEOUT_SECONDS = 20.0


def _fetch_id_token(audience: str) -> str:
    import google.auth.transport.requests
    import google.oauth2.id_token

    return google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), audience)


async def _call_function(did: str, client: httpx.AsyncClient | None) -> tuple[str, str | None]:
    url = os.environ.get("GE_OAUTH_REVOKE_URL")
    if not url:
        return "failed", "not_configured"
    try:
        headers: dict[str, str] = {}
        if urlparse(url).scheme == "https":
            token = await asyncio.to_thread(_fetch_id_token, url)
            headers["Authorization"] = f"Bearer {token}"
        owns_client = client is None
        http = client or httpx.AsyncClient()
        try:
            response = await http.post(
                url, json={"did": did}, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
            )
        finally:
            if owns_client:
                await http.aclose()
        if response.status_code != 200:
            return "failed", f"status_{response.status_code}"
        outcome = response.json().get("outcome")
        if outcome not in OUTCOMES:
            return "failed", "invalid_response"
        return outcome, None
    except Exception as exc:  # noqa: BLE001
        return "failed", type(exc).__name__


async def revoke_oauth_grant(
    db, did: str, actor: str, *, client: httpx.AsyncClient | None = None
) -> str:
    outcome, error = await _call_function(did, client)
    entry = {
        "did": did,
        "actor": actor,
        "outcome": outcome,
        "error": error,
        "created_at": datetime.now(UTC),
    }
    try:
        await db.collection(OAUTH_REVOCATIONS_COLLECTION).add(entry)
    except Exception:  # noqa: BLE001
        logger.error(
            "oauth revocation audit write failed did=%s actor=%s outcome=%s error=%s at=%s",
            did,
            actor,
            outcome,
            error,
            entry["created_at"].isoformat(),
        )
    return outcome

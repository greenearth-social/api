"""Signed ``feedContext`` tokens for AT Protocol feed interactions.

When serving a feed skeleton we attach a ``feedContext`` string to each item.
Bluesky's AppView echoes it back verbatim on the public
``app.bsky.feed.sendInteractions`` endpoint, which is the only way to attribute
an interaction to a user (the payload carries no DID).  Because that endpoint is
public, the token is **HMAC-signed** so a malicious caller can't forge
interactions and poison the data.

Token format (compact, JWT-like, ~200 chars — well under the 2000 limit)::

    urlsafe_b64(payload_json) "." urlsafe_b64(hmac_sha256(payload_b64, secret))

Both segments have ``=`` padding stripped.  The shared secret comes from
``GE_FEED_CONTEXT_SECRET`` and must be identical across all instances.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class FeedContextPayload(BaseModel):
    """Claims carried in a signed ``feedContext`` token."""

    did: str = Field(..., description="AT Protocol DID of the user the feed was served to")
    feed: str = Field(..., description="Feed rkey the token was issued for")
    rid: str = Field(..., description="Request id (also the feed-cache key) for this response")
    iat: int = Field(..., description="Issued-at time, unix seconds")


def _secret() -> bytes:
    """Return the signing secret, or raise if it is not configured.

    Startup already refuses to boot without ``GE_FEED_CONTEXT_SECRET`` (see the
    lifespan handler in ``main.py``); this is defense in depth.
    """
    secret = os.environ.get("GE_FEED_CONTEXT_SECRET")
    if not secret:
        raise RuntimeError("GE_FEED_CONTEXT_SECRET environment variable is required")
    return secret.encode()


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64decode(segment: str) -> bytes:
    padded = segment + "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(padded.encode())


def _sign(payload_b64: str, secret: bytes) -> str:
    sig = hmac.new(secret, payload_b64.encode(), hashlib.sha256).digest()
    return _b64encode(sig)


def encode_feed_context(payload: FeedContextPayload) -> str:
    """Serialise and sign a payload into a ``feedContext`` token."""
    secret = _secret()
    payload_b64 = _b64encode(payload.model_dump_json().encode())
    return f"{payload_b64}.{_sign(payload_b64, secret)}"


def decode_feed_context(token: str) -> FeedContextPayload | None:
    """Verify and deserialise a token.

    Returns the payload on success, or ``None`` if the token is missing,
    malformed, or its signature does not verify — i.e. any untrusted input that
    should be silently dropped rather than treated as a server error.
    """
    if not token:
        return None
    secret = _secret()
    try:
        payload_b64, sig = token.split(".", 1)
        expected = _sign(payload_b64, secret)
        if not hmac.compare_digest(expected, sig):
            return None
        return FeedContextPayload.model_validate_json(_b64decode(payload_b64))
    except Exception:
        logger.warning("Failed to decode feedContext token")
        return None

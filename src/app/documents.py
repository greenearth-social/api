"""Pydantic models for Firestore documents.

Each model represents a document type in a Firestore collection.  Models
provide validation on read/write and a consistent schema across the codebase.

Convention:
    - Model names end with ``Document`` (e.g. ``UserDocument``).
    - The Firestore collection name is derived by lower-casing the prefix
      and pluralising (e.g. ``UserDocument`` → ``users``).
    - ``created_at`` / ``updated_at`` are present on every document.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UserDocument(BaseModel):
    """A registered feed user.

    The document ID in Firestore is the user's DID (``user_did``).
    """

    user_did: str = Field(..., description="AT Protocol DID of the user (also the document ID)")
    username: str | None = Field(
        default=None,
        description="AT Protocol handle (e.g. foobar.bsky.app)",
    )
    created_at: datetime = Field(default_factory=_utcnow, description="When the user was first seen")
    updated_at: datetime = Field(default_factory=_utcnow, description="Last time the document was modified")
    last_seen_at: datetime = Field(default_factory=_utcnow, description="Most recent feed request from this user")


class FeedCacheDocument(BaseModel):
    """Cached feed result set used by cursor pagination.

    The document ID is an opaque cache key generated per feed request.
    """

    items: list[str] = Field(default_factory=list, description="Cached AT URI list")
    expires_at: datetime = Field(..., description="UTC expiration timestamp for this cache entry")


class FeedActivityDocument(BaseModel):
    feed_name: str = Field(..., description="AT Protocol rkey of the feed (also the document ID)")
    first_seen_at: datetime = Field(default_factory=_utcnow, description="When the user first loaded this feed")
    last_seen_at: datetime = Field(default_factory=_utcnow, description="Most recent time the user loaded this feed")


class ApiKeyDocument(BaseModel):
    """An issued API key stored in the ``api_keys`` collection.

    The document ID in Firestore is ``key_id``.
    The plaintext key is never stored — only the SHA-256 hash.
    """

    key_id: str = Field(..., description="8 hex chars; also the Firestore document ID")
    key_hash: str = Field(..., description="SHA-256(full_key.encode()) as hex")
    email: str = Field(..., description="Owner email address")
    is_active: bool = Field(default=True, description="Whether this API key is valid and usable")
    created_at: datetime = Field(default_factory=_utcnow, description="When the key was created")
    last_used_at: datetime = Field(default_factory=_utcnow, description="Last time this key was used for an API request")
    monthly_call_count: int = Field(default=0, description="Number of API calls made this billing month")
    monthly_period: str = Field(default="", description="YYYY-MM of the current call counters; resets each month")
class InteractionDocument(BaseModel):
    """A single user interaction reported via ``app.bsky.feed.sendInteractions``.

    Stored as an auto-ID document in the top-level ``interactions`` collection —
    one document per interaction event.  ``created_at`` is the server receive
    time and is the anchor for any future TTL/expiry policy.
    """

    user_did: str = Field(..., description="AT Protocol DID of the user (from the signed feedContext)")
    item_uri: str | None = Field(default=None, description="AT URI of the post the interaction relates to")
    event: str = Field(..., description="Interaction event name, e.g. 'interactionLike' (defs# prefix stripped)")
    feed_name: str = Field(..., description="Feed rkey the interaction originated from")
    request_id: str = Field(..., description="Request id of the feed response (also the feed-cache key)")
    feed_generated_at: datetime | None = Field(
        default=None, description="When the feed response was served (from the feedContext iat)"
    )
    created_at: datetime = Field(default_factory=_utcnow, description="When the interaction was received")

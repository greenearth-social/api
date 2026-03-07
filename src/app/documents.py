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

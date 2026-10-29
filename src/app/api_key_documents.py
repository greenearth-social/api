"""Lightweight API-key schemas, re-exported by app.documents.

Kept separate so key administration does not initialize feed generators.
"""

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ApiKeyDocument(BaseModel):
    """An issued API key stored in the ``api_keys`` collection.

    The document ID in Firestore is ``key_id``.
    The plaintext key is never stored — only the SHA-256 hash.
    """

    key_id: str = Field(..., description="8 hex chars; also the Firestore document ID")
    key_hash: str = Field(..., description="SHA-256(full_key.encode()) as hex")
    email: str = Field(..., description="Owner email address")
    is_active: bool = Field(default=True, description="Whether this API key is valid and usable")
    is_admin: bool = Field(
        default=False, description="Whether this key may access /admin/* endpoints"
    )
    created_at: datetime = Field(default_factory=_utcnow, description="When the key was created")
    last_used_at: datetime = Field(
        default_factory=_utcnow, description="Last time this key was used for an API request"
    )
    monthly_call_count: int = Field(
        default=0, description="Number of API calls made this billing month"
    )
    monthly_period: str = Field(
        default="", description="YYYY-MM of the current call counters; resets each month"
    )

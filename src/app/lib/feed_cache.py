"""Feed result cache — stores pre-materialised feed pages for cursor pagination.

The abstract :class:`FeedCache` interface intentionally hides the storage
backend so it can be swapped (e.g. to Redis) without touching callers.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

FEED_CACHE_COLLECTION = "feed_cache"
DEFAULT_TTL_SECONDS = 600  # 10 minutes


class FeedCache(ABC):
    """Backend-agnostic interface for storing and retrieving feed pages."""

    @abstractmethod
    async def store(self, key: str, items: list[str], ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
        """Persist *items* (AT URIs) under *key* with the given TTL."""
        ...

    @abstractmethod
    async def retrieve(self, key: str) -> list[str] | None:
        """Fetch the cached item list for *key*, or ``None`` if missing/expired."""
        ...


class FirestoreFeedCache(FeedCache):
    """Firestore-backed feed cache.

    Each cached result set is stored as a document in the ``feed_cache``
    collection.  An ``expires_at`` timestamp is written alongside the data
    and checked on reads so that stale entries are never returned even if
    Firestore's TTL policy hasn't run yet.
    """

    def __init__(self, db: AsyncClient) -> None:
        self._db = db

    async def store(self, key: str, items: list[str], ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        await (
            self._db.collection(FEED_CACHE_COLLECTION)
            .document(key)
            .set({"items": items, "expires_at": expires_at})
        )

    async def retrieve(self, key: str) -> list[str] | None:
        doc = await self._db.collection(FEED_CACHE_COLLECTION).document(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None

        expires_at = data.get("expires_at")
        if expires_at is not None:
            # Firestore may return a native datetime or a proto timestamp;
            # normalise to offset-aware UTC for comparison.
            if hasattr(expires_at, "timestamp"):
                # Already a datetime-like; ensure it's tz-aware.
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) >= expires_at:
                return None

        return data.get("items")

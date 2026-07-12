"""Feed result cache — stores pre-materialised feed pages for cursor pagination.

The abstract :class:`FeedCache` interface intentionally hides the storage
backend so it can be swapped (e.g. to Redis) without touching callers.

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ..documents import FeedCacheDocument

logger = logging.getLogger(__name__)

FEED_CACHE_COLLECTION = "feed_cache"
DEFAULT_TTL_SECONDS = 600  # 10 minutes


@dataclass
class CachedFeed:
    """Items and optional diversity scores retrieved from the cache."""

    items: list[str]
    diversity_scores: list[float] | None = field(default=None)


class FeedCache(ABC):
    """Backend-agnostic interface for storing and retrieving feed pages."""

    @abstractmethod
    async def store(
        self,
        key: str,
        items: list[str],
        scores: list[float] | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        """Persist *items* (AT URIs) and optional *scores* under *key* with the given TTL."""
        ...

    @abstractmethod
    async def retrieve(self, key: str) -> CachedFeed | None:
        """Fetch the cached feed for *key*, or ``None`` if missing/expired."""
        ...

    @abstractmethod
    async def append(
        self,
        key: str,
        new_items: list[str],
        new_scores: list[float] | None = None,
    ) -> CachedFeed | None:
        """Append *new_items* (and optionally *new_scores*) to an existing entry.

        Returns the full updated :class:`CachedFeed`, or ``None`` if the entry
        is missing or expired.  When either the existing or new scores are
        ``None``, the merged ``diversity_scores`` is also ``None``.
        """
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

    async def store(
        self,
        key: str,
        items: list[str],
        scores: list[float] | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        cache_doc = FeedCacheDocument(items=items, diversity_scores=scores, expires_at=expires_at)
        await (
            self._db.collection(FEED_CACHE_COLLECTION)
            .document(key)
            .set(cache_doc.model_dump())
        )

    async def retrieve(self, key: str) -> CachedFeed | None:
        doc = await self._db.collection(FEED_CACHE_COLLECTION).document(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None

        try:
            cache_doc = FeedCacheDocument.model_validate(data)
        except Exception:
            logger.warning("Invalid feed cache document shape for key=%s", key)
            return None

        expires_at = cache_doc.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None

        return CachedFeed(items=cache_doc.items, diversity_scores=cache_doc.diversity_scores)

    async def append(
        self,
        key: str,
        new_items: list[str],
        new_scores: list[float] | None = None,
    ) -> CachedFeed | None:
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)
        doc = await ref.get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None

        try:
            cache_doc = FeedCacheDocument.model_validate(data)
        except Exception:
            logger.warning("Invalid feed cache document shape for key=%s", key)
            return None

        expires_at = cache_doc.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None

        updated_items = cache_doc.items + new_items
        if cache_doc.diversity_scores is not None and new_scores is not None:
            updated_scores: list[float] | None = cache_doc.diversity_scores + new_scores
        else:
            updated_scores = None

        update_data: dict = {"items": updated_items}
        if updated_scores is not None:
            update_data["diversity_scores"] = updated_scores
        await ref.update(update_data)
        return CachedFeed(items=updated_items, diversity_scores=updated_scores)

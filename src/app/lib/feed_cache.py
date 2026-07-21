"""Feed result cache — stores pre-materialised feed pages for cursor pagination.

The abstract :class:`FeedCache` interface intentionally hides the storage
backend so it can be swapped (e.g. to Redis) without touching callers.

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ..documents import FeedCacheDocument, PipelineItemMeta

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

    @abstractmethod
    async def append(self, key: str, new_items: list[str]) -> list[str] | None:
        """Append *new_items* to an existing cache entry and return the full list.

        Returns ``None`` if the entry is missing or expired.
        """
        ...

    async def retrieve_document(self, key: str) -> FeedCacheDocument | None:
        items = await self.retrieve(key)
        if items is None:
            return None
        return FeedCacheDocument(
            items=items,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_TTL_SECONDS),
        )

    async def store_document(self, key: str, document: FeedCacheDocument) -> None:
        await self.store(key, document.items)

    async def append_document(
        self,
        key: str,
        new_items: list[str],
        new_items_meta: list[PipelineItemMeta],
    ) -> FeedCacheDocument | None:
        items = await self.append(key, new_items)
        if items is None:
            return None
        return FeedCacheDocument(
            items=items,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_TTL_SECONDS),
            items_meta=new_items_meta,
        )


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
        cache_doc = FeedCacheDocument(items=items, expires_at=expires_at)
        await (
            self._db.collection(FEED_CACHE_COLLECTION)
            .document(key)
            .set(cache_doc.model_dump())
        )

    async def retrieve(self, key: str) -> list[str] | None:
        document = await self.retrieve_document(key)
        return document.items if document is not None else None

    async def retrieve_document(self, key: str) -> FeedCacheDocument | None:
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
        # Firestore may return naive datetimes; treat them as UTC.
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None

        return cache_doc

    async def store_document(self, key: str, document: FeedCacheDocument) -> None:
        await self._db.collection(FEED_CACHE_COLLECTION).document(key).set(
            document.model_dump()
        )

    async def append(self, key: str, new_items: list[str]) -> list[str] | None:
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
            return None
        expires_at = cache_doc.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            return None
        updated_items = cache_doc.items + new_items
        await ref.update({"items": updated_items})
        return updated_items

    async def append_document(
        self,
        key: str,
        new_items: list[str],
        new_items_meta: list[PipelineItemMeta],
    ) -> FeedCacheDocument | None:
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

        updated_items = list(dict.fromkeys([*cache_doc.items, *new_items]))
        meta_by_uri = {meta.at_uri: meta for meta in cache_doc.items_meta}
        meta_by_uri.update({meta.at_uri: meta for meta in new_items_meta})
        updated_meta = [meta_by_uri[uri] for uri in updated_items if uri in meta_by_uri]
        updated = cache_doc.model_copy(
            update={"items": updated_items, "items_meta": updated_meta}
        )
        await ref.set(updated.model_dump())
        return updated

"""Feed result cache — stores pre-materialised feed pages for cursor pagination.

The abstract :class:`FeedCache` interface intentionally hides the storage
backend so it can be swapped (e.g. to Redis) without touching callers.

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from google.cloud.firestore import (  # type: ignore[import-untyped]
    AsyncClient,
    async_transactional,
)

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
        *,
        pending_discards: list[str] | None = None,
    ) -> FeedCacheDocument | None:
        items = await self.append(key, new_items)
        if items is None:
            return None
        return FeedCacheDocument(
            items=items,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_TTL_SECONDS),
            items_meta=new_items_meta,
        )

    async def claim_prefetch(self, key: str, *, lease_seconds: float) -> bool:
        """Take the lease on generating *key*'s next batch ahead of the reader.

        Backends without lease support never prefetch.
        """
        return False

    async def release_prefetch(self, key: str) -> None:
        """Give up the prefetch lease without appending a batch."""
        return None

    async def clear_pending_discards(self, key: str) -> None:
        """Forget a prefetched batch's discards once they have been recorded."""
        return None


def _merge_batch(
    cache_doc: FeedCacheDocument,
    new_items: list[str],
    new_items_meta: list[PipelineItemMeta],
    pending_discards: list[str] | None,
) -> FeedCacheDocument:
    """*cache_doc* with a generated batch appended and any prefetch lease released."""
    updated_items = list(dict.fromkeys([*cache_doc.items, *new_items]))
    meta_by_uri = {meta.at_uri: meta for meta in cache_doc.items_meta}
    meta_by_uri.update({meta.at_uri: meta for meta in new_items_meta})
    updated_meta = [meta_by_uri[uri] for uri in updated_items if uri in meta_by_uri]
    update: dict = {"items": updated_items, "items_meta": updated_meta, "prefetch_started_at": None}
    if pending_discards:
        update["pending_discards"] = list(pending_discards)
        update["pending_discards_from"] = len(cache_doc.items)
    return cache_doc.model_copy(update=update)


def _expired(cache_doc: FeedCacheDocument) -> bool:
    expires_at = cache_doc.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= expires_at


def prefetch_lease_held(cache_doc: FeedCacheDocument, lease_seconds: float) -> bool:
    started = cache_doc.prefetch_started_at
    if started is None:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - started).total_seconds() < lease_seconds


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
        *,
        pending_discards: list[str] | None = None,
    ) -> FeedCacheDocument | None:
        """Append a generated batch in a transaction.

        A background prefetch and a foreground regeneration can append to the
        same session; a plain read-modify-write would let one drop the other's
        batch.
        """
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)

        @async_transactional
        async def _append(transaction) -> FeedCacheDocument | None:
            cache_doc = await self._read_live(ref, key, transaction=transaction)
            if cache_doc is None:
                return None
            updated = _merge_batch(cache_doc, new_items, new_items_meta, pending_discards)
            transaction.set(ref, updated.model_dump())
            return updated

        return await _append(self._db.transaction())

    async def claim_prefetch(self, key: str, *, lease_seconds: float) -> bool:
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)

        @async_transactional
        async def _claim(transaction) -> bool:
            cache_doc = await self._read_live(ref, key, transaction=transaction)
            if cache_doc is None or prefetch_lease_held(cache_doc, lease_seconds):
                return False
            transaction.update(ref, {"prefetch_started_at": datetime.now(timezone.utc)})
            return True

        return await _claim(self._db.transaction())

    async def release_prefetch(self, key: str) -> None:
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)
        await ref.update({"prefetch_started_at": None})

    async def clear_pending_discards(self, key: str) -> None:
        ref = self._db.collection(FEED_CACHE_COLLECTION).document(key)
        await ref.update({"pending_discards": [], "pending_discards_from": None})

    async def _read_live(self, ref, key: str, *, transaction=None) -> FeedCacheDocument | None:
        """The unexpired, valid document at *ref*, or ``None``."""
        doc = await ref.get(transaction=transaction)
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
        if _expired(cache_doc):
            return None
        return cache_doc


class InMemoryFeedCache(FeedCache):
    """Dictionary-backed feed cache for tests and local tooling.

    Stores full :class:`FeedCacheDocument` objects (with ``items_meta``) so
    callers exercising similarity/observability data on cursor pages served
    purely from cache can set up realistic fixtures, mirroring
    ``FirestoreFeedCache``'s document-based methods. Entries never expire.
    """

    def __init__(self) -> None:
        self._store: dict[str, list[str]] = {}
        self._docs: dict[str, FeedCacheDocument] = {}

    async def store(
        self, key: str, items: list[str], ttl_seconds: int = DEFAULT_TTL_SECONDS
    ) -> None:
        self._store[key] = items

    async def retrieve(self, key: str) -> list[str] | None:
        return self._store.get(key)

    async def append(self, key: str, new_items: list[str]) -> list[str] | None:
        existing = self._store.get(key)
        if existing is None:
            return None
        updated = existing + new_items
        self._store[key] = updated
        return updated

    async def store_document(self, key: str, document: FeedCacheDocument) -> None:
        self._store[key] = document.items
        self._docs[key] = document

    async def retrieve_document(self, key: str) -> FeedCacheDocument | None:
        return self._docs.get(key)

    async def append_document(
        self,
        key: str,
        new_items: list[str],
        new_items_meta: list[PipelineItemMeta],
        *,
        pending_discards: list[str] | None = None,
    ) -> FeedCacheDocument | None:
        existing = self._docs.get(key)
        if existing is None:
            return None
        updated = _merge_batch(existing, new_items, new_items_meta, pending_discards)
        self._docs[key] = updated
        return updated

    async def claim_prefetch(self, key: str, *, lease_seconds: float) -> bool:
        existing = self._docs.get(key)
        if existing is None or prefetch_lease_held(existing, lease_seconds):
            return False
        self._docs[key] = existing.model_copy(
            update={"prefetch_started_at": datetime.now(timezone.utc)}
        )
        return True

    async def release_prefetch(self, key: str) -> None:
        existing = self._docs.get(key)
        if existing is not None:
            self._docs[key] = existing.model_copy(update={"prefetch_started_at": None})

    async def clear_pending_discards(self, key: str) -> None:
        existing = self._docs.get(key)
        if existing is not None:
            self._docs[key] = existing.model_copy(
                update={"pending_discards": [], "pending_discards_from": None}
            )

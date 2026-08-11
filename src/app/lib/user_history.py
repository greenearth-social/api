"""Shared cached user-history features for recommendation models.

The two-tower user side and heavy ranker consume the same recent-like
history.  This module assembles that history once, stores it in Firestore for
reuse across API instances, and uses the existing request cache to collapse
duplicate work inside a feed render.
"""

from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]
from pydantic import BaseModel, ValidationError

from .elasticsearch import (
    DEFAULT_LIKED_POSTS_LIMIT,
    fetch_post_embeddings_and_metadata,
    fetch_recent_liked_post_uris_and_times,
)
from .embeddings import (
    MINILM_L12_EMBEDDING_KEY,
    decode_float32_b64,
    encode_float32_b64,
)
from .metrics import get_metric_collector
from .request_cache import get_request_cache
from .telemetry import timed

logger = logging.getLogger(__name__)

USER_HISTORY_CACHE_COLLECTION = "user_history_cache"
USER_HISTORY_CACHE_TTL_SECONDS = 600
USER_HISTORY_CACHE_VERSION = 1
USER_HISTORY_LIMIT = DEFAULT_LIKED_POSTS_LIMIT


@dataclass(frozen=True)
class UserHistoryItem:
    """One recent like and its optional hydrated post features."""

    at_uri: str
    liked_at: str
    embedding: list[float] | None
    author_did: str = ""
    like_count: int = 0


@dataclass(frozen=True)
class UserHistory:
    """Ordered recent likes for a user."""

    items: list[UserHistoryItem]

    @property
    def liked_uris(self) -> list[str]:
        return [item.at_uri for item in self.items]

    @property
    def items_with_embeddings(self) -> list[UserHistoryItem]:
        return [item for item in self.items if item.embedding is not None]


class _CachedUserHistoryItem(BaseModel):
    at_uri: str
    liked_at: str
    embedding_b64: str | None
    author_did: str
    like_count: int


class _CachedUserHistoryDocument(BaseModel):
    schema_version: int
    user_did: str
    history_limit: int
    embedding_key: str
    fetched_at: datetime
    expires_at: datetime
    items: list[_CachedUserHistoryItem]


class UserHistoryCache(ABC):
    """Backend abstraction for cross-request user-history caching."""

    @abstractmethod
    async def retrieve(self, user_did: str) -> UserHistory | None:
        """Return a fresh cached history or ``None`` on a cache miss."""
        ...

    @abstractmethod
    async def store(self, user_did: str, history: UserHistory) -> None:
        """Store a history using the cache's configured TTL."""
        ...


def _user_history_cache_key(user_did: str) -> str:
    """Return a deterministic Firestore-safe document ID for a user DID."""
    return hashlib.sha256(user_did.encode()).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class FirestoreUserHistoryCache(UserHistoryCache):
    """Firestore-backed cache shared by all API instances."""

    def __init__(self, db: AsyncClient) -> None:
        self._db = db

    def _document(self, user_did: str):
        return self._db.collection(USER_HISTORY_CACHE_COLLECTION).document(
            _user_history_cache_key(user_did)
        )

    async def retrieve(self, user_did: str) -> UserHistory | None:
        snapshot = await self._document(user_did).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict()
        if data is None:
            return None

        try:
            document = _CachedUserHistoryDocument.model_validate(data)
            if (
                document.schema_version != USER_HISTORY_CACHE_VERSION
                or document.user_did != user_did
                or document.history_limit != USER_HISTORY_LIMIT
                or document.embedding_key != MINILM_L12_EMBEDDING_KEY
                or datetime.now(UTC) >= _as_utc(document.expires_at)
            ):
                return None

            items = [
                UserHistoryItem(
                    at_uri=item.at_uri,
                    liked_at=item.liked_at,
                    embedding=(
                        decode_float32_b64(item.embedding_b64)
                        if item.embedding_b64 is not None
                        else None
                    ),
                    author_did=item.author_did,
                    like_count=item.like_count,
                )
                for item in document.items
            ]
        except (ValidationError, TypeError, ValueError):
            logger.warning("Invalid user-history cache document", exc_info=True)
            return None

        return UserHistory(items=items)

    async def store(self, user_did: str, history: UserHistory) -> None:
        fetched_at = datetime.now(UTC)
        document = _CachedUserHistoryDocument(
            schema_version=USER_HISTORY_CACHE_VERSION,
            user_did=user_did,
            history_limit=USER_HISTORY_LIMIT,
            embedding_key=MINILM_L12_EMBEDDING_KEY,
            fetched_at=fetched_at,
            expires_at=fetched_at + timedelta(seconds=USER_HISTORY_CACHE_TTL_SECONDS),
            items=[
                _CachedUserHistoryItem(
                    at_uri=item.at_uri,
                    liked_at=item.liked_at,
                    embedding_b64=(
                        encode_float32_b64(item.embedding) if item.embedding is not None else None
                    ),
                    author_did=item.author_did,
                    like_count=item.like_count,
                )
                for item in history.items
            ],
        )
        await self._document(user_did).set(document.model_dump())


_user_history_cache: UserHistoryCache | None = None


def set_user_history_cache(cache: UserHistoryCache | None) -> None:
    """Install the process-wide shared cache backend."""
    global _user_history_cache
    _user_history_cache = cache


def get_user_history_cache() -> UserHistoryCache | None:
    return _user_history_cache


def _record_cache_count(metric: str, outcome: str) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(metric, 1, outcome=outcome)


async def _fetch_user_history_from_es(es, user_did: str) -> UserHistory:
    liked_uris, liked_at_times = await fetch_recent_liked_post_uris_and_times(
        es,
        user_did,
        limit=USER_HISTORY_LIMIT,
    )
    hydrated = await fetch_post_embeddings_and_metadata(es, liked_uris) if liked_uris else []
    hydrated_by_uri = {
        at_uri: (embedding, author_did, like_count)
        for at_uri, embedding, author_did, like_count in hydrated
    }

    items: list[UserHistoryItem] = []
    for at_uri, liked_at in zip(liked_uris, liked_at_times, strict=True):
        features = hydrated_by_uri.get(at_uri)
        if features is None:
            items.append(
                UserHistoryItem(
                    at_uri=at_uri,
                    liked_at=liked_at,
                    embedding=None,
                )
            )
            continue
        embedding, author_did, like_count = features
        items.append(
            UserHistoryItem(
                at_uri=at_uri,
                liked_at=liked_at,
                embedding=embedding,
                author_did=author_did,
                like_count=like_count,
            )
        )
    return UserHistory(items=items)


async def fetch_user_history_features(es, user_did: str) -> UserHistory:
    """Return recent-like features through request and Firestore caches."""

    async def _fetch() -> UserHistory:
        cache = get_user_history_cache()
        if cache is not None:
            try:
                async with timed(
                    logger,
                    "user_history.cache.lookup.duration_ms",
                    record_metric=True,
                ):
                    cached = await cache.retrieve(user_did)
            except Exception:
                _record_cache_count("user_history.cache.lookup_count", "error")
                logger.warning(
                    "User-history cache lookup failed; falling back to Elasticsearch",
                    exc_info=True,
                )
            else:
                outcome = "hit" if cached is not None else "miss"
                _record_cache_count("user_history.cache.lookup_count", outcome)
                if cached is not None:
                    return cached

        history = await _fetch_user_history_from_es(es, user_did)

        if cache is not None:
            try:
                async with timed(
                    logger,
                    "user_history.cache.write.duration_ms",
                    record_metric=True,
                ):
                    await cache.store(user_did, history)
            except Exception:
                _record_cache_count("user_history.cache.write_count", "error")
                logger.warning(
                    "User-history cache write failed; continuing with fresh data",
                    exc_info=True,
                )
            else:
                _record_cache_count("user_history.cache.write_count", "success")

        return history

    request_cache = get_request_cache()
    if request_cache is None:
        return await _fetch()
    key = (
        "fetch_user_history_features",
        user_did,
        USER_HISTORY_CACHE_VERSION,
        USER_HISTORY_LIMIT,
    )
    return await request_cache.get_or_compute(key, _fetch)

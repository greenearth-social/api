"""Shared cached user-history features for recommendation models.

The two-tower user side and heavy ranker consume the same recent-like
history.  This module assembles that history once, stores it in Firestore in
the background for reuse across API instances, and uses the existing request
cache to collapse duplicate work inside a feed render.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from google.cloud.firestore import (  # type: ignore[import-untyped]
    AsyncClient,
    async_transactional,
)
from pydantic import BaseModel, Field, ValidationError, field_validator

from .elasticsearch import (
    DEFAULT_LIKED_POSTS_LIMIT,
    fetch_post_embeddings_and_metadata,
    fetch_recent_liked_post_uris_and_times,
)
from .embeddings import (
    MINILM_L12_EMBEDDING_DIM,
    MINILM_L12_EMBEDDING_KEY,
    decode_float32_b64,
    encode_float32_b64,
)
from .metrics import get_metric_collector
from .request_cache import get_request_cache
from .telemetry import timed

logger = logging.getLogger(__name__)

USER_HISTORY_CACHE_COLLECTION = "user_history_cache"
USER_HISTORY_CACHE_VERSION = 1
USER_HISTORY_LIMIT = DEFAULT_LIKED_POSTS_LIMIT
# Cache reads are an optimization inside model calls whose total budgets are
# only a few seconds. Bound the entire Firestore operation (including SDK
# retries) so a slow read fails open with enough time left for
# Elasticsearch/model work. Background operations get a larger budget so
# normal Firestore tail latency does not turn successful work into a cache miss,
# while still preventing a stuck SDK call from delaying shutdown indefinitely.
USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS = 0.5
USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS = 5.0


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except ValueError:
        logger.warning("Invalid %s; using default %d", name, default)
        return default
    if value <= 0:
        logger.warning("Invalid %s=%d; using default %d", name, value, default)
        return default
    return value


def ttl_seconds() -> int:
    """Age at which an entry becomes stale and refreshes in the background."""
    return _int_env("GE_USER_HISTORY_CACHE_TTL_SEC", 600)


def max_age_seconds() -> int:
    """Hard age after which history is rebuilt instead of served stale."""
    configured = _int_env("GE_USER_HISTORY_CACHE_MAX_AGE_SEC", 1_800)
    fresh_ttl = ttl_seconds()
    if configured < fresh_ttl:
        logger.warning(
            "GE_USER_HISTORY_CACHE_MAX_AGE_SEC=%d is below the fresh TTL %d; using %d",
            configured,
            fresh_ttl,
            fresh_ttl,
        )
        return fresh_ttl
    return configured


def lease_seconds() -> int:
    """Longest a stale refresh may hold its cross-instance lease."""
    return _int_env("GE_USER_HISTORY_CACHE_LEASE_SEC", 30)


def refresh_timeout_seconds() -> float:
    """Budget for rebuilding stale history in the background."""
    return float(_int_env("GE_USER_HISTORY_REFRESH_TIMEOUT_SEC", 10))


def retry_cooldown_seconds() -> int:
    """Quiet period after a failed refresh before another instance retries."""
    return _int_env("GE_USER_HISTORY_CACHE_RETRY_COOLDOWN_SEC", 60)


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


@dataclass(frozen=True)
class UserHistoryCacheEntry:
    """A valid cached history and when its source data was fetched."""

    history: UserHistory
    fetched_at: datetime

    def age_seconds(self, *, now: datetime | None = None) -> float:
        current = now or datetime.now(UTC)
        return max(0.0, (current - _as_utc(self.fetched_at)).total_seconds())


class _CachedUserHistoryItem(BaseModel):
    at_uri: str = Field(min_length=1)
    liked_at: str = Field(min_length=1)
    embedding_b64: str | None
    author_did: str
    like_count: int = Field(ge=0)

    @field_validator("liked_at")
    @classmethod
    def validate_liked_at(cls, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("liked_at must be an ISO-8601 datetime") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("liked_at must include a timezone")
        return value


class _CachedUserHistoryDocument(BaseModel):
    schema_version: int
    user_did: str
    history_limit: int
    embedding_key: str
    fetched_at: datetime
    expires_at: datetime
    refresh_started_at: datetime | None = None
    refresh_failed_at: datetime | None = None
    items: list[_CachedUserHistoryItem] = Field(max_length=USER_HISTORY_LIMIT)


class UserHistoryCache(ABC):
    """Backend and background-work owner for user-history caching."""

    def __init__(self) -> None:
        # Keep task and pending-value state on the installed cache instance so
        # its lifecycle can be managed like the other application caches.
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._pending_histories: dict[str, UserHistory] = {}

    @abstractmethod
    async def retrieve(self, user_did: str) -> UserHistoryCacheEntry | None:
        """Return a valid cached entry or ``None`` on a hard cache miss."""
        ...

    @abstractmethod
    async def store(self, user_did: str, history: UserHistory) -> None:
        """Store a history using the cache's configured retention window."""
        ...

    async def claim_refresh(self, user_did: str) -> bool:
        """Claim a cross-instance stale-refresh lease.

        Non-Firestore test and alternate backends may rely on the process-local
        task guard and permit the claim by default.
        """
        return True

    async def release_refresh(self, user_did: str, *, failed: bool) -> None:
        """Release a stale-refresh lease, recording a failure when requested."""
        return None

    async def drain(self) -> None:
        """Await in-flight writes and stale refreshes during shutdown/tests."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks.values()), return_exceptions=True)


def _user_history_cache_key(user_did: str) -> str:
    """Return a deterministic Firestore-safe document ID for a user DID."""
    return hashlib.sha256(user_did.encode()).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _age_seconds(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return (now - _as_utc(value)).total_seconds()


def _decode_cached_embedding(value: str) -> list[float]:
    embedding = decode_float32_b64(value)
    if len(embedding) != MINILM_L12_EMBEDDING_DIM:
        raise ValueError(f"cached embedding must contain {MINILM_L12_EMBEDDING_DIM} floats")
    if not all(math.isfinite(component) for component in embedding):
        raise ValueError("cached embedding must contain only finite floats")
    return embedding


class FirestoreUserHistoryCache(UserHistoryCache):
    """Firestore-backed cache shared by all API instances."""

    def __init__(self, db: AsyncClient) -> None:
        super().__init__()
        self._db = db

    def _document(self, user_did: str):
        return self._db.collection(USER_HISTORY_CACHE_COLLECTION).document(
            _user_history_cache_key(user_did)
        )

    async def retrieve(self, user_did: str) -> UserHistoryCacheEntry | None:
        snapshot = await self._document(user_did).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict()
        if data is None:
            return None

        try:
            document = _CachedUserHistoryDocument.model_validate(data)
            fetched_at = _as_utc(document.fetched_at)
            expires_at = _as_utc(document.expires_at)
            if (
                document.schema_version != USER_HISTORY_CACHE_VERSION
                or document.user_did != user_did
                or document.history_limit != USER_HISTORY_LIMIT
                or document.embedding_key != MINILM_L12_EMBEDDING_KEY
                or expires_at <= fetched_at
                or datetime.now(UTC) >= expires_at
            ):
                return None

            items = [
                UserHistoryItem(
                    at_uri=item.at_uri,
                    liked_at=item.liked_at,
                    embedding=(
                        _decode_cached_embedding(item.embedding_b64)
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

        return UserHistoryCacheEntry(
            history=UserHistory(items=items),
            fetched_at=fetched_at,
        )

    async def store(self, user_did: str, history: UserHistory) -> None:
        fetched_at = datetime.now(UTC)
        document = _CachedUserHistoryDocument(
            schema_version=USER_HISTORY_CACHE_VERSION,
            user_did=user_did,
            history_limit=USER_HISTORY_LIMIT,
            embedding_key=MINILM_L12_EMBEDDING_KEY,
            fetched_at=fetched_at,
            expires_at=fetched_at + timedelta(seconds=max_age_seconds()),
            refresh_started_at=None,
            refresh_failed_at=None,
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

    async def claim_refresh(self, user_did: str) -> bool:
        """Transactionally claim the lease for one stale history refresh."""
        ref = self._document(user_did)
        fresh_ttl = ttl_seconds()
        lease = lease_seconds()
        cooldown = retry_cooldown_seconds()

        async def claim(transaction) -> bool:
            now = datetime.now(UTC)
            snapshot = await ref.get(transaction=transaction)
            data = snapshot.to_dict() if snapshot.exists else None
            if data is not None:
                try:
                    document = _CachedUserHistoryDocument.model_validate(data)
                except (ValidationError, TypeError, ValueError):
                    # An unreadable entry may be claimed and replaced. Lease
                    # fields cannot be trusted independently of validation.
                    document = None
                if document is not None:
                    compatible = (
                        document.schema_version == USER_HISTORY_CACHE_VERSION
                        and document.user_did == user_did
                        and document.history_limit == USER_HISTORY_LIMIT
                        and document.embedding_key == MINILM_L12_EMBEDDING_KEY
                    )
                    if compatible:
                        age = _age_seconds(document.fetched_at, now)
                        if age is not None and age <= fresh_ttl:
                            # Another instance completed the refresh after this
                            # process read the stale value.
                            return False
                    started = _age_seconds(document.refresh_started_at, now)
                    if started is not None and started < lease:
                        return False
                    failed = _age_seconds(document.refresh_failed_at, now)
                    if failed is not None and failed < cooldown:
                        return False
            transaction.set(ref, {"refresh_started_at": now}, merge=True)
            return True

        return await self._run_transaction(claim)

    async def _run_transaction(self, body: Callable[..., Awaitable[bool]]) -> bool:
        """Run a lease operation transactionally; kept as a test seam."""
        return await async_transactional(body)(self._db.transaction())

    async def release_refresh(self, user_did: str, *, failed: bool) -> None:
        update: dict = {"refresh_started_at": None}
        if failed:
            update["refresh_failed_at"] = datetime.now(UTC)
        await self._document(user_did).set(update, merge=True)


_user_history_cache: UserHistoryCache | None = None


def set_user_history_cache(cache: UserHistoryCache | None) -> None:
    """Install the process-wide shared cache backend."""
    global _user_history_cache
    _user_history_cache = cache


def get_user_history_cache() -> UserHistoryCache | None:
    return _user_history_cache


def _pending_user_history(
    cache: UserHistoryCache,
    user_did: str,
) -> UserHistory | None:
    return cache._pending_histories.get(user_did)


def _record_cache_count(metric: str, outcome: str) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(metric, 1, outcome=outcome)


def _record_cache_age(age_seconds: float) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record("user_history.cache.age_seconds", age_seconds)


async def _store_user_history(
    cache: UserHistoryCache,
    user_did: str,
    history: UserHistory,
) -> bool:
    try:
        async with timed(
            logger,
            "user_history.cache.write.duration_ms",
            record_metric=True,
        ):
            async with asyncio.timeout(USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS):
                await cache.store(user_did, history)
    except Exception:
        _record_cache_count("user_history.cache.write_count", "error")
        logger.warning(
            "User-history cache background write failed; continuing with fresh data",
            exc_info=True,
        )
        return False
    else:
        _record_cache_count("user_history.cache.write_count", "success")
        return True


async def _write_user_history(
    cache: UserHistoryCache,
    user_did: str,
    history: UserHistory,
) -> None:
    await _store_user_history(cache, user_did, history)


def _register_user_history_task(
    cache: UserHistoryCache,
    user_did: str,
    task: asyncio.Task[None],
    *,
    clears_pending: bool,
) -> None:
    cache._tasks[user_did] = task

    def _finished(completed: asyncio.Task[None]) -> None:
        if cache._tasks.get(user_did) is completed:
            cache._tasks.pop(user_did, None)
            if clears_pending:
                cache._pending_histories.pop(user_did, None)
        # Retrieve any unexpected exception so the event loop does not emit a
        # "Task exception was never retrieved" warning. Background cache
        # failures are normally contained by their task body.
        if not completed.cancelled():
            completed.exception()

    task.add_done_callback(_finished)


def _schedule_user_history_write(
    cache: UserHistoryCache,
    user_did: str,
    history: UserHistory,
) -> None:
    """Start one managed background write per cache/user pair.

    The pending value is also available to later requests in this process, so
    they do not repeat Elasticsearch work while Firestore persistence is still
    in flight.
    """
    if user_did in cache._tasks:
        return

    cache._pending_histories[user_did] = history
    task = asyncio.create_task(_write_user_history(cache, user_did, history))
    _register_user_history_task(cache, user_did, task, clears_pending=True)


async def _release_user_history_refresh(
    cache: UserHistoryCache,
    user_did: str,
) -> None:
    try:
        async with asyncio.timeout(USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS):
            await cache.release_refresh(user_did, failed=True)
    except Exception:
        # A lease is self-expiring, so failure here only delays the next retry.
        logger.warning(
            "Failed to release user-history refresh lease for %s",
            user_did,
            exc_info=True,
        )


async def _refresh_user_history(
    cache: UserHistoryCache,
    es,
    user_did: str,
) -> None:
    """Refresh one stale history after transactionally claiming its lease."""
    claimed = False
    outcome = "error"
    try:
        async with asyncio.timeout(refresh_timeout_seconds()):
            claimed = await cache.claim_refresh(user_did)
            if not claimed:
                outcome = "skipped"
                return
            history = await _fetch_user_history_from_es(es, user_did)

        if await _store_user_history(cache, user_did, history):
            outcome = "success"
            return
        outcome = "write_error"
    except TimeoutError:
        outcome = "timeout"
        logger.warning("User-history refresh timed out for %s", user_did)
    except Exception:
        logger.warning(
            "User-history refresh failed for %s; continuing with stale data",
            user_did,
            exc_info=True,
        )
    finally:
        if claimed and outcome != "success":
            await _release_user_history_refresh(cache, user_did)
        _record_cache_count("user_history.cache.refresh_count", outcome)


def _schedule_user_history_refresh(cache: UserHistoryCache, es, user_did: str) -> None:
    """Start at most one in-process refresh per cache/user pair."""
    if user_did in cache._tasks:
        return
    task = asyncio.create_task(_refresh_user_history(cache, es, user_did))
    _register_user_history_task(cache, user_did, task, clears_pending=False)


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
            pending = _pending_user_history(cache, user_did)
            if pending is not None:
                _record_cache_count("user_history.cache.lookup_count", "pending_hit")
                return pending

            try:
                async with timed(
                    logger,
                    "user_history.cache.lookup.duration_ms",
                    record_metric=True,
                ):
                    async with asyncio.timeout(USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS):
                        cached = await cache.retrieve(user_did)
            except Exception:
                _record_cache_count("user_history.cache.lookup_count", "error")
                logger.warning(
                    "User-history cache lookup failed; falling back to Elasticsearch",
                    exc_info=True,
                )
            else:
                if cached is not None:
                    age = cached.age_seconds()
                    _record_cache_age(age)
                    if age <= ttl_seconds():
                        _record_cache_count("user_history.cache.lookup_count", "hit")
                        return cached.history
                    if age < max_age_seconds():
                        _record_cache_count("user_history.cache.lookup_count", "stale")
                        _schedule_user_history_refresh(cache, es, user_did)
                        return cached.history
                _record_cache_count("user_history.cache.lookup_count", "miss")

        history = await _fetch_user_history_from_es(es, user_did)

        if cache is not None:
            _schedule_user_history_write(cache, user_did, history)

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

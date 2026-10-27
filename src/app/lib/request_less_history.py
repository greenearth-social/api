"""Durable requestLess feedback and background hydration for future rankers.

Feedback and retry budgets live together so concurrent API instances can claim
attempts safely. Readers return the saved snapshot; ES work never delays them.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from uuid import uuid4

from elastic_transport import ConnectionTimeout
from google.cloud.firestore import AsyncClient, async_transactional
from pydantic import AwareDatetime, BaseModel, Field

from .elasticsearch import fetch_post_embeddings_and_metadata
from .embeddings import (
    MINILM_L12_EMBEDDING_DIM,
    MINILM_L12_EMBEDDING_KEY,
    decode_float32_b64,
    encode_float32_b64,
)
from .metrics import get_metric_collector
from .request_cache import get_request_cache, request_cache_scope
from .user_history_cache import (
    USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS,
    USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS,
    USER_HISTORY_LIMIT,
)

logger = logging.getLogger(__name__)
HISTORY_COLLECTION = "request_less_history"
SCHEMA_VERSION = 1
MAX_HYDRATION_ATTEMPTS = 3
HYDRATION_RETRY_SECONDS = 15 * 60
HYDRATION_TIMEOUT_SECONDS = 5.0
_T = TypeVar("_T")
_Features = tuple[list[float], str, int]


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class RequestLessHistoryItem:
    at_uri: str
    received_at: datetime
    embedding: list[float] | None
    author_did: str = ""
    like_count: int = 0


@dataclass(frozen=True)
class RequestLessHistory:
    """Newest feedback first, including posts whose features are unavailable."""

    items: list[RequestLessHistoryItem]

    @property
    def items_with_embeddings(self) -> list[RequestLessHistoryItem]:
        return [item for item in self.items if item.embedding is not None]


class _StoredItem(BaseModel):
    at_uri: str = Field(min_length=1)
    received_at: AwareDatetime
    embedding_b64: str | None = None
    author_did: str = ""
    like_count: int = Field(default=0, ge=0)
    attempt_count: int = Field(default=0, ge=0, le=MAX_HYDRATION_ATTEMPTS)
    next_attempt_at: AwareDatetime | None = None
    attempt_id: str | None = None

    def eligible(self, now: datetime) -> bool:
        return (
            self.embedding_b64 is None
            and self.attempt_count < MAX_HYDRATION_ATTEMPTS
            and (self.next_attempt_at is None or self.next_attempt_at <= now)
        )


class _StoredHistory(BaseModel):
    schema_version: int = SCHEMA_VERSION
    user_did: str
    embedding_key: str = MINILM_L12_EMBEDDING_KEY
    items: list[_StoredItem] = Field(default_factory=list)


@dataclass(frozen=True)
class _HydrationClaim:
    at_uri: str
    attempt_count: int
    next_attempt_at: datetime
    attempt_id: str

    def matches(self, item: _StoredItem) -> bool:
        return (
            item.embedding_b64 is None
            and item.attempt_id == self.attempt_id
            and item.attempt_count == self.attempt_count
            and item.next_attempt_at == self.next_attempt_at
        )


@dataclass(frozen=True)
class _HydrationResult:
    saved: set[str]
    current: set[str]


def _parse_history(data: dict[str, Any] | None, user_did: str) -> _StoredHistory:
    history = _StoredHistory.model_validate(data)
    if (
        history.schema_version != SCHEMA_VERSION
        or history.embedding_key != MINILM_L12_EMBEDDING_KEY
        or history.user_did != user_did
    ):
        raise ValueError("Incompatible requestLess history document")
    return history


def _valid_embedding(embedding: list[float]) -> bool:
    return len(embedding) == MINILM_L12_EMBEDDING_DIM and all(
        isinstance(value, (int, float)) and math.isfinite(value)
        for value in embedding
    )


def _metric(name: str, **labels: str) -> None:
    collector = get_metric_collector()
    if collector is not None:
        try:
            collector.record(f"request_less.hydration.{name}", 1, **labels)
        except Exception:
            logger.warning("Failed to record requestLess hydration metric", exc_info=True)


class RequestLessHistoryService:
    def __init__(self, db: AsyncClient, hydration_es: Any) -> None:
        self._db = db
        self._es = hydration_es
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    def _document(self, user_did: str):
        key = hashlib.sha256(user_did.encode()).hexdigest()
        return self._db.collection(HISTORY_COLLECTION).document(key)

    async def _run_transaction(self, body: Callable[..., Awaitable[_T]]) -> _T:
        async with asyncio.timeout(USER_HISTORY_CACHE_BACKGROUND_TIMEOUT_SECONDS):
            return await async_transactional(body)(self._db.transaction())

    async def record(
        self, user_did: str, uri_times: Mapping[str, datetime]
    ) -> None:
        """Merge server receipt times without resetting retained retry budgets."""
        if not uri_times:
            return
        ref = self._document(user_did)

        async def merge(transaction):
            snapshot = await ref.get(transaction=transaction)
            history = (
                _parse_history(snapshot.to_dict(), user_did)
                if snapshot.exists else _StoredHistory(user_did=user_did)
            )
            by_uri = {item.at_uri: item for item in history.items}
            for uri, received_at in uri_times.items():
                if uri in by_uri:
                    item = by_uri[uri]
                    item.received_at = max(item.received_at, received_at)
                else:
                    by_uri[uri] = _StoredItem(at_uri=uri, received_at=received_at)
            history.items = sorted(
                by_uri.values(), key=lambda item: (item.received_at, item.at_uri),
                reverse=True,
            )[:USER_HISTORY_LIMIT]
            transaction.set(ref, history.model_dump())

        await self._run_transaction(merge)
        self.schedule_hydration(user_did)

    async def retrieve(self, user_did: str) -> RequestLessHistory:
        """Return saved features and schedule eligible work without awaiting it."""
        try:
            async with asyncio.timeout(USER_HISTORY_CACHE_READ_TIMEOUT_SECONDS):
                snapshot = await self._document(user_did).get()
            if not snapshot.exists:
                return RequestLessHistory([])
            history = _parse_history(snapshot.to_dict(), user_did)
            items = []
            for item in history.items:
                embedding = (
                    decode_float32_b64(item.embedding_b64)
                    if item.embedding_b64 is not None else None
                )
                if embedding is not None and not _valid_embedding(embedding):
                    raise ValueError("Invalid saved requestLess embedding")
                items.append(RequestLessHistoryItem(
                    item.at_uri, item.received_at, embedding,
                    item.author_did, item.like_count,
                ))
            if any(item.eligible(_now()) for item in history.items):
                self.schedule_hydration(user_did)
            return RequestLessHistory(items)
        except Exception:
            logger.warning("Failed to read requestLess history", exc_info=True)
            return RequestLessHistory([])

    async def _claim_due(self, user_did: str) -> list[_HydrationClaim]:
        ref = self._document(user_did)

        async def claim(transaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return []
            history = _parse_history(snapshot.to_dict(), user_did)
            now = _now()
            claims = []
            for item in history.items:
                if item.eligible(now):
                    item.attempt_count += 1
                    item.next_attempt_at = now + timedelta(seconds=HYDRATION_RETRY_SECONDS)
                    # A unique token also distinguishes an evicted/re-added URI
                    # from an older in-flight attempt for that same URI.
                    item.attempt_id = uuid4().hex
                    claims.append(_HydrationClaim(
                        item.at_uri, item.attempt_count, item.next_attempt_at,
                        item.attempt_id,
                    ))
            if claims:
                transaction.set(ref, history.model_dump())
            return claims

        return await self._run_transaction(claim)

    async def _store_results(
        self, user_did: str, claims: list[_HydrationClaim],
        features_by_uri: dict[str, _Features],
    ) -> _HydrationResult:
        ref = self._document(user_did)
        by_uri = {claim.at_uri: claim for claim in claims}

        async def merge(transaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return _HydrationResult(set(), set())
            history = _parse_history(snapshot.to_dict(), user_did)
            current: set[str] = set()
            saved: set[str] = set()
            for item in history.items:
                claim = by_uri.get(item.at_uri)
                if claim is None or not claim.matches(item):
                    continue
                current.add(item.at_uri)
                if item.at_uri in features_by_uri:
                    embedding, author_did, like_count = features_by_uri[item.at_uri]
                    item.embedding_b64 = encode_float32_b64(embedding)
                    item.author_did = author_did
                    item.like_count = like_count
                    item.next_attempt_at = None
                    saved.add(item.at_uri)
            if saved:
                transaction.set(ref, history.model_dump())
            return _HydrationResult(saved, current)

        return await self._run_transaction(merge)

    async def _hydrate(self, user_did: str) -> None:
        claims: list[_HydrationClaim] = []
        outcomes: dict[str, str] = {}
        try:
            claims = await self._claim_due(user_did)
            for claim in claims:
                _metric("attempt_count", attempt=str(claim.attempt_count))
            if not claims:
                return
            features: dict[str, _Features] = {}
            try:
                # Child tasks inherit request caches. Every consumed attempt
                # must perform its own search, while retaining metric context.
                async with request_cache_scope(), asyncio.timeout(HYDRATION_TIMEOUT_SECONDS):
                    rows = await fetch_post_embeddings_and_metadata(
                        self._es, [claim.at_uri for claim in claims], index="posts,replies"
                    )
                by_uri = {uri: (embedding, author, likes) for uri, embedding, author, likes in rows}
                for claim in claims:
                    row = by_uri.get(claim.at_uri)
                    if row is None:
                        outcomes[claim.at_uri] = "missing_features"
                        continue
                    embedding, author, likes = row
                    try:
                        if not _valid_embedding(embedding):
                            raise ValueError("Unusable MiniLM embedding")
                        # Ensure the vector is representable in the compact
                        # storage format before trying to save a partial batch.
                        encode_float32_b64(embedding)
                        if not isinstance(author, str) or not isinstance(likes, int) or likes < 0:
                            raise ValueError("Invalid post metadata")
                    except (TypeError, ValueError, OverflowError):
                        outcomes[claim.at_uri] = "invalid_features"
                    else:
                        features[claim.at_uri] = row
            except (TimeoutError, ConnectionTimeout):
                outcomes = {claim.at_uri: "timeout" for claim in claims}
                logger.warning("requestLess hydration timed out", exc_info=True)
            except Exception:
                outcomes = {claim.at_uri: "es_error" for claim in claims}
                logger.warning("requestLess hydration ES query failed", exc_info=True)

            try:
                result = await self._store_results(user_did, claims, features)
            except Exception:
                # Missing/invalid rows already have their final outcome; a
                # write failure affects only otherwise-successful rows.
                outcomes.update({uri: "write_error" for uri in features})
                logger.warning("Failed to save requestLess hydration results", exc_info=True)
            else:
                outcomes = {
                    uri: reason for uri, reason in outcomes.items() if uri in result.current
                }
                outcomes.update({uri: "success" for uri in result.saved})
        except asyncio.CancelledError:
            for claim in claims:
                outcomes.setdefault(claim.at_uri, "cancelled")
            raise
        except Exception:
            logger.exception("Failed to claim requestLess hydration")
        finally:
            # All telemetry runs after transactions finish; Firestore can
            # replay transaction callbacks without duplicating these counters.
            for claim in claims:
                outcome = outcomes.get(claim.at_uri)
                if outcome is None:
                    continue  # Evicted or superseded while ES was running.
                if outcome == "success":
                    _metric("success_count", attempt=str(claim.attempt_count))
                else:
                    _metric("failure_count", attempt=str(claim.attempt_count), reason=outcome)
                    if claim.attempt_count == MAX_HYDRATION_ATTEMPTS:
                        _metric("exhausted_count")
                        logger.info("requestLess hydration attempt budget exhausted")

    def schedule_hydration(self, user_did: str) -> None:
        if self._closing:
            return
        task = asyncio.create_task(self._hydrate(user_did))
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("requestLess hydration task failed", exc_info=task.exception())

    async def drain(self) -> None:
        """Stop accepting work and finish tasks before shared clients close."""
        self._closing = True
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)


_service: RequestLessHistoryService | None = None


def get_request_less_history_service() -> RequestLessHistoryService | None:
    return _service


def set_request_less_history_service(service: RequestLessHistoryService | None) -> None:
    global _service
    _service = service


async def fetch_request_less_history_features(user_did: str) -> RequestLessHistory:
    """Share one saved snapshot and retry decision among request consumers."""
    async def fetch() -> RequestLessHistory:
        service = get_request_less_history_service()
        return await service.retrieve(user_did) if service else RequestLessHistory([])

    cache = get_request_cache()
    if cache is None:
        return await fetch()
    return await cache.get_or_compute(("request_less_history", user_did), fetch)

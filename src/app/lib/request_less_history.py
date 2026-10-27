"""Durable requestLess feedback and background hydration for future rankers.

Feedback and retry budgets live together so concurrent API instances can claim
attempts safely. Readers return the saved snapshot; ES work never delays them.

Recording feedback and reading history can both schedule hydration for that
user. Each worker claims eligible posts, fetches their features in one ES batch,
and merges the results into the latest saved history.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
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
from .request_cache import request_cache_scope
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
_HydrationFeatures = tuple[list[float], str, int]


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class RequestLessHistoryItem:
    """One feedback entry exposed to callers, with optional hydrated features.

    ``received_at`` is the latest server receipt time for this URI. An absent
    embedding means features are unavailable; retry bookkeeping stays internal.
    """

    at_uri: str
    received_at: datetime
    embedding: list[float] | None
    author_did: str = ""
    like_count: int = 0


@dataclass(frozen=True)
class RequestLessHistory:
    """Saved history returned to callers, ordered by newest feedback first.

    ``items`` includes every retained entry. ``items_with_embeddings`` exposes
    the subset whose features are ready for use by a ranker.
    """

    items: list[RequestLessHistoryItem]

    @property
    def items_with_embeddings(self) -> list[RequestLessHistoryItem]:
        return [item for item in self.items if item.embedding is not None]


class _StoredItem(BaseModel):
    """Firestore representation of one URI, its features, and its retry budget.

    Features use a compact base64-encoded float32 embedding. A stored embedding
    makes the entry ready; otherwise it is pending until all attempts are used.
    Claims consume attempts before ES work and set the earliest next retry time.
    That time controls eligibility only; another feedback or read trigger must
    start the retry. There is no timer that wakes up when the cooldown expires.
    """

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
    """One user's Firestore document, containing their latest distinct URIs.

    Recording prunes all entries to USER_HISTORY_LIMIT, including pending and
    exhausted entries. Version fields identify the schema and embedding model;
    successful features remain stored until the entry is evicted.
    """

    schema_version: int = SCHEMA_VERSION
    user_did: str
    embedding_key: str = MINILM_L12_EMBEDDING_KEY
    items: list[_StoredItem] = Field(default_factory=list)


@dataclass(frozen=True)
class _HydrationClaim:
    """Identity of a committed attempt passed from Firestore to a worker.

    Results may only update an entry that still matches this claim. Its unique
    ID distinguishes both later attempts and feedback evicted then added again.
    """

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
    """Report from merging a batch's results into the latest history.

    ``hydrated_uris`` contains posts whose features were successfully fetched
    and saved to Firestore. Fetching features alone does not put a post here.

    ``applicable_uris`` contains posts whose feedback was still in history and
    still matched this attempt when results were merged. It includes both
    successful and failed attempts, but excludes feedback that was evicted or
    attempts that were replaced by newer ones. ``hydrated_uris`` is a subset.

    For example: A's features were saved, B had no features in ES, and C was
    evicted during the query. Then hydrated_uris = {A}, applicable_uris = {A, B}.
    Metrics count A as a success and B as a failure, and ignore C's result.
    """

    hydrated_uris: set[str]
    applicable_uris: set[str]


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
    """Own per-user persistence, hydration attempts, and background tasks.

    ``record`` saves feedback; ``retrieve`` returns the saved snapshot. Both can
    schedule hydration, while Firestore transactions coordinate concurrent work
    across API instances. The application drains this service before closing
    the shared database and ES clients.
    """

    def __init__(self, db: AsyncClient, hydration_es: Any) -> None:
        self._db = db
        self._es = hydration_es
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    def _document(self, user_did: str):
        """Use the like-history cache's hashed-DID document key convention."""
        key = hashlib.sha256(user_did.encode()).hexdigest()
        return self._db.collection(HISTORY_COLLECTION).document(key)

    async def _run_transaction[T](self, body: Callable[..., Awaitable[T]]) -> T:
        """Run a retryable Firestore callback with a bounded total duration.

        Firestore may replay ``body`` on conflicts, so callbacks only read and
        update documents. ES calls, task scheduling, and metrics happen outside.
        """
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
            # Merge and prune together so retention uses the latest combined
            # history even when feedback arrives concurrently.
            history.items = sorted(
                by_uri.values(), key=lambda item: (item.received_at, item.at_uri),
                reverse=True,
            )[:USER_HISTORY_LIMIT]
            transaction.set(ref, history.model_dump())

        await self._run_transaction(merge)
        # Only committed feedback is eligible for hydration. This also picks up
        # older entries for the same user whose retry cooldown has elapsed.
        self.schedule_hydration(user_did)

    async def retrieve(self, user_did: str) -> RequestLessHistory:
        """Return saved features and schedule eligible work without awaiting it.

        Hydration never changes this returned snapshot; saved results become
        available on a later read. A missing or unreadable document returns an
        empty history without falling back to synchronous ES work.
        """
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
            # This snapshot check only avoids unnecessary tasks. The worker must
            # recheck eligibility in a transaction before consuming an attempt.
            if any(item.eligible(_now()) for item in history.items):
                self.schedule_hydration(user_did)
            return RequestLessHistory(items)
        except Exception:
            logger.warning("Failed to read requestLess history", exc_info=True)
            return RequestLessHistory([])

    async def _claim_due(self, user_did: str) -> list[_HydrationClaim]:
        """Reserve eligible attempts atomically before any ES work starts.

        Concurrent workers see the committed cooldown and cannot claim those
        entries again immediately. A crash or failed result write still consumes
        the reserved attempt; unsuccessful entries have no age-based deadline.
        """
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
        features_by_uri: dict[str, _HydrationFeatures],
    ) -> _HydrationResult:
        """Merge valid features only into entries still owned by these claims.

        Reloading the document preserves feedback received while ES was running.
        Even a batch with no valid features checks which attempts still apply
        so its failure metrics exclude evicted or superseded entries.
        """
        ref = self._document(user_did)
        by_uri = {claim.at_uri: claim for claim in claims}

        async def merge(transaction):
            snapshot = await ref.get(transaction=transaction)
            if not snapshot.exists:
                return _HydrationResult(hydrated_uris=set(), applicable_uris=set())
            history = _parse_history(snapshot.to_dict(), user_did)
            applicable_uris: set[str] = set()
            hydrated_uris: set[str] = set()
            for item in history.items:
                claim = by_uri.get(item.at_uri)
                if claim is None or not claim.matches(item):
                    continue
                applicable_uris.add(item.at_uri)
                if item.at_uri in features_by_uri:
                    embedding, author_did, like_count = features_by_uri[item.at_uri]
                    item.embedding_b64 = encode_float32_b64(embedding)
                    item.author_did = author_did
                    item.like_count = like_count
                    item.next_attempt_at = None
                    hydrated_uris.add(item.at_uri)
            if hydrated_uris:
                transaction.set(ref, history.model_dump())
            return _HydrationResult(
                hydrated_uris=hydrated_uris, applicable_uris=applicable_uris
            )

        return await self._run_transaction(merge)

    async def _hydrate(self, user_did: str) -> None:
        """Claim one user's due posts, hydrate one batch, and report each outcome."""
        claims: list[_HydrationClaim] = []
        outcomes: dict[str, str] = {}
        try:
            claims = await self._claim_due(user_did)
            for claim in claims:
                _metric("attempt_count", attempt=str(claim.attempt_count))
            if not claims:
                return
            features: dict[str, _HydrationFeatures] = {}
            try:
                # Child tasks inherit request caches. Every consumed attempt
                # must perform its own search, while retaining metric context.
                async with request_cache_scope(), asyncio.timeout(HYDRATION_TIMEOUT_SECONDS):
                    rows = await fetch_post_embeddings_and_metadata(
                        self._es, [claim.at_uri for claim in claims], index="posts,replies"
                    )
                by_uri = {uri: (embedding, author, likes) for uri, embedding, author, likes in rows}
                # Classify each post separately so missing or malformed rows do
                # not prevent valid features from the same batch being saved.
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
                    uri: reason for uri, reason in outcomes.items() if uri in result.applicable_uris
                }
                outcomes.update({uri: "success" for uri in result.hydrated_uris})
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
        """Track a task for this trigger; transactional claims prevent duplicate work.

        Separate tasks let new feedback be picked up while an earlier batch is
        still running. The task set lets shutdown wait for all outstanding work.
        """
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


# Installed by the application lifespan so callers share clients and task ownership.
_service: RequestLessHistoryService | None = None


def get_request_less_history_service() -> RequestLessHistoryService | None:
    return _service


def set_request_less_history_service(service: RequestLessHistoryService | None) -> None:
    global _service
    _service = service


async def fetch_request_less_history_features(user_did: str) -> RequestLessHistory:
    """Read saved history and schedule eligible retries without awaiting hydration."""
    service = get_request_less_history_service()
    return await service.retrieve(user_did) if service else RequestLessHistory([])

"""Shared cache for the popularity candidate pool.

Popularity candidates are the same for every user — the generator's only
per-user input is the exclusion list, which is a filter over the result, not
an input to the ranking.  The underlying Elasticsearch query is also the most
expensive one on the feed path (~1.5s), and we run it for a large share of
requests.  So instead of querying per request we keep one pool of top
popularity candidates in Firestore, shared by every user and every API
instance, and filter it per request (see issue #330).

Three layers, cheapest first:

1. **In-process** — a parsed pool per key, reused for ``local_ttl_seconds``.
   Avoids a Firestore read (and a gunzip + parse) on every feed request.
2. **Firestore** — the shared entry, considered fresh for ``ttl_seconds``.
   Posts take a while to become popular, so a few minutes of staleness costs
   nothing in feed quality.
3. **Elasticsearch** — only on a refresh, which happens in the background
   after a response has already been served.

Refreshes are single-flight in two places: a set of in-flight keys inside the
process, and a transactional lease field on the document across instances.  A
stale entry is served — never awaited — while its refresh runs, so no user
request pays for the query.  When there is no entry at all (cold start, new
freshness window) the caller falls back to querying Elasticsearch directly for
just the candidates it needs, which is exactly the pre-cache behaviour.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Awaitable, Callable

from google.cloud.firestore import AsyncClient, async_transactional  # type: ignore[import-untyped]
from pydantic import TypeAdapter

from ...models import CandidatePost, MaxAgeHours
from ..metrics import get_metric_collector
from ..release import api_release_sha

if TYPE_CHECKING:
    from ...documents import PopularityCacheDocument

logger = logging.getLogger(__name__)

POPULARITY_CACHE_COLLECTION = "popularity_cache"

# Encoding of the stored blob. Bump the suffix on any incompatible change:
# entries written in an unknown format are ignored rather than mis-parsed.
PAYLOAD_FORMAT = "gzip-json-v1"

# Firestore's hard document limit is 1 MiB. Stay well under it — a pool that
# would exceed this is truncated rather than rejected at write time.
MAX_PAYLOAD_BYTES = 800_000

# Age at which a served entry is loud enough to alert on. The pool refreshes
# every ``ttl_seconds``; reaching this means refreshes are failing outright.
STALE_ALERT_SECONDS = 1200  # 20 minutes


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.warning("Invalid %s; using default %d", name, default)
        return default


# How many candidates to keep in the pool. Every request filters its own
# exclusions out of this shared pool, so it has to be big enough that a user
# who has seen a lot of popular posts still has candidates left. 500 is ~50x a
# feed's popularity allocation.
def pool_size() -> int:
    return _int_env("GE_POPULARITY_CACHE_POOL_SIZE", 500)


def ttl_seconds() -> int:
    return _int_env("GE_POPULARITY_CACHE_TTL_SEC", 300)


def local_ttl_seconds() -> int:
    return _int_env("GE_POPULARITY_CACHE_LOCAL_TTL_SEC", 30)


# Longest a refresh may hold the lease. Must comfortably exceed one pool-sized
# Elasticsearch query plus the Firestore write, or two instances will refresh
# concurrently; short enough that an instance killed mid-refresh doesn't
# freeze the entry for long.
def lease_seconds() -> int:
    return _int_env("GE_POPULARITY_CACHE_LEASE_SEC", 60)


_CANDIDATES_ADAPTER = TypeAdapter(list[CandidatePost])


def pool_key(*, video_only: bool, max_age_hours: MaxAgeHours) -> str:
    """Document ID for a pool.

    Both inputs change the Elasticsearch query — ``max_age_hours`` sets the
    hard window *and* scales the recency decay — so each combination needs its
    own pool. Only the combinations real traffic asks for are ever populated.
    """
    return f"{max_age_hours}h{'-video' if video_only else ''}"


def serialize_pool(candidates: list[CandidatePost]) -> bytes:
    """Encode *candidates* as a gzipped JSON array.

    JSON keeps the payload inspectable from any language and needs no
    deserialization allowlist — nothing here is worth the footgun of pickle.
    Candidate posts are mostly text and repeat their field names, so gzip cuts
    them to roughly a third; the embedding (the one incompressible field) is
    not part of a candidate at generation time.
    """
    raw = _CANDIDATES_ADAPTER.dump_json(candidates, exclude_none=True)
    return gzip.compress(raw, compresslevel=6)


def deserialize_pool(payload: bytes) -> list[CandidatePost]:
    """Decode a payload written by :func:`serialize_pool`."""
    return _CANDIDATES_ADAPTER.validate_json(gzip.decompress(payload))


def _serialize_within_limit(candidates: list[CandidatePost]) -> tuple[bytes, list[CandidatePost]]:
    """Serialize *candidates*, dropping the tail until the blob fits Firestore.

    Post length varies, so a fixed pool size can't guarantee a fitting
    document; trimming the least-popular entries is always preferable to a
    rejected write.
    """
    while True:
        payload = serialize_pool(candidates)
        if len(payload) <= MAX_PAYLOAD_BYTES or len(candidates) <= 1:
            return payload, candidates
        keep = max(1, int(len(candidates) * 0.8))
        logger.warning(
            "Popularity pool payload %d bytes exceeds limit; trimming %d candidates to %d",
            len(payload),
            len(candidates),
            keep,
        )
        candidates = candidates[:keep]


@dataclass(frozen=True)
class PopularityPool:
    """A cached pool and the time it was generated."""

    candidates: list[CandidatePost]
    generated_at: datetime

    def age_seconds(self, now: datetime | None = None) -> float:
        now = now or datetime.now(timezone.utc)
        return max(0.0, (now - self.generated_at).total_seconds())


@dataclass
class _LocalEntry:
    pool: PopularityPool
    fetched_at: float  # monotonic clock, for the in-process TTL only


# Fetches a pool of the requested size from Elasticsearch. Supplied by the
# caller so this module stays free of query knowledge.
PoolFetcher = Callable[[int], Awaitable[list[CandidatePost]]]


class PopularityCache:
    """Firestore-backed store for the shared popularity pool."""

    def __init__(self, db: AsyncClient) -> None:
        self._db = db
        self._local: dict[str, _LocalEntry] = {}
        self._refreshing: set[str] = set()
        # Strong references keep background refreshes from being garbage
        # collected mid-flight; the done callback drops them.
        self._tasks: set[asyncio.Task] = set()

    # -- public API --------------------------------------------------------

    async def get_pool(
        self,
        *,
        video_only: bool,
        max_age_hours: MaxAgeHours,
        fetch: PoolFetcher,
    ) -> PopularityPool | None:
        """Return the cached pool, refreshing it in the background if stale.

        Returns ``None`` when nothing usable is cached, which the caller
        should treat as "query Elasticsearch yourself this once".  Never
        raises: a cache that is down must degrade to the uncached path rather
        than break feed serving.
        """
        key = pool_key(video_only=video_only, max_age_hours=max_age_hours)
        ttl = ttl_seconds()

        pool = self._local_get(key, ttl)
        if pool is None:
            try:
                pool = await self._read(key)
            except Exception:
                logger.exception("Failed to read popularity cache entry '%s'", key)
                self._record("error", key)
                return None
            if pool is not None:
                self._local[key] = _LocalEntry(pool=pool, fetched_at=_now_monotonic())

        if pool is None:
            self._record("miss", key)
            self._spawn_refresh(key, fetch)
            return None

        age = pool.age_seconds()
        if mc := get_metric_collector():
            mc.record("candidates.popularity_cache.age_seconds", age, pool_key=key)

        if age <= ttl:
            self._record("hit", key)
            logger.debug("Popularity cache hit key=%s age=%.1fs", key, age)
            return pool

        self._record("stale", key)
        if age >= STALE_ALERT_SECONDS:
            # Alertable: refreshes have been failing for four TTLs running.
            logger.error(
                "Popularity cache entry '%s' is %.0fs old (>= %ds); refreshes are not completing",
                key,
                age,
                STALE_ALERT_SECONDS,
            )
        else:
            logger.info("Popularity cache stale key=%s age=%.1fs; refreshing", key, age)
        self._spawn_refresh(key, fetch)
        return pool

    async def drain(self) -> None:
        """Await in-flight background refreshes (shutdown and tests)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- internals ---------------------------------------------------------

    def _local_get(self, key: str, ttl: int) -> PopularityPool | None:
        """Return the in-process copy when it is both recent and fresh.

        Re-reading Firestore once the pool goes stale is what lets an instance
        pick up another instance's refresh instead of repeatedly trying to
        claim the lease itself.
        """
        entry = self._local.get(key)
        if entry is None:
            return None
        if _now_monotonic() - entry.fetched_at >= local_ttl_seconds():
            return None
        if entry.pool.age_seconds() > ttl:
            return None
        return entry.pool

    def _doc_ref(self, key: str):
        return self._db.collection(POPULARITY_CACHE_COLLECTION).document(key)

    async def _read(self, key: str) -> PopularityPool | None:
        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> ... -> popularity_cache -> documents.
        from ...documents import PopularityCacheDocument

        doc = await self._doc_ref(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        if data is None:
            return None
        try:
            cache_doc = PopularityCacheDocument.model_validate(data)
        except Exception:
            logger.warning("Invalid popularity cache document shape for key=%s", key)
            return None
        return self._pool_from_document(key, cache_doc)

    def _pool_from_document(
        self, key: str, cache_doc: PopularityCacheDocument
    ) -> PopularityPool | None:
        """Decode a document, or return ``None`` if it holds no usable pool.

        A document with only a lease stamped on it (written by a refresh that
        has not finished) is a miss, as is one in a format this release
        doesn't know how to read.
        """
        if cache_doc.payload is None or cache_doc.generated_at is None:
            return None
        if cache_doc.payload_format != PAYLOAD_FORMAT:
            logger.warning(
                "Ignoring popularity cache entry '%s' in unknown format %r",
                key,
                cache_doc.payload_format,
            )
            return None
        try:
            candidates = deserialize_pool(cache_doc.payload)
        except Exception:
            logger.warning(
                "Failed to decode popularity cache payload for key=%s", key, exc_info=True
            )
            return None
        return PopularityPool(candidates=candidates, generated_at=_as_utc(cache_doc.generated_at))

    async def _store(self, key: str, candidates: list[CandidatePost]) -> None:
        from ...documents import PopularityCacheDocument  # cycle; see _read

        payload, stored = _serialize_within_limit(candidates)
        now = datetime.now(timezone.utc)
        cache_doc = PopularityCacheDocument(
            generated_at=now,
            payload=payload,
            payload_format=PAYLOAD_FORMAT,
            count=len(stored),
            refresh_started_at=None,
            api_release_sha=api_release_sha(),
        )
        await self._doc_ref(key).set(cache_doc.model_dump())
        pool = PopularityPool(candidates=stored, generated_at=now)
        self._local[key] = _LocalEntry(pool=pool, fetched_at=_now_monotonic())
        logger.info(
            "Stored popularity cache entry key=%s candidates=%d payload_bytes=%d",
            key,
            len(stored),
            len(payload),
        )
        if mc := get_metric_collector():
            # In KB so the values land inside the default histogram buckets.
            mc.record("candidates.popularity_cache.payload_kb", len(payload) / 1024, pool_key=key)

    async def _claim_refresh(self, key: str) -> bool:
        """Take the refresh lease for *key*, transactionally.

        Returns ``False`` when another instance already refreshed the entry
        (it is fresh again) or is refreshing it now under an unexpired lease.
        """
        from ...documents import PopularityCacheDocument  # cycle; see _read

        ref = self._doc_ref(key)
        ttl = ttl_seconds()
        lease = lease_seconds()

        async def _claim(transaction) -> bool:
            now = datetime.now(timezone.utc)
            snapshot = await ref.get(transaction=transaction)
            data = snapshot.to_dict() if snapshot.exists else None
            if data is not None:
                try:
                    cache_doc = PopularityCacheDocument.model_validate(data)
                except Exception:
                    # Unreadable entry: claim it and overwrite with a good one.
                    cache_doc = PopularityCacheDocument()
                if (
                    cache_doc.generated_at is not None
                    and (now - _as_utc(cache_doc.generated_at)).total_seconds() <= ttl
                ):
                    return False
                started = cache_doc.refresh_started_at
                if started is not None and (now - _as_utc(started)).total_seconds() < lease:
                    return False
            transaction.set(ref, {"refresh_started_at": now}, merge=True)
            return True

        return await self._run_transaction(_claim)

    async def _run_transaction(self, body: Callable[..., Awaitable[bool]]) -> bool:
        """Run *body* inside a Firestore transaction.

        A seam: the SDK's transaction machinery is impractical to fake, so
        tests substitute a plain transaction object here and exercise the
        claim logic itself.
        """
        return await async_transactional(body)(self._db.transaction())

    def _spawn_refresh(self, key: str, fetch: PoolFetcher) -> None:
        if key in self._refreshing:
            return
        self._refreshing.add(key)
        task = asyncio.create_task(self._refresh(key, fetch))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _refresh(self, key: str, fetch: PoolFetcher) -> None:
        """Regenerate one pool, if this process wins the lease.

        Runs detached from any request, so every failure is contained here:
        the worst outcome is that the entry stays stale until the next
        request tries again.
        """
        outcome = "error"
        try:
            if not await self._claim_refresh(key):
                outcome = "skipped"
                return
            candidates = await fetch(pool_size())
            if not candidates:
                logger.warning("Popularity refresh for '%s' returned no candidates", key)
                outcome = "empty"
                return
            await self._store(key, candidates)
            outcome = "success"
        except Exception:
            logger.exception("Popularity cache refresh failed for key=%s", key)
        finally:
            self._refreshing.discard(key)
            if mc := get_metric_collector():
                mc.record(
                    "candidates.popularity_cache.refresh_count", 1, pool_key=key, outcome=outcome
                )

    def _record(self, outcome: str, key: str) -> None:
        if mc := get_metric_collector():
            mc.record("candidates.popularity_cache.lookup_count", 1, outcome=outcome, pool_key=key)


def _now_monotonic() -> float:
    return time.monotonic()


def _as_utc(value: datetime) -> datetime:
    """Firestore may hand back naive datetimes; they are always UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Process-level accessor
#
# Candidate generators are constructed at import time and their ``generate``
# signature is fixed by the CandidateGenerator interface, so the cache is
# reached the same way the metric collector and PostHog client are: a
# process-level handle installed during app startup. When it is unset (unit
# tests, scripts) the generator simply queries Elasticsearch directly.
# ---------------------------------------------------------------------------

_popularity_cache: PopularityCache | None = None


def set_popularity_cache(cache: PopularityCache | None) -> None:
    global _popularity_cache
    _popularity_cache = cache


def get_popularity_cache() -> PopularityCache | None:
    return _popularity_cache

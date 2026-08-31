"""Per-user cache of Bluesky follow lists.

Two candidate generators — ``followed_users`` and ``network_likes`` — need the
set of accounts a user follows.  Before this cache each of them walked
``app.bsky.graph.getFollows`` live on every feed request: up to ten sequential
round-trips to ``public.api.bsky.app`` under a 1s budget, sitting in front of
the Elasticsearch query, silently truncated when the budget expired, and
yielding nothing at all when Bluesky hiccuped (see issue #83).

Follows change rarely, so they are cached in Firestore — Firestore rather than
Elasticsearch because Elasticsearch is the loaded resource the caching is meant
to protect.  One document per user, holding a plain list of DIDs; a user's
follow list is small enough that neither compression nor an in-process layer
earns its keep, and per-user scoping means one person's unfollow invalidates
only their own entry.

Freshness comes from three places:

1. **Jetstream** (``ingex``) appends newly-followed DIDs to ``pending_adds``
   and stamps ``invalidated_at`` on an unfollow.  This is the primary
   mechanism; the TTL below is the backstop for anything it misses.
2. **A recurring backfill job** (``ingex``'s ``followed_users_backfill``,
   see api#453), which re-walks any entry this module's ``_staleness`` rule
   would flag. The API itself no longer refreshes on staleness — it only
   still does one synchronous cold-start walk when there is no entry at all,
   and falls back to a live walk on a Firestore read failure. Both of those
   remaining cases are "nothing to serve", not "what's cached is aging".
3. **The completeness flag** — see :class:`~app.lib.bsky.FollowsFetch`.  A
   truncated walk is still served, but is never trusted: it is flagged for the
   backfill job on every read until one walk finishes, and it can never
   overwrite a complete entry.

Every failure degrades to the pre-cache behaviour rather than breaking feed
serving: a Firestore outage means a live Bluesky walk, and a Bluesky outage
means the last known follow list.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from google.cloud.firestore import (  # type: ignore[import-untyped]
    AsyncClient,
    FieldFilter,
)

from .bsky import FollowedUsersLookupError, FollowsFetch, fetch_followed_user_dids
from .metrics import get_metric_collector
from .release import api_release_sha
from .request_cache import get_request_cache

if TYPE_CHECKING:
    from ..documents import FollowedUsersCacheDocument

logger = logging.getLogger(__name__)

FOLLOWED_USERS_CACHE_COLLECTION = "followed_users_cache"

# Cap on how many follows are fetched and stored. Consolidated from the two
# copies that previously lived in the candidate generators.
MAX_FOLLOWED_USERS = 1_000

# How long an entry lives before native Firestore TTL reclaims it. Bounds the
# collection to users who actually request feeds.
RETENTION_DAYS = 30

# Beyond this many un-folded jetstream deltas, refresh instead of merging on
# every read. Keeps the array (and the read-time merge) bounded.
MAX_PENDING_ADDS = 500

# Age at which staleness is loud enough to alert on: refreshes have been
# failing for long enough that jetstream cannot be the explanation either.
STALE_ALERT_SECONDS = 86_400  # 24 hours

# Single-flight key for the population sweep. A NUL byte cannot appear in a
# Firestore document ID, so this can never collide with a user's key.
_SWEEP_KEY = "\x00sweep"


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.warning("Invalid %s; using default %d", name, default)
        return default


def ttl_seconds() -> int:
    """Staleness threshold for observability only (see ``_staleness``/``_sweep``);
    ingex's recurring backfill job is what actually refreshes an entry, on its
    own copy of this same threshold. Must stay in sync with ingex's
    GE_FOLLOWS_CACHE_TTL_SEC — do not diverge."""
    return _int_env("GE_FOLLOWS_CACHE_TTL_SEC", 21_600)  # 6 hours


def sweep_interval_seconds() -> int:
    """How often one process samples population health (see ``_sweep``)."""
    return _int_env("GE_FOLLOWS_CACHE_SWEEP_SEC", 300)


# Rate-limits the population sweep per process. Module-level rather than
# per-instance so it survives a cache being rebuilt in tests and scripts.
_last_sweep_at: float | None = None


def reset_sweep_clock() -> None:
    """Forget when the last sweep ran (tests)."""
    global _last_sweep_at
    _last_sweep_at = None


def user_doc_id(user_did: str) -> str:
    """Firestore document ID for *user_did*.

    Deferred rather than imported at module scope: ``lib.firestore`` imports
    ``documents``, which pulls in the candidate generators, which import this
    module.  Sharing the helper keeps this collection keyed exactly like
    ``users``.
    """
    from .firestore import user_doc_id as _user_doc_id

    return _user_doc_id(user_did)


def _as_utc(value: datetime) -> datetime:
    """Firestore may hand back naive datetimes; they are always UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _age_seconds(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return (now - _as_utc(value)).total_seconds()


def _merge(follows: list[str], pending_adds: list[str]) -> list[str]:
    """``follows`` then any jetstream additions, de-duplicated, order preserved."""
    seen = set(follows)
    merged = list(follows)
    for did in pending_adds:
        if did not in seen:
            seen.add(did)
            merged.append(did)
    return merged


class FollowedUsersCache:
    """Firestore-backed store for per-user follow lists."""

    def __init__(self, db: AsyncClient) -> None:
        self._db = db
        # In-flight background tasks, so one instance does not stack
        # duplicates for the same key (a cold-store write, the sweep).
        self._refreshing: set[str] = set()
        # Strong references keep background tasks from being garbage
        # collected mid-flight; the done callback drops them.
        self._tasks: set[asyncio.Task] = set()

    # -- public API --------------------------------------------------------

    async def get_followed_dids(self, user_did: str) -> list[str]:
        """Return the DIDs *user_did* follows.

        A stale, incomplete, or invalidated entry is still served as-is —
        see the module docstring — the ingex backfill job re-walks it, not
        this request path.

        Raises :class:`FollowedUsersLookupError` only when there is nothing
        cached *and* the live walk fails — the same signal the generators
        already handle.
        """
        try:
            return await self._lookup(user_did)
        finally:
            # Deferred deliberately: the sweep is an aggregation over the whole
            # collection and must never sit on the request path.
            self._maybe_sweep()

    async def _lookup(self, user_did: str) -> list[str]:
        key = user_doc_id(user_did)

        try:
            entry = await self._read(key)
        except Exception:
            logger.exception("Failed to read followed-users cache for %s", user_did)
            self._record("error")
            return (await self._fetch(user_did)).dids

        if entry is None:
            # Cold: exactly the pre-cache path, under the request-path budget.
            self._record("miss")
            fetch = await self._fetch(user_did)
            self._spawn(key, self._store_cold(key, user_did, fetch))
            return fetch.dids

        follows = _merge(entry.follows, entry.pending_adds)
        # Recorded for every served entry, fresh or not: a series containing
        # only overdue entries says nothing about how fresh the healthy
        # population is, and reads as alarming even when nothing is wrong.
        self._note_age(entry, user_did)

        reason = self._staleness(entry)
        if reason is None:
            self._record("hit")
            return follows

        # Refreshing stale entries is now owned by the ingex followed-users
        # backfill job (a recurring Cloud Run Job), not the request path —
        # see api#453. This branch only records why the entry is stale.
        self._record(reason)
        return follows

    async def drain(self) -> None:
        """Await in-flight background refreshes (shutdown and tests)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- internals ---------------------------------------------------------

    def _doc_ref(self, key: str):
        return self._db.collection(FOLLOWED_USERS_CACHE_COLLECTION).document(key)

    async def _read(self, key: str) -> "FollowedUsersCacheDocument | None":
        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> ... -> followed_users_cache -> documents.
        from ..documents import FollowedUsersCacheDocument

        snapshot = await self._doc_ref(key).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict()
        if data is None:
            return None
        try:
            entry = FollowedUsersCacheDocument.model_validate(data)
        except Exception:
            # An unreadable document is a miss, not an error: it will be
            # overwritten by the next refresh.
            logger.warning("Invalid followed-users cache document for key=%s", key)
            return None
        if entry.generated_at is None:
            # No walk has ever populated this document — it holds only a lease
            # or a failure stamp. Serving it would look exactly like "follows
            # nobody" and silently drop the generator's allocation.
            return None
        return entry

    def _staleness(self, entry: "FollowedUsersCacheDocument") -> str | None:
        """Why *entry* needs refreshing, or ``None`` if it does not."""
        if not entry.complete:
            # However young: a truncated follow list must never be trusted.
            return "incomplete"
        if entry.invalidated_at is not None:
            return "invalidated"
        if len(entry.pending_adds) > MAX_PENDING_ADDS:
            return "pending_overflow"
        age = _age_seconds(entry.generated_at, datetime.now(timezone.utc))
        if age is None or age > ttl_seconds():
            return "stale"
        return None

    def _note_age(self, entry: "FollowedUsersCacheDocument", user_did: str) -> None:
        age = _age_seconds(entry.generated_at, datetime.now(timezone.utc))
        if age is None:
            return
        if mc := get_metric_collector():
            mc.record("follows_cache.age_seconds", age)
        if age >= STALE_ALERT_SECONDS:
            logger.error(
                "Followed-users cache for %s is %.0fs old (>= %ds); refreshes are not completing",
                user_did,
                age,
                STALE_ALERT_SECONDS,
            )

    async def _fetch(self, user_did: str, timeout_seconds: float | None = None) -> FollowsFetch:
        return await fetch_followed_user_dids(
            user_did,
            MAX_FOLLOWED_USERS,
            timeout_seconds=timeout_seconds,
        )

    def _document(self, fetch: FollowsFetch) -> dict:
        from ..documents import FollowedUsersCacheDocument  # cycle; see _read

        now = datetime.now(timezone.utc)
        return FollowedUsersCacheDocument(
            follows=fetch.dids,
            complete=fetch.complete,
            generated_at=now,
            pending_adds=[],
            invalidated_at=None,
            refresh_started_at=None,
            refresh_failed_at=None,
            expires_at=now + timedelta(days=RETENTION_DAYS),
            api_release_sha=api_release_sha(),
        ).model_dump()

    async def _store_cold(self, key: str, user_did: str, fetch: FollowsFetch) -> None:
        """Persist a cold-path result after the response has gone out."""
        try:
            await self._doc_ref(key).set(self._document(fetch))
        except Exception:
            logger.exception("Failed to store followed-users cache for %s", user_did)
            return
        logger.info(
            "Stored followed-users cache for %s follows=%d complete=%s",
            user_did,
            len(fetch.dids),
            fetch.complete,
        )

    def _maybe_sweep(self) -> None:
        """Schedule a population sweep if one is due.

        Rate-limited per process rather than coordinated across instances:
        the aggregation is three counts over a small collection, and a handful
        of instances reporting the same gauge is cheaper than a lease.
        """
        global _last_sweep_at
        now = time.monotonic()
        if _last_sweep_at is not None and now - _last_sweep_at < sweep_interval_seconds():
            return
        # Stamped before spawning so concurrent lookups don't stack sweeps.
        _last_sweep_at = now
        self._spawn(_SWEEP_KEY, self._sweep())

    async def _sweep(self) -> None:
        """Count the cached population by health and report it as gauges.

        Lookup metrics are request-weighted, so one heavy user stuck
        incomplete is indistinguishable from many users briefly incomplete.
        These are the per-user counterparts, and they are what answers
        "is follow state broadly correct, or degraded?".
        """
        try:
            collection = self._db.collection(FOLLOWED_USERS_CACHE_COLLECTION)
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds())
            total = await self._count(collection.count())
            incomplete = await self._count(
                collection.where(filter=FieldFilter("complete", "==", False)).count()
            )
            stale = await self._count(
                collection.where(filter=FieldFilter("generated_at", "<", cutoff)).count()
            )
        except Exception:
            # Observability must never take the feature down with it.
            logger.warning("Followed-users population sweep failed", exc_info=True)
            return

        if mc := get_metric_collector():
            # endpoint/traffic are normally inherited from the request that
            # spawned the task; pinned here so this gauge is one timeseries
            # rather than one per endpoint that happened to trigger it.
            labels = {"endpoint": "followed_users_sweep", "traffic": "internal"}
            mc.record("follows_cache.users_rate", total, state="total", **labels)
            mc.record("follows_cache.users_rate", incomplete, state="incomplete", **labels)
            mc.record("follows_cache.users_rate", stale, state="stale", **labels)
        logger.info(
            "Followed-users population: total=%d incomplete=%d stale=%d",
            total,
            incomplete,
            stale,
        )

    async def _count(self, aggregation) -> int:
        """Unwrap a Firestore count() aggregation result."""
        result = await aggregation.get()
        return int(result[0][0].value)

    def _spawn(self, key: str, coro) -> None:
        if key in self._refreshing:
            coro.close()
            return
        self._refreshing.add(key)
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda _: self._refreshing.discard(key))

    def _record(self, outcome: str) -> None:
        if mc := get_metric_collector():
            mc.record("follows_cache.lookup_count", 1, outcome=outcome)


# ---------------------------------------------------------------------------
# Process-level accessor
#
# Candidate generators are constructed at import time and their ``generate``
# signature is fixed by the CandidateGenerator interface, so the cache is
# reached the same way the metric collector and PostHog client are: a
# process-level handle installed during app startup. When it is unset (unit
# tests, scripts) callers fall through to a direct Bluesky walk.
# ---------------------------------------------------------------------------

_followed_users_cache: FollowedUsersCache | None = None


def set_followed_users_cache(cache: FollowedUsersCache | None) -> None:
    global _followed_users_cache
    _followed_users_cache = cache


def get_followed_users_cache() -> FollowedUsersCache | None:
    return _followed_users_cache


async def get_followed_dids_cached(user_did: str) -> list[str]:
    """Followed DIDs for *user_did*, via the cache when one is installed.

    Wrapped in the per-request cache so ``followed_users`` and
    ``network_likes`` in the same feed request share one lookup instead of
    each paying for a read.
    """

    async def lookup() -> list[str]:
        cache = get_followed_users_cache()
        if cache is None:
            return (await fetch_followed_user_dids(user_did, MAX_FOLLOWED_USERS)).dids
        return await cache.get_followed_dids(user_did)

    request_cache = get_request_cache()
    if request_cache is None:
        return await lookup()
    return await request_cache.get_or_compute(("followed_dids", user_did), lookup)

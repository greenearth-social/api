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
2. **A TTL refresh**, run in the background after a response has been served,
   so no user request ever waits on Bluesky.
3. **The completeness flag** — see :class:`~app.lib.bsky.FollowsFetch`.  A
   truncated walk is still served, but is never trusted: it refreshes on every
   read until one walk finishes, and it can never overwrite a complete entry.

Every failure degrades to the pre-cache behaviour rather than breaking feed
serving: a Firestore outage means a live Bluesky walk, and a Bluesky outage
means the last known follow list.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Awaitable, Callable

from google.cloud.firestore import AsyncClient, async_transactional  # type: ignore[import-untyped]

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


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.warning("Invalid %s; using default %d", name, default)
        return default


def ttl_seconds() -> int:
    """Age at which an entry is refreshed. A backstop under jetstream, so hours."""
    return _int_env("GE_FOLLOWS_CACHE_TTL_SEC", 21_600)  # 6 hours


def lease_seconds() -> int:
    """Longest a refresh may hold the lease before another instance may retry."""
    return _int_env("GE_FOLLOWS_CACHE_LEASE_SEC", 30)


def refresh_timeout_seconds() -> float:
    """Budget for a background walk.

    Generous compared with the request-path clamp in ``bsky.py``: nothing is
    waiting on it, and a walk that gives up early writes an incomplete entry
    that has to be redone.
    """
    return float(_int_env("GE_FOLLOWS_REFRESH_TIMEOUT_SEC", 15))


def retry_cooldown_seconds() -> int:
    """Quiet period after a failed refresh.

    Without it, a user whose follows cannot be fetched would spawn a refresh
    task on every single request.
    """
    return _int_env("GE_FOLLOWS_CACHE_RETRY_COOLDOWN_SEC", 60)


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
        # In-flight refreshes, so one instance does not stack duplicate tasks
        # for the same user while a refresh is already running.
        self._refreshing: set[str] = set()
        # Strong references keep background refreshes from being garbage
        # collected mid-flight; the done callback drops them.
        self._tasks: set[asyncio.Task] = set()

    # -- public API --------------------------------------------------------

    async def get_followed_dids(self, user_did: str) -> list[str]:
        """Return the DIDs *user_did* follows, refreshing in the background if stale.

        Raises :class:`FollowedUsersLookupError` only when there is nothing
        cached *and* the live walk fails — the same signal the generators
        already handle.
        """
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
        reason = self._staleness(entry)
        if reason is None:
            self._record("hit")
            return follows

        self._record(reason)
        self._note_age(entry, user_did)
        self._spawn(key, self._refresh(key, user_did))
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

    async def _claim(self, key: str) -> bool:
        """Take the refresh lease for *key*, transactionally.

        Returns ``False`` when another instance already refreshed the entry,
        is refreshing it now under an unexpired lease, or failed so recently
        that a retry would be pointless.
        """
        from ..documents import FollowedUsersCacheDocument  # cycle; see _read

        ref = self._doc_ref(key)
        lease = lease_seconds()
        cooldown = retry_cooldown_seconds()

        async def claim(transaction) -> bool:
            now = datetime.now(timezone.utc)
            snapshot = await ref.get(transaction=transaction)
            data = snapshot.to_dict() if snapshot.exists else None
            if data is not None:
                try:
                    entry = FollowedUsersCacheDocument.model_validate(data)
                except Exception:
                    # Unreadable: claim it and overwrite with a good one.
                    entry = FollowedUsersCacheDocument()
                else:
                    if self._staleness(entry) is None:
                        return False
                started = _age_seconds(entry.refresh_started_at, now)
                if started is not None and started < lease:
                    return False
                failed = _age_seconds(entry.refresh_failed_at, now)
                if failed is not None and failed < cooldown:
                    return False
            transaction.set(ref, {"refresh_started_at": now}, merge=True)
            return True

        return await self._run_transaction(claim)

    async def _run_transaction(self, body: Callable[..., Awaitable[bool]]) -> bool:
        """Run *body* inside a Firestore transaction.

        A seam: the SDK's transaction machinery is impractical to fake, so
        tests substitute a plain transaction object here and exercise the
        claim logic itself.
        """
        return await async_transactional(body)(self._db.transaction())

    async def _refresh(self, key: str, user_did: str) -> None:
        """Re-walk one user's follows, if this process wins the lease.

        Runs detached from any request, so every failure is contained here:
        the worst outcome is that the entry stays stale until the next request
        tries again.
        """
        outcome = "error"
        try:
            if not await self._claim(key):
                outcome = "skipped"
                return
            try:
                fetch = await self._fetch(user_did, timeout_seconds=refresh_timeout_seconds())
            except FollowedUsersLookupError:
                logger.warning("Followed-users refresh for %s found nothing", user_did)
                outcome = "failed"
                await self._release(key, failed=True)
                return

            if not fetch.complete and not await self._may_overwrite(key):
                # A partial walk must never shrink a complete entry. Leave it
                # stale so the next request tries again.
                logger.info(
                    "Discarding partial followed-users refresh for %s (%d dids); "
                    "keeping the complete entry",
                    user_did,
                    len(fetch.dids),
                )
                outcome = "partial_discarded"
                await self._release(key, failed=True)
                return

            await self._doc_ref(key).set(self._document(fetch))
            outcome = "success" if fetch.complete else "partial"
            logger.info(
                "Refreshed followed-users cache for %s follows=%d complete=%s",
                user_did,
                len(fetch.dids),
                fetch.complete,
            )
        except Exception:
            logger.exception("Followed-users cache refresh failed for %s", user_did)
            await self._release(key, failed=True)
        finally:
            self._refreshing.discard(key)
            if mc := get_metric_collector():
                mc.record("follows_cache.refresh_count", 1, outcome=outcome)

    async def _may_overwrite(self, key: str) -> bool:
        """True when the stored entry is absent or itself incomplete."""
        entry = await self._read(key)
        return entry is None or not entry.complete

    async def _release(self, key: str, *, failed: bool) -> None:
        """Drop the lease so the next request can retry immediately.

        The lease exists to exclude concurrent refreshes, not to impose a
        penalty box; ``refresh_failed_at`` is what paces the retries.
        """
        update: dict = {"refresh_started_at": None}
        if failed:
            update["refresh_failed_at"] = datetime.now(timezone.utc)
        try:
            await self._doc_ref(key).set(update, merge=True)
        except Exception:
            # The lease expires on its own; nothing further to do.
            logger.warning("Failed to release followed-users refresh lease for %s", key)

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

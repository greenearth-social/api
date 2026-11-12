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

1. **Jetstream** (``ingex``) appends each follow to ``pending_add_records`` and
   each unfollow's record key to ``pending_removes``, for users the API flags
   ``precompute_artifacts``.  Both apply on the next read: refreshes walk the
   user's own follow records, so ``follow_rkeys`` maps an unfollow's rkey back
   to the DID to drop.  This is the primary mechanism; the TTL below is the
   backstop for anything it misses.
2. **A TTL refresh.**  For ``precompute_artifacts`` users it is owned by an
   hourly job (``app.jobs.refresh_followed_users``), so their requests never
   touch Bluesky; everyone else (mostly Explore-tab previews) is refreshed in
   the background after a response has been served.  Either way no user
   request waits on Bluesky.
3. **The completeness flag** — see :class:`~app.lib.bsky.FollowsFetch`.  A
   truncated walk is still served, but is never trusted: it is refreshed until
   one walk finishes, and it can never overwrite a complete entry.

Every failure degrades to the pre-cache behaviour rather than breaking feed
serving: a Firestore outage means a live Bluesky walk, and a Bluesky outage
means the last known follow list.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Awaitable, Callable

from google.cloud.firestore import (  # type: ignore[import-untyped]
    AsyncClient,
    FieldFilter,
    async_transactional,
)

from .bsky import (
    AccountGoneError,
    FollowedUsersLookupError,
    FollowsFetch,
    fetch_followed_user_dids,
    fetch_visible_follow_records,
)
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
# every read. Keeps the arrays (and the read-time merge) bounded.
MAX_PENDING_DELTAS = 500

# Staleness the refresh job owns for precompute_artifacts users.  Their request
# path serves these without refreshing, unless the job looks dead (see
# job_grace_seconds).  Anything else (an unfollow that can't be applied, or an
# overflowing delta list) is refreshed from the request path as for everyone.
JOB_OWNED_STALENESS = frozenset({"stale", "incomplete"})

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


def job_grace_seconds() -> int:
    """Age past which a precompute user's request refreshes despite the job.

    The job refreshes these users hourly, so an entry this old means it is not
    running; falling back to the request path keeps follow lists from freezing.
    """
    return _int_env("GE_FOLLOWS_CACHE_JOB_GRACE_SEC", 4 * ttl_seconds())


def sweep_interval_seconds() -> int:
    """How often one process samples population health (see ``_sweep``)."""
    return _int_env("GE_FOLLOWS_CACHE_SWEEP_SEC", 300)


# Whether the current feed request's user is flagged precompute_artifacts, so
# the refresh job owns their TTL refresh.  Set by the router once it has read
# the user document; unset (False) means "treat as an Explore user", which
# keeps today's request-path refresh and so is the safe default.
_precompute_user: ContextVar[bool] = ContextVar("ge_follows_precompute_user", default=False)


def set_precompute_user(flag: bool) -> None:
    """Record whether the current request's user is a precompute_artifacts user."""
    _precompute_user.set(flag)


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


def _pending_adds(entry: "FollowedUsersCacheDocument") -> list[tuple[str, str]]:
    """``pending_add_records`` as ``(rkey, did)``; malformed records are skipped."""
    adds = []
    for record in entry.pending_add_records:
        rkey, sep, did = record.partition(":")
        if sep and rkey and did:
            adds.append((rkey, did))
    return adds


def _has_rkeys(entry: "FollowedUsersCacheDocument") -> bool:
    """Whether every stored follow has its record key (i.e. came from a repo walk)."""
    return len(entry.follow_rkeys) == len(entry.follows)


def _merge(entry: "FollowedUsersCacheDocument") -> list[str]:
    """``follows`` then jetstream additions, minus jetstream removals.

    De-duplicated, order preserved.  A removal is matched by record key, so a
    follow that was removed and re-made (a new rkey) stays.
    """
    removed = set(entry.pending_removes)
    stored: list[tuple[str | None, str]] = (
        list(zip(entry.follow_rkeys, entry.follows, strict=True))
        if _has_rkeys(entry)
        else [(None, did) for did in entry.follows]
    )
    seen: set[str] = set()
    merged: list[str] = []
    for rkey, did in stored + list(_pending_adds(entry)):
        if rkey in removed or did in seen:
            continue
        seen.add(did)
        merged.append(did)
    return merged


def _has_unresolved_removes(entry: "FollowedUsersCacheDocument") -> bool:
    """Whether an unfollow names a record key this entry cannot map to a DID.

    With record keys, an unknown key on a capped list is a follow beyond the
    cap — never stored, so nothing to drop.  Otherwise (an entry from
    getFollows, a follow getFollows hid, or one missing for any other reason)
    the unfollowed DID is unknown and only a refresh can drop it.
    """
    if not entry.pending_removes:
        return False
    if _has_rkeys(entry) and entry.capped:
        return False
    known = set(entry.follow_rkeys) | {rkey for rkey, _ in _pending_adds(entry)}
    return any(rkey not in known for rkey in entry.pending_removes)


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

        follows = _merge(entry)
        # Recorded for every served entry, fresh or not: a series containing
        # only overdue entries says nothing about how fresh the healthy
        # population is, and reads as alarming even when nothing is wrong.
        self._note_age(entry)

        reason = self._staleness(entry)
        if reason is None:
            self._record("hit")
            return follows

        if _precompute_user.get() and reason in JOB_OWNED_STALENESS:
            age = _age_seconds(entry.generated_at, datetime.now(timezone.utc))
            if age is None or age <= job_grace_seconds():
                # The refresh job owns this; serve what we have.
                self._record(reason)
                return follows
            reason = "job_overdue"

        self._record(reason)
        self._spawn(key, self.refresh(key, user_did, records=_precompute_user.get()))
        return follows

    async def drain(self) -> None:
        """Await in-flight background refreshes (shutdown and tests)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- internals ---------------------------------------------------------

    def _doc_ref(self, key: str):
        return self._db.collection(FOLLOWED_USERS_CACHE_COLLECTION).document(key)

    async def _read(self, key: str) -> "FollowedUsersCacheDocument | None":
        return self.entry_from(await self._doc_ref(key).get(), key)

    def entry_from(self, snapshot, key: str) -> "FollowedUsersCacheDocument | None":
        """The usable cache entry in *snapshot*, or ``None`` (treated as a miss)."""
        # Imported here (not at module top) to avoid an import cycle:
        # documents -> candidates.base -> ... -> followed_users_cache -> documents.
        from ..documents import FollowedUsersCacheDocument

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
        """Why *entry* needs refreshing, or ``None`` if it does not.

        The delta reasons come first: for precompute_artifacts users they are
        the ones refreshed from the request path (everything else waits for
        the job), so an unfollow is never held up behind another reason.
        """
        if _has_unresolved_removes(entry):
            return "unresolved_remove"
        if len(entry.pending_add_records) + len(entry.pending_removes) > MAX_PENDING_DELTAS:
            return "pending_overflow"
        if not entry.complete:
            # However young: a truncated follow list must never be trusted.
            return "incomplete"
        age = _age_seconds(entry.generated_at, datetime.now(timezone.utc))
        if age is None or age > ttl_seconds():
            return "stale"
        return None

    def refresh_reason(self, entry: "FollowedUsersCacheDocument | None") -> str | None:
        """Why the refresh job should re-walk *entry*, or ``None``.

        :meth:`_staleness`, plus the two cases only a repo walk fixes: no entry
        at all, and an entry without record keys (written by the request-path
        cold walk), which cannot apply jetstream unfollows.
        """
        if entry is None:
            return "missing"
        reason = self._staleness(entry)
        if reason is None and not _has_rkeys(entry):
            reason = "no_rkeys"
        return reason

    def _note_age(self, entry: "FollowedUsersCacheDocument") -> None:
        # Refreshes run on demand, so age alone does not indicate a failure.
        age = _age_seconds(entry.generated_at, datetime.now(timezone.utc))
        if age is None:
            return
        if mc := get_metric_collector():
            mc.record("follows_cache.age_seconds", age)

    async def _fetch(self, user_did: str, timeout_seconds: float | None = None) -> FollowsFetch:
        """Walk getFollows; the request-path budget unless *timeout_seconds* is given."""
        return await fetch_followed_user_dids(
            user_did,
            MAX_FOLLOWED_USERS,
            timeout_seconds=timeout_seconds,
        )

    async def _fetch_records(self, user_did: str) -> FollowsFetch:
        """Walk the user's visible follow records, with rkeys.  Off the request path only."""
        return await fetch_visible_follow_records(
            user_did,
            MAX_FOLLOWED_USERS,
            timeout_seconds=refresh_timeout_seconds(),
        )

    def _document(self, fetch: FollowsFetch) -> dict:
        from ..documents import FollowedUsersCacheDocument  # cycle; see entry_from

        now = datetime.now(timezone.utc)
        return FollowedUsersCacheDocument(
            follows=fetch.dids,
            follow_rkeys=fetch.rkeys,
            complete=fetch.complete,
            capped=fetch.capped,
            generated_at=now,
            pending_add_records=[],
            pending_removes=[],
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

    async def _claim(self, key: str, *, records: bool) -> bool:
        """Take the refresh lease for *key*, transactionally.

        Returns ``False`` when another instance already refreshed the entry,
        is refreshing it now under an unexpired lease, or failed so recently
        that a retry would be pointless.
        """
        from ..documents import FollowedUsersCacheDocument  # cycle; see entry_from

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
                    # A records walk also upgrades an entry lacking rkeys; a
                    # getFollows walk could not, so only staleness counts.
                    fresh = (
                        self.refresh_reason(entry) if records else self._staleness(entry)
                    ) is None
                    if fresh:
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

    async def refresh(self, key: str, user_did: str, *, records: bool) -> str:
        """Re-walk one user's follows, if this process wins the lease.

        *records* walks the user's own follow records, which carry the rkeys
        jetstream unfollows need; it is for precompute_artifacts users, the
        only ones jetstream tracks.  Everyone else is walked through getFollows,
        which needs no per-user PDS resolution.

        Runs detached from any request — spawned after a response, or from the
        refresh job — so every failure is contained here: the worst outcome is
        that the entry stays stale until the next attempt.  Returns the
        outcome recorded in ``follows_cache.refresh_count``.
        """
        outcome = "error"
        try:
            if not await self._claim(key, records=records):
                outcome = "skipped"
                return outcome
            try:
                fetch = await (
                    self._fetch_records(user_did)
                    if records
                    else self._fetch(user_did, timeout_seconds=refresh_timeout_seconds())
                )
            except AccountGoneError:
                # The caller decides what a gone account means (the refresh
                # job tombstones it); here it is just not a failure.
                logger.info("Followed-users refresh for %s: account is gone", user_did)
                outcome = "account_gone"
                await self._release(key, failed=True)
                return outcome
            except FollowedUsersLookupError:
                logger.warning("Followed-users refresh for %s found nothing", user_did)
                outcome = "failed"
                await self._release(key, failed=True)
                return outcome

            previous = await self._read(key)
            if not fetch.complete and previous is not None and previous.complete:
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
                return outcome

            self._note_drift(previous, fetch, user_did)
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
        return outcome

    def _note_drift(
        self,
        previous: "FollowedUsersCacheDocument | None",
        fetch: FollowsFetch,
        user_did: str,
    ) -> None:
        """Record how far the refreshed set moved from what we were serving.

        This is the only correctness signal in the system. An entry that is
        confidently ``complete`` but *wrong* — because jetstream deltas were
        dropped and the TTL had not yet fired — is otherwise indistinguishable
        from a correct one. In steady state churn per refresh sits near zero;
        a rising delta at TTL boundaries means the live update path is not
        doing its job.
        """
        if previous is None:
            return  # Nothing was being served; there is no drift to speak of.
        served = set(_merge(previous))
        refreshed = set(fetch.dids)
        added = len(refreshed - served)
        removed = len(served - refreshed)
        if mc := get_metric_collector():
            mc.record("follows_cache.refresh_added", added)
            mc.record("follows_cache.refresh_removed", removed)
        if added or removed:
            logger.info(
                "Followed-users refresh for %s moved the set: +%d -%d",
                user_did,
                added,
                removed,
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

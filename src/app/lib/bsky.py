"""Helper functions for querying bluesky API"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import httpx

from .http_client import get_http_client

logger = logging.getLogger(__name__)


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def _status_code_label(exc: BaseException) -> str:
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    if isinstance(exc, (httpx.ConnectError, httpx.NetworkError, httpx.RemoteProtocolError)):
        return "connection"
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return str(status) if status else "other"


def _count_failure(metric: str, exc: BaseException) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(metric, 1, status_code=_status_code_label(exc))


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum page size accepted by app.bsky.graph.getFollows and
# com.atproto.repo.listRecords (should not be changed)
FOLLOWS_PAGE_LIMIT = 100

# Maximum total time spent paginating followed users for one request
FOLLOWS_LOOKUP_TIMEOUT_SECONDS = 1.0

# Per-request timeout for each app.bsky.graph.getFollows call
FOLLOWS_HTTP_TIMEOUT = httpx.Timeout(connect=1.0, read=2.0, write=2.0, pool=1.0)

# Retry transient Bluesky failures once before giving up on the current page
FOLLOWS_MAX_RETRIES = 1
FOLLOWS_RETRY_BACKOFF_SECONDS = 0.1


# ---------------------------------------------------------------------------
# Followed users API query
# ---------------------------------------------------------------------------

class FollowedUsersLookupError(Exception):
    """Raised when followed-user lookup fails."""


class AccountGoneError(FollowedUsersLookupError):
    """The account no longer exists: deleted, deactivated or taken down.

    Raised only on an explicit "not found" from the service walked, never on a
    transient failure; :func:`fetch_visible_follow_records` further requires
    the user's PDS and the AppView to agree.
    """


# What the user's PDS (listRecords) and the AppView (getFollows) answer, with
# a 400, for an account that is gone.  The reference PDS reports a missing or
# taken-down repo as InvalidRequest "Could not find repo"; others use the
# dedicated error names.
_PDS_GONE_ERRORS = frozenset({"RepoNotFound", "RepoTakendown", "RepoDeactivated", "RepoSuspended"})
_PDS_GONE_MESSAGE = "Could not find repo"
_APPVIEW_GONE_ERRORS = frozenset({"AccountTakedown", "AccountDeactivated"})
_APPVIEW_GONE_MESSAGE = "Actor not found"


def _says_gone(exc: httpx.HTTPStatusError, errors: frozenset[str], message: str) -> bool:
    if exc.response.status_code != 400:
        return False
    try:
        body = exc.response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    return body.get("error") in errors or str(body.get("message", "")).startswith(message)


@dataclass(frozen=True)
class FollowsFetch:
    """The outcome of one followed-users walk.

    ``complete`` is ``True`` only when the walk ended on its own terms — the
    cursor ran out, or ``limit`` was reached, which is a deliberate bound
    rather than a failure. A page error or an expired budget yields whatever
    was collected with ``complete=False``.

    The distinction exists for the followed-users cache: a partial list is
    still worth serving (it beats no candidates at all), but storing one as
    authoritative would pin a user to a truncated follow set until the entry
    expires. Callers that only want the DIDs can use
    :func:`get_followed_user_dids`.
    """

    dids: list[str] = field(default_factory=list)
    complete: bool = True
    rkeys: list[str] = field(default_factory=list)
    """Follow-record keys, parallel to ``dids``.  Only a walk of the user's own
    repo (:func:`fetch_follow_records`) has them; ``getFollows`` does not
    expose record keys, so its walks leave this empty."""
    capped: bool = False
    """The walk stopped at ``limit`` with follows left unread."""


def _is_retryable_follow_lookup_error(exc: httpx.HTTPError) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 429 or 500 <= status_code < 600
    return False


async def _get_follows_page(
    client: httpx.AsyncClient,
    base_url: str,
    params: dict[str, str | int],
) -> dict:
    for attempt in range(FOLLOWS_MAX_RETRIES + 1):
        try:
            resp = await client.get(base_url, params=params, timeout=FOLLOWS_HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            if (
                attempt >= FOLLOWS_MAX_RETRIES
                or not _is_retryable_follow_lookup_error(exc)
            ):
                raise
            await asyncio.sleep(FOLLOWS_RETRY_BACKOFF_SECONDS)

    raise AssertionError("unreachable follow lookup retry state")


# One page of a walk: (did, rkey) pairs and the next cursor, if any.  The rkey
# is None for sources that do not expose record keys.
FollowsPage = tuple[list[tuple[str, str | None]], str | None]
PageFetcher = Callable[[str | None, int], Awaitable[FollowsPage]]


async def get_followed_user_dids(user_did: str, limit: int) -> list[str]:
    """Followed DIDs only, discarding the completeness signal."""
    return (await fetch_followed_user_dids(user_did, limit)).dids


async def fetch_followed_user_dids(
    user_did: str,
    limit: int,
    timeout_seconds: float | None = None,
) -> FollowsFetch:
    """Walk ``app.bsky.graph.getFollows`` for *user_did*.

    *timeout_seconds* bounds the whole walk; it defaults to the tight
    request-path budget, and the background cache refresh passes a larger one
    because it is not blocking a response.

    Raises :class:`FollowedUsersLookupError` only when nothing at all could be
    fetched. Any partial result comes back with ``complete=False``.
    """
    base_url = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows"
    client = get_http_client()

    async def fetch_page(cursor: str | None, page_limit: int) -> FollowsPage:
        params: dict[str, str | int] = {"actor": user_did, "limit": page_limit}
        if cursor:
            params["cursor"] = cursor
        try:
            data = await _get_follows_page(client, base_url, params)
        except httpx.HTTPStatusError as exc:
            if _says_gone(exc, _APPVIEW_GONE_ERRORS, _APPVIEW_GONE_MESSAGE):
                raise AccountGoneError(f"AppView has no account {user_did}") from exc
            raise
        if not isinstance(data, dict):
            raise FollowedUsersLookupError(f"Unexpected follows response for {user_did}")
        follows = data.get("follows", [])
        if not isinstance(follows, list):
            raise FollowedUsersLookupError(f"Unexpected follows response for {user_did}")
        items: list[tuple[str, str | None]] = [
            (follow["did"], None)
            for follow in follows
            if isinstance(follow, dict) and isinstance(follow.get("did"), str)
        ]
        return items, data.get("cursor")

    return await _walk(user_did, limit, timeout_seconds, fetch_page)


# Resolves a user's DID to their PDS for fetch_follow_records.  Created lazily
# and shared by the process: its in-memory cache saves a PLC directory round
# trip per refresh for users seen before.
_id_resolver = None


async def _resolve_pds(user_did: str) -> str:
    """The base URL of *user_did*'s PDS."""
    global _id_resolver
    if _id_resolver is None:
        from .atproto_auth import init_id_resolver

        _id_resolver = init_id_resolver()
    try:
        data = await _id_resolver.did.resolve_atproto_data(user_did)
    except Exception as exc:
        raise FollowedUsersLookupError(f"Could not resolve PDS for {user_did}") from exc
    if not data.pds:
        raise FollowedUsersLookupError(f"No PDS in DID document for {user_did}")
    return data.pds.rstrip("/")


async def fetch_follow_records(
    user_did: str,
    limit: int,
    timeout_seconds: float | None = None,
) -> FollowsFetch:
    """Walk *user_did*'s ``app.bsky.graph.follow`` records on their own PDS.

    Same contract as :func:`fetch_followed_user_dids`, plus record keys: a
    jetstream unfollow names only the deleted record's rkey, so the cache needs
    them to drop exactly the unfollowed account without a re-walk.  Records
    come back newest first, like ``getFollows``, so *limit* keeps the same
    follows.  Unlike ``getFollows`` this includes follows of deactivated
    accounts, which is harmless: they have no posts to retrieve.

    Resolving the PDS costs a PLC directory lookup on first use per user, so
    this is for off-request refreshes; the request path uses
    :func:`fetch_followed_user_dids`.
    """
    client = get_http_client()
    base_url: str | None = None

    async def fetch_page(cursor: str | None, page_limit: int) -> FollowsPage:
        nonlocal base_url
        if base_url is None:
            pds = await _resolve_pds(user_did)
            base_url = f"{pds}/xrpc/com.atproto.repo.listRecords"
        params: dict[str, str | int] = {
            "repo": user_did,
            "collection": "app.bsky.graph.follow",
            "limit": page_limit,
        }
        if cursor:
            params["cursor"] = cursor
        try:
            data = await _get_follows_page(client, base_url, params)
        except httpx.HTTPStatusError as exc:
            if _says_gone(exc, _PDS_GONE_ERRORS, _PDS_GONE_MESSAGE):
                raise AccountGoneError(f"PDS has no repo for {user_did}") from exc
            raise
        if not isinstance(data, dict):
            raise FollowedUsersLookupError(f"Unexpected listRecords response for {user_did}")
        records = data.get("records", [])
        if not isinstance(records, list):
            raise FollowedUsersLookupError(f"Unexpected listRecords response for {user_did}")
        items: list[tuple[str, str | None]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            uri, value = record.get("uri"), record.get("value")
            subject = value.get("subject") if isinstance(value, dict) else None
            if isinstance(uri, str) and isinstance(subject, str):
                items.append((subject, uri.rsplit("/", 1)[-1]))
        return items, data.get("cursor")

    return await _walk(user_did, limit, timeout_seconds, fetch_page)


async def _walk(
    user_did: str,
    limit: int,
    timeout_seconds: float | None,
    fetch_page: PageFetcher,
) -> FollowsFetch:
    """Paginate *fetch_page* up to *limit* follows within *timeout_seconds*.

    Raises :class:`FollowedUsersLookupError` only when nothing at all could be
    fetched. Any partial result comes back with ``complete=False``.
    """
    followed: list[tuple[str, str | None]] = []
    cursor: str | None = None

    if limit <= 0:
        return FollowsFetch(complete=True)

    if timeout_seconds is None:
        timeout_seconds = FOLLOWS_LOOKUP_TIMEOUT_SECONDS

    def result(complete: bool, capped: bool = False) -> FollowsFetch:
        kept = followed[:limit]
        rkeys = [rkey for _, rkey in kept if rkey is not None]
        return FollowsFetch(
            dids=[did for did, _ in kept],
            complete=complete,
            rkeys=rkeys if len(rkeys) == len(kept) else [],
            capped=capped,
        )

    try:
        async with asyncio.timeout(timeout_seconds):
            while len(followed) < limit:
                page_limit = min(FOLLOWS_PAGE_LIMIT, limit - len(followed))
                try:
                    items, cursor = await fetch_page(cursor, page_limit)
                except (
                    httpx.HTTPError,
                    ValueError,
                    FollowedUsersLookupError,
                ) as exc:
                    if followed:
                        # Terminal outcome for this lookup (partial success) —
                        # count it here since it never reaches the outer
                        # handlers below. A bare re-raise, in contrast, is
                        # counted once by whichever outer handler catches it,
                        # so we must not double-count here.
                        _count_failure("bsky.follows.failure_count", exc)
                        logger.warning(
                            "Returning %s partial followed users for %s after "
                            "follow lookup page failed: %s",
                            len(followed),
                            user_did,
                            exc,
                        )
                        return result(complete=False)
                    raise

                followed.extend(items)
                if not isinstance(cursor, str) or not cursor:
                    return result(complete=True)
    except TimeoutError as exc:
        _count_failure("bsky.follows.failure_count", exc)
        if followed:
            logger.warning(
                "Returning %s partial followed users for %s after follow lookup "
                "exceeded %.1fs",
                len(followed),
                user_did,
                timeout_seconds,
            )
            return result(complete=False)
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc
    except AccountGoneError:
        # Not a failure: Bluesky answered, and the answer is that the account
        # no longer exists.  The caller decides what that means.
        raise
    except FollowedUsersLookupError as exc:
        _count_failure("bsky.follows.failure_count", exc)
        raise
    except (httpx.HTTPError, ValueError) as exc:
        _count_failure("bsky.follows.failure_count", exc)
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc

    # Fell out of the loop on a reached limit with a cursor left: a deliberate
    # bound, so the walk is still complete.
    return result(complete=True, capped=True)


async def fetch_visible_follow_records(
    user_did: str,
    limit: int,
    timeout_seconds: float | None = None,
) -> FollowsFetch:
    """*user_did*'s follow records, limited to follows ``getFollows`` shows.

    The repo holds every follow record, including follows of accounts that
    have since deactivated, or that block or are blocked by *user_did*;
    ``getFollows`` hides those.  Walking both (concurrently, within one
    budget) gives ``getFollows``' DIDs with the record keys only the repo has.
    Both lists are newest first, so the visible follows among the first
    *limit* records are all within ``getFollows``' first *limit*.

    When ``getFollows`` cannot be walked in full the records are returned
    unfiltered and marked incomplete: never trusted, and never allowed to
    replace a complete entry.

    Raises :class:`AccountGoneError` only when both the PDS and the AppView
    say the account is gone; one of them alone is treated as a failed walk.
    """
    if timeout_seconds is None:
        timeout_seconds = FOLLOWS_LOOKUP_TIMEOUT_SECONDS
    records, visible = await asyncio.gather(
        fetch_follow_records(user_did, limit, timeout_seconds),
        fetch_followed_user_dids(user_did, limit, timeout_seconds),
        return_exceptions=True,
    )
    if isinstance(records, AccountGoneError):
        if isinstance(visible, AccountGoneError):
            raise records
        raise FollowedUsersLookupError(
            f"PDS has no repo for {user_did}, but the AppView still has the account"
        ) from records
    if isinstance(records, BaseException):
        raise records
    if isinstance(visible, BaseException) or not visible.complete:
        if isinstance(visible, BaseException) and not isinstance(
            visible, FollowedUsersLookupError
        ):
            raise visible
        logger.warning(
            "Could not walk getFollows for %s; keeping unfiltered follow records",
            user_did,
        )
        return FollowsFetch(
            dids=records.dids, complete=False, rkeys=records.rkeys, capped=records.capped
        )
    shown = set(visible.dids)
    kept = [(did, rkey) for did, rkey in zip(records.dids, records.rkeys, strict=True)
            if did in shown]
    return FollowsFetch(
        dids=[did for did, _ in kept],
        complete=records.complete,
        rkeys=[rkey for _, rkey in kept],
        capped=records.capped,
    )

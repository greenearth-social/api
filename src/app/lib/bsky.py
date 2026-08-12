"""Helper functions for querying bluesky API"""

import asyncio
import logging
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

# Maximum page size accepted by app.bsky.graph.getFollows (should not be changed)
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
    followed_dids: list[str] = []
    cursor: str | None = None

    if limit <= 0:
        return FollowsFetch(dids=followed_dids, complete=True)

    if timeout_seconds is None:
        timeout_seconds = FOLLOWS_LOOKUP_TIMEOUT_SECONDS

    client = get_http_client()

    try:
        async with asyncio.timeout(timeout_seconds):
            while len(followed_dids) < limit:
                page_limit = min(FOLLOWS_PAGE_LIMIT, limit - len(followed_dids))
                params = {"actor": user_did, "limit": page_limit}
                if cursor:
                    params["cursor"] = cursor

                try:
                    data = await _get_follows_page(client, base_url, params)
                    if not isinstance(data, dict):
                        raise FollowedUsersLookupError(
                            f"Unexpected follows response for {user_did}"
                        )

                    follows = data.get("follows", [])
                    if not isinstance(follows, list):
                        raise FollowedUsersLookupError(
                            f"Unexpected follows response for {user_did}"
                        )
                except (
                    httpx.HTTPError,
                    ValueError,
                    FollowedUsersLookupError,
                ) as exc:
                    if followed_dids:
                        # Terminal outcome for this lookup (partial success) —
                        # count it here since it never reaches the outer
                        # handlers below. A bare re-raise, in contrast, is
                        # counted once by whichever outer handler catches it,
                        # so we must not double-count here.
                        _count_failure("bsky.follows.failure_count", exc)
                        logger.warning(
                            "Returning %s partial followed users for %s after "
                            "follow lookup page failed: %s",
                            len(followed_dids),
                            user_did,
                            exc,
                        )
                        return FollowsFetch(dids=followed_dids[:limit], complete=False)
                    raise

                followed_dids.extend(
                    follow["did"]
                    for follow in follows
                    if isinstance(follow, dict)
                    and isinstance(follow.get("did"), str)
                )

                cursor = data.get("cursor")
                if not isinstance(cursor, str) or not cursor:
                    break
    except TimeoutError as exc:
        _count_failure("bsky.follows.failure_count", exc)
        if followed_dids:
            logger.warning(
                "Returning %s partial followed users for %s after follow lookup "
                "exceeded %.1fs",
                len(followed_dids),
                user_did,
                timeout_seconds,
            )
            return FollowsFetch(dids=followed_dids[:limit], complete=False)
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc
    except FollowedUsersLookupError as exc:
        _count_failure("bsky.follows.failure_count", exc)
        raise
    except (httpx.HTTPError, ValueError) as exc:
        _count_failure("bsky.follows.failure_count", exc)
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc

    # Fell out of the loop on an exhausted cursor or a reached limit: both are
    # the walk finishing on its own terms.
    return FollowsFetch(dids=followed_dids[:limit], complete=True)

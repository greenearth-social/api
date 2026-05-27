"""Helper functions for querying bluesky API"""

import asyncio
import logging

import httpx

from .http_client import get_http_client

logger = logging.getLogger(__name__)

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
    base_url = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows"
    followed_dids: list[str] = []
    cursor: str | None = None

    if limit <= 0:
        return followed_dids

    client = get_http_client()

    try:
        async with asyncio.timeout(FOLLOWS_LOOKUP_TIMEOUT_SECONDS):
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
                        logger.warning(
                            "Returning %s partial followed users for %s after "
                            "follow lookup page failed: %s",
                            len(followed_dids),
                            user_did,
                            exc,
                        )
                        return followed_dids[:limit]
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
        if followed_dids:
            logger.warning(
                "Returning %s partial followed users for %s after follow lookup "
                "exceeded %.1fs",
                len(followed_dids),
                user_did,
                FOLLOWS_LOOKUP_TIMEOUT_SECONDS,
            )
            return followed_dids[:limit]
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc
    except FollowedUsersLookupError:
        raise
    except (httpx.HTTPError, ValueError) as exc:
        raise FollowedUsersLookupError(
            f"Failed to fetch followed users for {user_did}"
        ) from exc

    return followed_dids[:limit]

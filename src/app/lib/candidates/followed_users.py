"""Candidate generator for posts from followed users.

Returns the last N posts from users that the requesting user follows"""

import asyncio
import logging

import httpx

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import candidate_posts_from_es_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum number of followed users to use in the query
MAX_FOLLOWED_USERS = 1_000

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
            resp = await client.get(base_url, params=params)
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

    try:
        async with httpx.AsyncClient(timeout=FOLLOWS_HTTP_TIMEOUT) as client:
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


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

async def followed_users_search(
    es,
    user_did: str,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Fetch posts from users followed by user_did from the ``posts`` index."""

    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    must_not: list[dict] = []
    if exclude_uris:
        must_not.append({"terms": {"at_uri": exclude_uris}})

    try:
        followed_dids: list[str] = await get_followed_user_dids(
            user_did,
            limit=MAX_FOLLOWED_USERS,
        )
    except FollowedUsersLookupError as exc:
        logger.warning(
            "Skipping followed_users candidate generation for %s after follow "
            "lookup failed: %s",
            user_did,
            exc,
        )
        return []

    if not followed_dids:
        return []

    query = {
        "bool": {
            "filter": [
                *filters,
                {"terms": {"author_did": followed_dids}},
            ],
            **("must_not" and {"must_not": must_not} if must_not else {}),
        }
    }

    resp = await es.search(
        index="posts",
        query=query,
        size=num_candidates,
        sort=[{"created_at": "desc"}],
    )
    return candidate_posts_from_es_response(resp, generator_name=generator_name)


class FollowedUsersCandidateGenerator(CandidateGenerator):
    """Returns the last N posts from users that the requesting user follows."""

    @property
    def name(self) -> str:
        return "followed_users"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
    ) -> CandidateResult:
        candidates = await followed_users_search(
            es,
            user_did,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)

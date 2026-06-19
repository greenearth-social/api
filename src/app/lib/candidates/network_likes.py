"""Candidate generator for posts that followed users have liked.

The likes index and posts index are not perfectly aligned: a recent like can
point at a post that is missing from our posts index, filtered out, a reply, or
otherwise unavailable as a candidate. To avoid guessing one fixed number of
liked URIs up front, this generator pages through recent likes with
``search_after`` until it has enough matching posts or reaches a hard scan cap.

As pages are scanned, repeated likes for the same post are deduplicated before
querying the posts index, but the repeated like count is retained and used as
the candidate score. Final ordering is by like count descending, with
last-seen like recency as the tie-breaker.
"""

import logging
from dataclasses import dataclass
from typing import Any

from ...models import CandidatePost
from ..bsky import FollowedUsersLookupError, get_followed_user_dids
from ..elasticsearch import unwrap_es_response
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum number of followed users to use in the query
MAX_FOLLOWED_USERS = 1_000

# Minimum number of recent likes to fetch in each page.
LIKED_POSTS_PAGE_SIZE = 100

# Hard cap on how many like documents to scan while looking for post hits.
MAX_LIKES_SCANNED = 5_000


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LikedPostUriPage:
    uris: list[str]
    next_search_after: list[Any] | None
    hit_count: int


async def fetch_recent_liked_post_uri_page(
    es,
    user_dids: list[str],
    size: int,
    search_after: list[Any] | None = None,
) -> LikedPostUriPage:
    """Return one page of recently liked post URIs for the given users."""
    if not user_dids or size <= 0:
        return LikedPostUriPage(uris=[], next_search_after=None, hit_count=0)

    query = {
        "bool": {
            "filter": [{"terms": {"author_did": user_dids}}],
        }
    }

    search_kwargs: dict[str, Any] = {}
    if search_after is not None:
        search_kwargs["search_after"] = search_after

    resp = await es.search(
        index="likes",
        query=query,
        size=size,
        sort=[{"created_at": "desc"}],
        _source=["subject_uri"],
        **search_kwargs,
    )

    data = unwrap_es_response(resp)
    hits = data.get("hits", {}).get("hits", [])
    uris: list[str] = []
    for hit in hits:
        uri = (hit.get("_source") or {}).get("subject_uri")
        if uri:
            uris.append(uri)

    next_search_after = None
    if hits:
        next_search_after = hits[-1].get("sort")

    return LikedPostUriPage(
        uris=uris,
        next_search_after=next_search_after,
        hit_count=len(hits),
    )


async def fetch_posts_by_uris(
    es,
    at_uris: list[str],
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Fetch posts for the supplied URIs, preserving the requested URI order."""
    if not at_uris:
        return []

    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    posts_query = {
        "bool": {
            "filter": [
                *filters,
                {"terms": {"at_uri": at_uris}},
            ],
        }
    }

    resp = await es.search(
        index="posts",
        query=posts_query,
        size=len(at_uris),
        _source=CANDIDATE_SOURCE_FIELDS,
    )

    exclude_set = set(exclude_uris) if exclude_uris else set()
    candidates_by_uri: dict[str, CandidatePost] = {}
    for candidate in candidate_posts_from_es_response(resp, generator_name=generator_name):
        if candidate.at_uri and candidate.at_uri not in exclude_set:
            candidates_by_uri[candidate.at_uri] = candidate

    return [
        candidates_by_uri[at_uri]
        for at_uri in at_uris
        if at_uri in candidates_by_uri
    ]


async def network_likes_search(
    es,
    user_did: str,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Fetch posts liked by users followed by user_did."""

    try:
        followed_dids: list[str] = await get_followed_user_dids(
            user_did,
            limit=MAX_FOLLOWED_USERS,
        )
    except FollowedUsersLookupError as exc:
        logger.warning(
            "Skipping network_likes candidate generation for %s after follow "
            "lookup failed: %s",
            user_did,
            exc,
        )
        return []

    if not followed_dids:
        return []

    page_size = min(
        max(LIKED_POSTS_PAGE_SIZE, num_candidates * 3),
        MAX_LIKES_SCANNED,
    )
    search_after: list[Any] | None = None
    scanned_likes = 0
    queried_uris: set[str] = set()
    like_counts: dict[str, int] = {}
    last_seen_order: dict[str, int] = {}
    liked_uri_order = 0
    candidates_by_uri: dict[str, CandidatePost] = {}

    while len(candidates_by_uri) < num_candidates and scanned_likes < MAX_LIKES_SCANNED:
        remaining_budget = MAX_LIKES_SCANNED - scanned_likes
        page = await fetch_recent_liked_post_uri_page(
            es,
            followed_dids,
            size=min(page_size, remaining_budget),
            search_after=search_after,
        )

        if page.hit_count == 0:
            break

        scanned_likes += page.hit_count

        new_uris: list[str] = []
        for uri in page.uris:
            like_counts[uri] = like_counts.get(uri, 0) + 1
            last_seen_order[uri] = liked_uri_order
            liked_uri_order += 1
            if uri not in queried_uris:
                queried_uris.add(uri)
                new_uris.append(uri)

        for candidate in await fetch_posts_by_uris(
            es,
            new_uris,
            generator_name=generator_name,
            video_only=video_only,
            exclude_uris=exclude_uris,
        ):
            if candidate.at_uri:
                candidates_by_uri[candidate.at_uri] = candidate

        if page.next_search_after is None or page.hit_count < min(page_size, remaining_budget):
            break
        search_after = page.next_search_after

    candidates = [
        candidate.model_copy(update={"score": float(like_counts[at_uri])})
        for at_uri, candidate in candidates_by_uri.items()
        if at_uri in like_counts
    ]
    candidates.sort(
        key=lambda candidate: (
            -(candidate.score or 0.0),
            last_seen_order.get(candidate.at_uri or "", liked_uri_order),
        )
    )
    return candidates[:num_candidates]


class NetworkLikesCandidateGenerator(CandidateGenerator):
    """Returns the last N posts that were liked by users that the target user follows"""

    @property
    def name(self) -> str:
        return "network_likes"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
    ) -> CandidateResult:
        candidates = await network_likes_search(
            es,
            user_did,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
        )

        if not candidates:
            logger.info("No liked posts found for followed users of user %s", user_did)

        return CandidateResult(generator_name=self.name, candidates=candidates)

"""Candidate generator for posts that followed users have liked.

The likes index and posts index are not perfectly aligned: a recent like can
point at a post that is missing from our posts index, filtered out, a reply, or
otherwise unavailable as a candidate. Rather than paging through likes until
enough posts match — which cost ~5 serial ES round trips inside a 4s budget —
this generator overfetches recent likes in a single query and hydrates the
resulting URIs in a single second query.

Repeated likes for the same post are deduplicated before querying the posts
index, but the repeated like count is retained and used as the candidate score.
Ordering is by like count descending, with last-seen like recency as the
tie-breaker; that same ordering picks which URIs are hydrated when the scan
yields more unique URIs than one hydrate query should carry.
"""

import logging

from ...models import CandidatePost, MaxAgeHours
from ..bsky import FollowedUsersLookupError
from ..config import fail_fast
from ..elasticsearch import unwrap_es_response
from ..followed_users_cache import MAX_FOLLOWED_USERS, get_followed_dids_cached
from ..metrics import get_metric_collector
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum number of followed users to use in the query is MAX_FOLLOWED_USERS,
# imported from followed_users_cache so the fetch cap and the query cap cannot
# drift apart.

# Likes scanned per requested candidate. Only a minority of recent likes point
# at a post that survives hydration, so the single scan overfetches heavily.
LIKES_OVERFETCH_FACTOR = 20

# Floor on the single likes scan, so small candidate requests still see enough
# likes to survive index skew without a second round trip.
MIN_LIKES_SCANNED = 1_000

# Hard cap on how many like documents to scan while looking for post hits.
MAX_LIKES_SCANNED = 5_000

# Hydrated docs carry embeddings, so the hydrate query is bounded more tightly
# than the likes scan: only the best-ranked URIs from the scan are hydrated.
HYDRATE_OVERFETCH_FACTOR = 10
MIN_HYDRATED_URIS = 300
MAX_HYDRATED_URIS = 1_000

# Lookback window for finding the matching posts
MAX_AGE_HOURS = 168


def liked_post_scan_size(num_candidates: int) -> int:
    """Number of like documents to pull in the generator's single likes query."""
    return min(
        max(MIN_LIKES_SCANNED, num_candidates * LIKES_OVERFETCH_FACTOR),
        MAX_LIKES_SCANNED,
    )


def hydrated_uri_limit(num_candidates: int) -> int:
    """Number of scanned URIs to send to the generator's single hydrate query."""
    return min(
        max(MIN_HYDRATED_URIS, num_candidates * HYDRATE_OVERFETCH_FACTOR),
        MAX_HYDRATED_URIS,
    )


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

async def fetch_recent_liked_post_uris(
    es,
    user_dids: list[str],
    size: int,
    max_age_hours: MaxAgeHours = MAX_AGE_HOURS,
    exclude_uris: list[str] | None = None,
) -> list[str]:
    """Return recently liked post URIs for the given users, most recent first."""
    if not user_dids or size <= 0:
        return []

    query = {
        "bool": {
            "filter": [
                {"terms": {"author_did": user_dids}},
                {"range": {"created_at": {"gte": f"now-{max_age_hours}h"}}},
            ],
        }
    }
    if exclude_uris:
        query["bool"]["must_not"] = [
            {"terms": {"subject_uri": exclude_uris}},
        ]

    resp = await es.search(
        index="likes",
        op="likes",
        query=query,
        size=size,
        sort=[{"created_at": "desc"}],
        _source=["subject_uri"],
    )

    data = unwrap_es_response(resp)
    hits = data.get("hits", {}).get("hits", [])
    uris: list[str] = []
    for hit in hits:
        uri = (hit.get("_source") or {}).get("subject_uri")
        if uri:
            uris.append(uri)

    return uris


async def fetch_posts_by_uris(
    es,
    at_uris: list[str],
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    max_age_hours: MaxAgeHours = MAX_AGE_HOURS,
) -> list[CandidatePost]:
    """Fetch posts for the supplied URIs, preserving the requested URI order."""
    if not at_uris:
        return []

    filters: list[dict] = [
        {"range": {"created_at": {"gte": f"now-{max_age_hours}h"}}},
    ]
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
        index="posts_recent",
        op="hydrate",
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


def _record_scan_telemetry(
    *,
    scanned_likes: int,
    scan_size: int,
    unique_uris: int,
    hydrated_uris: int,
    hydrated_hits: int,
) -> None:
    """Report whether either single-query limit was the binding constraint.

    Scanning once instead of paging trades round trips for the risk of
    under-filling a user's request. These say whether that happened and which
    limit caused it: a saturated scan means more recent likes existed than the
    scan window covered, a truncated hydrate means the scan found more unique
    URIs than one hydrate query carries, and the hit share is the measured
    likes-to-candidates yield the two limits are sized against.
    """
    if not hydrated_uris:
        return

    collector = get_metric_collector()
    if collector is None:
        return

    collector.record(
        "candidates.network_likes.hydrate_hit_share",
        hydrated_hits / hydrated_uris,
    )
    if scanned_likes >= scan_size:
        collector.record("candidates.network_likes.likes_scan_saturated_count", 1)
    if unique_uris > hydrated_uris:
        collector.record("candidates.network_likes.hydrate_truncated_count", 1)


async def network_likes_search(
    es,
    user_did: str,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    max_age_hours: MaxAgeHours = MAX_AGE_HOURS,
) -> list[CandidatePost]:
    """Fetch posts liked by users followed by user_did."""

    try:
        async with timed(logger, "follows_lookup", user_did=user_did):
            followed_dids: list[str] = await get_followed_dids_cached(user_did)
    except FollowedUsersLookupError as exc:
        logger.warning(
            "Skipping network_likes candidate generation for %s after follow "
            "lookup failed: %s",
            user_did,
            exc,
        )
        if fail_fast():
            raise
        return []

    if not followed_dids:
        return []

    scan_size = liked_post_scan_size(num_candidates)
    like_counts: dict[str, int] = {}
    last_seen_order: dict[str, int] = {}

    async with timed(
        logger,
        "es_network_likes_search",
        n_followed=len(followed_dids),
        num_candidates=num_candidates,
        scan_size=scan_size,
    ):
        liked_uris = await fetch_recent_liked_post_uris(
            es,
            followed_dids,
            size=scan_size,
            max_age_hours=max_age_hours,
            exclude_uris=exclude_uris,
        )

        for order, uri in enumerate(liked_uris):
            like_counts[uri] = like_counts.get(uri, 0) + 1
            last_seen_order[uri] = order

        # Likes come back most-recent-first, so a lower last-seen order means a
        # more recent like.
        ranked_uris = sorted(
            like_counts,
            key=lambda uri: (-like_counts[uri], last_seen_order[uri]),
        )[:hydrated_uri_limit(num_candidates)]

        hydrated = await fetch_posts_by_uris(
            es,
            ranked_uris,
            generator_name=generator_name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            max_age_hours=max_age_hours,
        )

    _record_scan_telemetry(
        scanned_likes=len(liked_uris),
        scan_size=scan_size,
        unique_uris=len(like_counts),
        hydrated_uris=len(ranked_uris),
        hydrated_hits=len(hydrated),
    )

    # fetch_posts_by_uris preserves the requested URI order, which is already
    # the final ranking.
    candidates = [
        candidate.model_copy(update={"score": float(like_counts[candidate.at_uri])})
        for candidate in hydrated
        if candidate.at_uri in like_counts
    ]
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
        max_age_hours: MaxAgeHours = MAX_AGE_HOURS,
    ) -> CandidateResult:
        candidates = await network_likes_search(
            es,
            user_did,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            max_age_hours=max_age_hours,
        )

        if not candidates:
            logger.info("No liked posts found for followed users of user %s", user_did)

        return CandidateResult(generator_name=self.name, candidates=candidates)

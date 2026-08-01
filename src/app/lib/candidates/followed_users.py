"""Candidate generator for posts from followed users.

Returns the newest posts, within the requested freshness window, from users
that the requesting user follows.
"""

import logging

from ...models import CandidatePost, MaxAgeHours
from ..bsky import FollowedUsersLookupError, get_followed_user_dids
from ..config import fail_fast
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response

logger = logging.getLogger(__name__)

MAX_FOLLOWED_USERS = 1_000


async def followed_users_search(
    es,
    user_did: str,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    max_age_hours: MaxAgeHours = 168,
) -> list[CandidatePost]:
    """Fetch followed-user posts from ``posts_recent`` within one strict window."""
    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    try:
        async with timed(logger, "bsky_get_follows", user_did=user_did):
            followed_dids = await get_followed_user_dids(
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
        if fail_fast():
            raise
        return []

    if not followed_dids:
        return []

    # Freshness applies to the candidate post's creation time. The query does
    # not expand beyond this bound when the requested allocation cannot be filled.
    filters.append(
        {"range": {"created_at": {"gte": f"now-{max_age_hours}h"}}}
    )
    query = {
        "bool": {
            "filter": [
                *filters,
                {"terms": {"author_did": followed_dids}},
            ],
        }
    }
    # Exclusions in the query (not client-side after an overfetch) keep the
    # fetch size at num_candidates even when a user's seen list is large.
    if exclude_uris:
        query["bool"]["must_not"] = [{"terms": {"at_uri": exclude_uris}}]

    async with timed(
        logger,
        "es_followed_users",
        n_followed=len(followed_dids),
        num_candidates=num_candidates,
    ):
        resp = await es.search(
            index="posts_recent",
            op="author_scan",
            query=query,
            size=num_candidates,
            sort=[{"created_at": "desc"}],
            _source=CANDIDATE_SOURCE_FIELDS,
        )

    candidates = candidate_posts_from_es_response(resp, generator_name=generator_name)
    if exclude_uris:
        # ES already excluded these; kept as a cheap safety net.
        exclude_set = set(exclude_uris)
        candidates = [candidate for candidate in candidates if candidate.at_uri not in exclude_set]
    return candidates[:num_candidates]


class FollowedUsersCandidateGenerator(CandidateGenerator):
    """Returns recent posts from users that the requesting user follows."""

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
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        candidates = await followed_users_search(
            es,
            user_did,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            max_age_hours=max_age_hours,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)

"""Candidate generator for posts from followed users.

Returns the last N posts from users that the requesting user follows"""

import logging

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response
from ..bsky import get_followed_user_dids, FollowedUsersLookupError
from ..telemetry import timed

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum number of followed users to use in the query
MAX_FOLLOWED_USERS = 1_000


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

    try:
        async with timed(logger, "bsky_get_follows", user_did=user_did):
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
        }
    }

    fetch_size = num_candidates + len(exclude_uris or [])

    async with timed(
        logger,
        "es_followed_users",
        n_followed=len(followed_dids),
        num_candidates=num_candidates,
    ):
        resp = await es.search(
            index="posts",
            query=query,
            size=fetch_size,
            sort=[{"created_at": "desc"}],
            _source=CANDIDATE_SOURCE_FIELDS,
        )

    candidates = candidate_posts_from_es_response(resp, generator_name=generator_name)
    if exclude_uris:
        exclude_set = set(exclude_uris)
        candidates = [c for c in candidates if c.at_uri not in exclude_set]
    return candidates[:num_candidates]


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

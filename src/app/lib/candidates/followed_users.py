"""Candidate generator for posts from followed users.

Returns the last N posts from users that the requesting user follows"""

import logging

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import candidate_posts_from_es_response
from ..bsky import get_followed_user_dids, FollowedUsersLookupError

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

    must_not: list[dict] = [{"exists": {"field": "thread_parent_post"}}]
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

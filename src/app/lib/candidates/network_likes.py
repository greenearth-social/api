"""Candidate generator for posts that followed users have liked"""

import logging

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import candidate_posts_from_es_response
from ..bsky import get_followed_user_dids, FollowedUsersLookupError
from ..elasticsearch import fetch_recent_liked_post_uris, fetch_post_embeddings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Maximum number of followed users to use in the query
MAX_FOLLOWED_USERS = 1_000

MAX_LIKED_POSTS = 1_000


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

async def network_likes_search(
    es,
    user_did: str,
    num_candidates: int,
) -> list[str]:
    """Fetch posts from users followed by user_did from the ``posts`` index."""

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

    # now query recent likes for the given followed_dids
    return await fetch_recent_liked_post_uris(es, followed_dids, limit=num_candidates)


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
        # 1. Get recently liked post URIs for followed users
        liked_uris = await network_likes_search(
            es,
            user_did,
            num_candidates,
        )

        if not liked_uris:
            logger.info("No liked posts found for followed users of user %s", user_did)
            return CandidateResult(generator_name=self.name, candidates=[])
        
        # 2. Fetch the rest of the data (e.g. embeddings) for those posts
        filters: list[dict] = []
        if video_only:
            filters.append({"term": {"contains_video": True}})

        must_not: list[dict] = [{"exists": {"field": "thread_parent_post"}}]
        if exclude_uris:
            must_not.append({"terms": {"at_uri": exclude_uris}})

        posts_query = {
            "bool": {
                "filter": [
                    *filters,
                    {"terms": {"at_uri": liked_uris}},
                ],
                **("must_not" and {"must_not": must_not} if must_not else {}),
            }
        }

        resp = await es.search(
            index="posts",
            query=posts_query,
            size=len(liked_uris),
        )

        candidates = candidate_posts_from_es_response(resp, generator_name=self.name)

        return CandidateResult(generator_name=self.name, candidates=candidates)

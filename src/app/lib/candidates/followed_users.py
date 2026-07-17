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
RECENT_FRIENDS_WINDOW = "24h"
MAX_FRIENDS_WINDOW = "7d"


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
    """Fetch posts from users followed by user_did from the ``posts_recent`` index."""
    try:
        candidates, _ = await _followed_users_search_details(
            es,
            user_did,
            num_candidates,
            generator_name=generator_name,
            video_only=video_only,
            exclude_uris=exclude_uris,
        )
        return candidates
    except FollowedUsersLookupError:
        return []


async def _followed_users_search_details(
    es,
    user_did: str,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    created_at_gte: str | None = None,
    created_at_lt: str | None = None,
    followed_dids: list[str] | None = None,
) -> tuple[list[CandidatePost], str | None]:

    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    if followed_dids is None:
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
            raise

    if not followed_dids:
        return [], "no_followed_users"

    if created_at_gte is not None or created_at_lt is not None:
        bounds = {}
        if created_at_gte is not None:
            bounds["gte"] = created_at_gte
        if created_at_lt is not None:
            bounds["lt"] = created_at_lt
        filters.append({"range": {"created_at": bounds}})

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
            index="posts_recent",
            query=query,
            size=fetch_size,
            sort=[{"created_at": "desc"}],
            _source=CANDIDATE_SOURCE_FIELDS,
        )

    candidates = candidate_posts_from_es_response(resp, generator_name=generator_name)
    if exclude_uris:
        exclude_set = set(exclude_uris)
        candidates = [c for c in candidates if c.at_uri not in exclude_set]
    candidates = candidates[:num_candidates]
    return candidates, None if candidates else "no_recent_followed_posts"


class FollowedUsersCandidateGenerator(CandidateGenerator):
    """Returns the last N posts from users that the requesting user follows."""

    @property
    def name(self) -> str:
        return "followed_users"

    async def generate_stages(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
    ) -> list[CandidateResult]:
        """Fill the social quota from 24h posts, then posts up to seven days old."""
        try:
            async with timed(logger, "bsky_get_follows", user_did=user_did):
                followed_dids = await get_followed_user_dids(
                    user_did,
                    limit=MAX_FOLLOWED_USERS,
                )
        except FollowedUsersLookupError:
            return [
                CandidateResult(
                    generator_name=self.name,
                    candidates=[],
                    status="error",
                    reason="follow_lookup_failed",
                    mode="direct_friends_recent",
                )
            ]

        if not followed_dids:
            return [
                CandidateResult(
                    generator_name=self.name,
                    candidates=[],
                    status="empty",
                    reason="no_followed_users",
                    mode="direct_friends_recent",
                )
            ]

        recent, recent_reason = await _followed_users_search_details(
            es,
            user_did,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            created_at_gte=f"now-{RECENT_FRIENDS_WINDOW}",
            followed_dids=followed_dids,
        )
        results = [
            CandidateResult(
                generator_name=self.name,
                candidates=recent,
                status="success" if recent else "empty",
                reason=recent_reason,
                mode="direct_friends_recent",
            )
        ]

        shortfall = num_candidates - len(recent)
        if shortfall <= 0:
            return results

        older_exclusions = list(dict.fromkeys([
            *(exclude_uris or []),
            *(candidate.at_uri for candidate in recent if candidate.at_uri),
        ]))
        older, older_reason = await _followed_users_search_details(
            es,
            user_did,
            shortfall,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=older_exclusions,
            created_at_gte=f"now-{MAX_FRIENDS_WINDOW}",
            created_at_lt=f"now-{RECENT_FRIENDS_WINDOW}",
            followed_dids=followed_dids,
        )
        results.append(
            CandidateResult(
                generator_name=self.name,
                candidates=older,
                status="success" if older else "empty",
                reason=older_reason or (None if older else "no_older_followed_posts"),
                mode="direct_friends_7d",
            )
        )
        return results

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
    ) -> CandidateResult:
        stages = await self.generate_stages(
            es,
            user_did,
            num_candidates,
            video_only,
            exclude_uris,
        )
        candidates = [candidate for stage in stages for candidate in stage.candidates]
        failure = next((stage for stage in stages if stage.status == "error"), None)
        return CandidateResult(
            generator_name=self.name,
            candidates=candidates,
            status="success" if candidates else "empty",
            reason=failure.reason if failure else (stages[-1].reason if stages else None),
        )

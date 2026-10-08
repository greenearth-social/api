"""Candidate generator for posts the caller retrieved themselves.

The slate API (``POST /slate/generate``) lets a partner mix their own
candidates into a slate alongside our generators. This generator wraps those
caller-supplied URIs so they flow through the same allocation, dedup, hydration
and diagnostics path as every registered generator.

Only the URI and an optional score come from the caller. Everything else about
a post (text, author, media flags, politics and Perspective scores) is loaded
from our own index in one ``terms`` lookup, so downstream rankers and MMR see
the same shape they see for internally-retrieved posts. A URI we don't have in
the index is dropped: there is no path for a caller to supply post metadata.

Instances are request-scoped (one per call, holding that call's candidates) and
are passed to ``run_generate`` via ``extra_generators`` rather than registered
globally.
"""

from __future__ import annotations

import logging

from ...models import EXTERNAL_GENERATOR_NAME, ExternalCandidate, MaxAgeHours
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response

logger = logging.getLogger(__name__)

# Same index the pipeline hydrates embeddings from, so a URI resolvable here is
# also hydratable later.
SUPPLIED_CANDIDATES_INDEX = "posts_recent"


class SuppliedCandidatesGenerator(CandidateGenerator):
    """Serve caller-supplied URIs, hydrated from our index, in the caller's order."""

    def __init__(self, candidates: list[ExternalCandidate]) -> None:
        # First occurrence wins, matching dedup_candidates downstream.
        seen: set[str] = set()
        self._candidates: list[ExternalCandidate] = []
        for candidate in candidates:
            if candidate.at_uri in seen:
                continue
            seen.add(candidate.at_uri)
            self._candidates.append(candidate)

    @property
    def name(self) -> str:
        return EXTERNAL_GENERATOR_NAME

    @property
    def supplied_count(self) -> int:
        return len(self._candidates)

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        # max_age_hours is deliberately not applied: anything still in the
        # recent-posts index is recent enough, and the caller chose these.
        excluded = set(exclude_uris or [])
        wanted = [c for c in self._candidates if c.at_uri not in excluded]
        if not wanted:
            return CandidateResult(
                generator_name=self.name, candidates=[], status="empty", reason="all_excluded"
            )

        uris = [c.at_uri for c in wanted]
        async with timed(logger, "es_supplied_candidates", n_uris=len(uris)):
            resp = await es.search(
                index=SUPPLIED_CANDIDATES_INDEX,
                op="supplied",
                query={"terms": {"at_uri": uris}},
                size=len(uris),
                _source=CANDIDATE_SOURCE_FIELDS,
            )
        found = {
            post.at_uri: post
            for post in candidate_posts_from_es_response(resp, generator_name=self.name)
            if post.at_uri
        }

        posts = []
        for candidate in wanted:
            post = found.get(candidate.at_uri)
            if post is None:
                continue
            if video_only and not post.contains_video:
                continue
            # A terms query's _score is meaningless; the caller's score is the
            # generator score for this post.
            posts.append(post.model_copy(update={"score": candidate.score}))
        posts = posts[:num_candidates]

        if len(found) < len(wanted):
            logger.info(
                "Supplied candidates not in index",
                extra={
                    "generator_name": self.name,
                    "supplied": len(wanted),
                    "found": len(found),
                },
            )
        if not posts:
            reason = "not_in_index" if not found else "video_only_excluded"
            return CandidateResult(
                generator_name=self.name, candidates=[], status="empty", reason=reason
            )
        return CandidateResult(generator_name=self.name, candidates=posts)

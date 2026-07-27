"""Popularity candidate generator.

Returns recent, popular posts using an Elasticsearch ``function_score``
query that combines:

* A **recency decay** (Gaussian on ``created_at``) so newer posts are
  boosted relative to older ones.
* A **like-count boost** (scripted ``log1p`` on ``like_count``) so posts
  with more likes rank higher, but the
  effect is sub-linear to avoid mega-viral posts dominating everything.

This produces a single performant query that naturally balances freshness
and engagement without needing multiple time-bucket queries.

Tuning knobs live as module-level constants and can be overridden later
via configuration.
"""

import logging

from ...models import CandidatePost, MaxAgeHours
from ..telemetry import timed
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# ``offset`` — posts within this window of "now" are treated as equally new.
DECAY_OFFSET = "1h"
# ``decay`` — the score at ``scale`` distance from the origin (0–1).
DECAY_FACTOR = 0.5

# Script-score parameters for like_count.
LIKE_FACTOR = 1.5
# log(1 + like_count), clamping bad negative values to avoid NaN.
LIKE_MISSING = 0


def recency_decay_scale(max_age_hours: MaxAgeHours) -> str:
    """Return a decay scale equal to one quarter of the freshness window."""
    scale_minutes = max_age_hours * 15
    if scale_minutes % 60 == 0:
        return f"{scale_minutes // 60}h"
    return f"{scale_minutes}m"


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------


async def popularity_search(
    es,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
    max_age_hours: MaxAgeHours = 168,
) -> list[CandidatePost]:
    """Run a function_score query combining recency and like_count."""

    # Freshness applies to candidate post created_at. Scaling the Gaussian with
    # the hard window keeps wider presets meaningful rather than scoring their
    # oldest eligible posts effectively at zero.
    filters: list[dict] = [
        {"range": {"created_at": {"gte": f"now-{max_age_hours}h"}}},
    ]
    if video_only:
        filters.append({"term": {"contains_video": True}})

    # Exclusions go into the query as must_not so we can fetch exactly
    # num_candidates docs. The previous approach — overfetching
    # num_candidates + len(exclude_uris) and filtering client-side — made
    # fetch sizes balloon to ~2000 as a user's seen-post list grew, which
    # dominated query cost (fetch phase + response payload).
    bool_query: dict = {"filter": filters}
    if exclude_uris:
        bool_query["must_not"] = [{"terms": {"at_uri": exclude_uris}}]

    query = {
        "function_score": {
            "query": {"bool": bool_query},
            "functions": [
                {
                    "gauss": {
                        "created_at": {
                            "origin": "now",
                            "scale": recency_decay_scale(max_age_hours),
                            "offset": DECAY_OFFSET,
                            "decay": DECAY_FACTOR,
                        }
                    },
                },
                {
                    "script_score": {
                        "script": {
                            "source": (
                                "double likes = params.missing; "
                                "if (!doc['like_count'].empty) { "
                                "likes = doc['like_count'].value; } "
                                "likes = Math.max(likes, 0.0); "
                                "return params.factor * Math.log1p(likes);"
                            ),
                            "params": {
                                "factor": LIKE_FACTOR,
                                "missing": LIKE_MISSING,
                            },
                        },
                    },
                },
            ],
            "score_mode": "multiply",
            "boost_mode": "replace",
        }
    }

    async with timed(logger, "es_popularity", num_candidates=num_candidates):
        resp = await es.search(
            index="posts_recent",
            query=query,
            size=num_candidates,
            _source=CANDIDATE_SOURCE_FIELDS,
        )

    candidates = candidate_posts_from_es_response(resp, generator_name=generator_name)
    if exclude_uris:
        # ES already excluded these; kept as a cheap safety net.
        exclude_set = set(exclude_uris)
        candidates = [c for c in candidates if c.at_uri not in exclude_set]
    return candidates[:num_candidates]


# ---------------------------------------------------------------------------
# Generator class
# ---------------------------------------------------------------------------


class PopularityCandidateGenerator(CandidateGenerator):
    """Returns recent popular posts.

    ``user_did`` is accepted for interface consistency but is not used –
    popularity candidates are the same for every user.
    """

    @property
    def name(self) -> str:
        return "popularity"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        candidates = await popularity_search(
            es,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            max_age_hours=max_age_hours,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)

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

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_posts_from_es_response
from ..telemetry import timed

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# How far back to look for posts (ES date-math expression).
RECENCY_WINDOW = "24h"

# Gaussian decay parameters for created_at.
# ``origin`` is implicitly "now".
# ``scale`` controls how quickly the score falls off — posts older than
#  this lose about half their recency boost.
DECAY_SCALE = "6h"
# ``offset`` — posts within this window of "now" are treated as equally new.
DECAY_OFFSET = "1h"
# ``decay`` — the score at ``scale`` distance from the origin (0–1).
DECAY_FACTOR = 0.5

# Script-score parameters for like_count.
LIKE_FACTOR = 1.5
# log(1 + like_count), clamping bad negative values to avoid NaN.
LIKE_MISSING = 0


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

async def popularity_search(
    es,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Run a function_score query combining recency and like_count."""

    filters: list[dict] = [
        {"range": {"created_at": {"gte": f"now-{RECENCY_WINDOW}"}}},
    ]
    if video_only:
        filters.append({"term": {"contains_video": True}})

    query = {
        "function_score": {
            "query": {
                "bool": {
                    "filter": filters,
                }
            },
            "functions": [
                {
                    "gauss": {
                        "created_at": {
                            "origin": "now",
                            "scale": DECAY_SCALE,
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
                                "if (!doc['like_count'].empty) { likes = doc['like_count'].value; } "
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

    fetch_size = num_candidates + len(exclude_uris or [])

    async with timed(logger, "es_popularity", num_candidates=num_candidates):
        resp = await es.search(
            index="posts",
            query=query,
            size=fetch_size,
            _source=CANDIDATE_SOURCE_FIELDS,
        )

    candidates = candidate_posts_from_es_response(resp, generator_name=generator_name)
    if exclude_uris:
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
    ) -> CandidateResult:
        candidates = await popularity_search(
            es, num_candidates, generator_name=self.name, video_only=video_only,
            exclude_uris=exclude_uris,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)

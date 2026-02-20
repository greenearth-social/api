"""Popularity candidate generator.

Returns recent, popular posts using an Elasticsearch ``function_score``
query that combines:

* A **recency decay** (Gaussian on ``created_at``) so newer posts are
  boosted relative to older ones.
* A **like-count boost** (``field_value_factor`` on ``like_count`` with
  ``log1p`` modifier) so posts with more likes rank higher, but the
  effect is sub-linear to avoid mega-viral posts dominating everything.

This produces a single performant query that naturally balances freshness
and engagement without needing multiple time-bucket queries.

Tuning knobs live as module-level constants and can be overridden later
via configuration.
"""

import logging

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from ..elasticsearch import unwrap_es_response
from ..embeddings import encode_float32_b64

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

# field_value_factor parameters for like_count.
LIKE_FACTOR = 1.5
LIKE_MODIFIER = "log1p"  # log(1 + like_count) — gentle sub-linear boost
LIKE_MISSING = 0  # treat missing like_count as 0


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

async def popularity_search(
    es,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
) -> list[CandidatePost]:
    """Run a function_score query combining recency and like_count."""

    query = {
        "function_score": {
            "query": {
                "bool": {
                    "filter": [
                        f for f in [
                            {"term": {"contains_video": True}} if video_only else None,
                            {"range": {"created_at": {"gte": f"now-{RECENCY_WINDOW}"}}},
                        ] if f is not None
                    ],
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
                    "field_value_factor": {
                        "field": "like_count",
                        "factor": LIKE_FACTOR,
                        "modifier": LIKE_MODIFIER,
                        "missing": LIKE_MISSING,
                    },
                },
            ],
            "score_mode": "multiply",
            "boost_mode": "multiply",
        }
    }

    resp = await es.search(index="posts", query=query, size=num_candidates)
    data = unwrap_es_response(resp)

    candidates: list[CandidatePost] = []
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        embeddings_obj = src.get("embeddings") or {}

        l12 = (
            embeddings_obj.get("all_MiniLM_L12_v2")
            if isinstance(embeddings_obj, dict)
            else None
        )

        encoded = None
        if l12 is not None:
            try:
                encoded = encode_float32_b64(l12)
            except Exception:
                encoded = None

        candidates.append(
            CandidatePost(
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
                score=hit.get("_score"),
                generator_name=generator_name,
            )
        )
    return candidates


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
    ) -> CandidateResult:
        candidates = await popularity_search(
            es, num_candidates, generator_name=self.name, video_only=video_only,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)

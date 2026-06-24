"""Ranking framework for the recommendation system.

Provides an abstraction for named rankers that can be called internally or via
the `/rank` API.
"""

from ...models import RankPredictRequest, RankPredictResult
from .base import (
    Ranker,
    RankerError,
    RankerExecutionError,
    RankerResult,
    get_ranker,
    list_rankers,
    register_ranker,
)
from .predict import (
    RankModelNotFoundError,
    run_predict,
)
from .candidate_score import CandidateScoreRanker
from .perspective import PerspectiveRanker
from .two_tower import TwoTowerRanker
from .heavy_ranker import HeavyRanker

_candidate_score = CandidateScoreRanker()
register_ranker(_candidate_score)

_two_tower = TwoTowerRanker()
register_ranker(_two_tower)

_perspective = PerspectiveRanker()
register_ranker(_perspective)

_heavy_ranker = HeavyRanker()
register_ranker(_heavy_ranker)

__all__ = [
    "CandidateScoreRanker",
    "PerspectiveRanker",
    "TwoTowerRanker",
    "HeavyRanker",
    "Ranker",
    "RankerError",
    "RankerExecutionError",
    "RankerResult",
    "RankModelNotFoundError",
    "RankPredictRequest",
    "RankPredictResult",
    "get_ranker",
    "list_rankers",
    "register_ranker",
    "run_predict",
]

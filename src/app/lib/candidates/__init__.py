"""Candidate generation framework for the recommendation system.

Provides an abstraction for named candidate generators that can be called
internally (as a pipeline step) or via an API endpoint.
"""

from .base import (
    CandidateGenerator,
    CandidateResult,
    get_generator,
    list_generators,
    register_generator,
)
from ...models import (
    CandidateGenerateRequest,
    CandidateGenerateResult,
    GeneratorSpec,
)
from .generate import (
    GeneratorError,
    GeneratorNotFoundError,
    run_generate,
)
from .popularity import PopularityCandidateGenerator
from .random_posts import RandomPostsCandidateGenerator
from .followed_users import FollowedUsersCandidateGenerator
from .network_likes import NetworkLikesCandidateGenerator
from .two_tower import TwoTowerCandidateGenerator

# Register built-in generators
_popularity = PopularityCandidateGenerator()
register_generator(_popularity)

_random_posts = RandomPostsCandidateGenerator()
register_generator(_random_posts)

_followed_users = FollowedUsersCandidateGenerator()
register_generator(_followed_users)

_network_likes = NetworkLikesCandidateGenerator()
register_generator(_network_likes)

_two_tower = TwoTowerCandidateGenerator(
    name="two_tower",
    history_mode="actual",
)
register_generator(_two_tower)

_two_tower_empty_history = TwoTowerCandidateGenerator(
    name="two_tower_empty_history",
    history_mode="empty",
)
register_generator(_two_tower_empty_history)

__all__ = [
    "CandidateGenerator",
    "CandidateGenerateRequest",
    "CandidateGenerateResult",
    "CandidateResult",
    "GeneratorError",
    "GeneratorNotFoundError",
    "GeneratorSpec",
    "get_generator",
    "list_generators",
    "register_generator",
    "run_generate",
    "PopularityCandidateGenerator",
    "RandomPostsCandidateGenerator",
    "FollowedUsersCandidateGenerator",
    "NetworkLikesCandidateGenerator",
    "TwoTowerCandidateGenerator",
]

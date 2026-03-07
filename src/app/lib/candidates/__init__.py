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
from .post_similarity import PostSimilarityCandidateGenerator
from .random_posts import RandomPostsCandidateGenerator

# Register built-in generators
_post_similarity = PostSimilarityCandidateGenerator()
register_generator(_post_similarity)

_popularity = PopularityCandidateGenerator()
register_generator(_popularity)

_random_posts = RandomPostsCandidateGenerator()
register_generator(_random_posts)

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
    "PostSimilarityCandidateGenerator",
    "RandomPostsCandidateGenerator",
]

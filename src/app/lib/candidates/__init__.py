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
from .post_similarity import PostSimilarityCandidateGenerator

# Register built-in generators
_post_similarity = PostSimilarityCandidateGenerator()
register_generator(_post_similarity)

__all__ = [
    "CandidateGenerator",
    "CandidateResult",
    "get_generator",
    "list_generators",
    "register_generator",
    "PostSimilarityCandidateGenerator",
]

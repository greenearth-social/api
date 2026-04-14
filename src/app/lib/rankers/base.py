"""Base abstraction for candidate rankers.

Each ranker has a unique name and an async `predict` method that returns a
`RankerResult`. Rankers are registered in a global registry so they can be
looked up by name from the API layer or composed with other internal logic.
"""

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from ...models import RankPredictResult, CandidatePost


class RankerResult(BaseModel):
    """The output of a ranker invocation."""

    model: str = Field(..., description="Name of the ranker that produced this result")
    result: RankPredictResult = Field(..., description="Ordered ranking output")


class RankerError(Exception):
    """Raised when ranking cannot be completed for a valid request."""


class RankerExecutionError(Exception):
    """Raised when a ranker fails while executing."""

    def __init__(self, name: str, message: str):
        self.name = name
        self.message = message
        super().__init__(f"Ranker '{name}' failed: {message}")


class Ranker(ABC):
    """Abstract base class for named rankers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this ranker."""
        ...

    @abstractmethod
    async def predict(
        self, 
        es,
        user_did: str,
        candidates: list[CandidatePost]
    ) -> RankerResult:
        """Rank the supplied candidates."""
        ...


_rankers: dict[str, Ranker] = {}


def register_ranker(ranker: Ranker) -> None:
    """Register a ranker instance by name."""
    _rankers[ranker.name] = ranker


def get_ranker(name: str) -> Ranker | None:
    """Look up a registered ranker by name."""
    return _rankers.get(name)


def list_rankers() -> list[str]:
    """Return all registered ranker instances."""
    return list(_rankers.keys())

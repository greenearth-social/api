"""Base abstraction for candidate generators.

Each generator has a unique name and an async `generate` method that returns
a `CandidateResult` containing scored candidates.  Generators are registered
in a global registry so they can be looked up by name from the API layer or
composed with other generators.
"""

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Candidate(BaseModel):
    """A single candidate post returned by a generator."""

    at_uri: str | None = Field(None, description="AT URI of the candidate post")
    content: str | None = Field(None, description="Post text content")
    minilm_l12_embedding: str | None = Field(
        None, description="Base64-encoded float32 MiniLM L12 embedding (384-d)"
    )
    score: float | None = Field(
        None, description="Relevance score assigned by the generator (if available)"
    )


class CandidateResult(BaseModel):
    """The output of a candidate generator invocation."""

    generator_name: str = Field(..., description="Name of the generator that produced these candidates")
    candidates: list[Candidate] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class CandidateGenerator(ABC):
    """Abstract base class for named candidate generators.

    Subclasses must implement `name` (property) and `generate`.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name identifying this generator (e.g. ``post_similarity``)."""
        ...

    @abstractmethod
    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
    ) -> CandidateResult:
        """Produce candidate posts for the given user.

        Parameters
        ----------
        es:
            An ``AsyncElasticsearch`` client instance.
        user_did:
            The AT Protocol DID of the requesting user.
        num_candidates:
            Maximum number of candidates to return.

        Returns
        -------
        CandidateResult
        """
        ...


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_generators: dict[str, CandidateGenerator] = {}


def register_generator(gen: CandidateGenerator) -> None:
    """Register a generator instance by its name."""
    _generators[gen.name] = gen


def get_generator(name: str) -> CandidateGenerator | None:
    """Look up a registered generator by name.  Returns ``None`` if not found."""
    return _generators.get(name)


def list_generators() -> list[str]:
    """Return the names of all registered generators."""
    return list(_generators.keys())

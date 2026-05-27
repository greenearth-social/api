import base64
from typing import Any

from pydantic import BaseModel, Field


class FeedCursor(BaseModel):
    """Opaque pagination cursor for scrolling through feed results.

    Serialised as base64-encoded JSON in the ``cursor`` field of XRPC
    feed-skeleton responses.  The ``v`` field enables forward-compatible
    format evolution.
    """

    id: str = Field(..., description="Cache key for the stored result set")
    offset: int = Field(..., ge=0, description="Next position in the cached result list")
    v: int = Field(default=1, description="Cursor format version")

    def encode(self) -> str:
        """Serialise to a URL-safe, opaque string."""
        return base64.urlsafe_b64encode(
            self.model_dump_json().encode()
        ).decode()

    @classmethod
    def decode(cls, raw: str) -> "FeedCursor":
        """Deserialise from the opaque string produced by :meth:`encode`.

        Raises ``ValueError`` on any decoding or validation failure.
        """
        try:
            payload = base64.urlsafe_b64decode(raw.encode())
            return cls.model_validate_json(payload)
        except Exception as exc:
            raise ValueError(f"Invalid cursor: {exc}") from exc


class CandidatePost(BaseModel):
    """A post returned by search or candidate generation."""

    at_uri: str | None = Field(
        None, description="The AT URI of the post (e.g. at://...)")
    content: str | None = Field(None, description="The post text content")
    minilm_l12_embedding: str | None = Field(
        None, description="Base64-encoded float32 MiniLM L12 embedding (384-d)"
    )
    score: float | None = Field(
        None, description="Relevance score (e.g. from ES or a model)"
    )
    generator_name: str | None = Field(
        None, description="Name of the candidate generator that produced this post"
    )
    author_did: str | None = Field(
        None, description="AT Protocol DID of the post author"
    )


class GeneratorSpec(BaseModel):
    """Specifies a generator and the proportion of candidates it should supply."""

    name: str = Field(..., description="Name of the candidate generator")
    weight: float = Field(
        1.0, gt=0, description="Relative weight — proportional share of total candidates"
    )


class CandidateGenerateRequest(BaseModel):
    """Describes a candidate-generation job.

    Used as the POST body for ``/candidates/generate`` and constructed
    internally by other endpoints (e.g. XRPC feed skeleton).
    """

    generators: list[GeneratorSpec] = Field(
        ...,
        min_length=1,
        description="List of generators with relative weights",
    )
    user_did: str = Field(..., description="AT Protocol DID of the user")
    num_candidates: int = Field(100, ge=1, le=1000, description="Total candidates to return")
    video_only: bool = Field(False, description="When true, only return posts containing video")
    exclude_uris: list[str] = Field(
        default_factory=list,
        description=(
            "AT URIs to exclude from results (e.g. posts already shown to "
            "the user in previous pages)."
        ),
    )
    infill: str | None = Field(
        None,
        description=(
            "Generator used to fill remaining slots when the primary "
            "generators return fewer candidates than requested. "
            "If omitted, no infill is performed."
        ),
    )


class CandidateGenerateResult(BaseModel):
    """The output of a generation pipeline run."""

    candidates: list[CandidatePost] = Field(
        default_factory=list,
        description="De-duplicated candidate posts in interleaved generator order.",
    )


class RankPredictRequest(BaseModel):
    """Describes a ranking job over a set of candidate posts."""

    candidates: list[CandidatePost] = Field(
        ...,
        description="Candidates to rank in the same shape returned by /candidates/generate",
    )
    model: str | None = Field(
        None,
        description="Optional ranking model identifier. Defaults to the service default.",
    )
    user_did: str = Field(
        ...,
        description="AT Protocol DID of the user being ranked for",
    )


class RankedCandidate(BaseModel):
    """A single ranked candidate and any metadata produced during ranking."""

    at_uri: str = Field(..., description="AT URI of the ranked post")
    rank: int = Field(..., ge=1, description="1-based rank of the post")
    rank_score: float | None = Field(None, description="Ranking score when available")


class RankPredictResult(BaseModel):
    """The ordered output of a ranking pipeline run."""

    rankings: list[RankedCandidate] = Field(
        default_factory=list,
        description="Per-candidate ranking data in ranked order",
    )


class FeedConfig(BaseModel):
    """Configuration for a single published feed.

    ``gen_request_template`` holds the generator pipeline spec using the same
    shape as ``CandidateGenerateRequest``.  Session-specific fields
    (``user_did``, ``num_candidates``) are filled in at request time.

    ``rank_request_template`` optionally holds a ranking spec.  When set,
    candidates are ranked by the named model before URIs are returned.
    Runtime fields (``candidates``, ``user_did``) are filled via ``model_copy``.

    ``diversify`` controls whether MMR reranking is applied after candidate
    generation and optional model ranking.  Defaults to ``True``.
    """

    display_name: str = Field(..., max_length=19)
    description: str = ""
    public: bool = Field(False)
    internal_rkey: str
    internal_display_name: str
    gen_request_template: CandidateGenerateRequest
    rank_request_template: RankPredictRequest | None = Field(
        None,
        description="When set, candidates are ranked by this model before being returned.",
    )
    diversify: bool = Field(True, description="When False, MMR reranking is skipped.")

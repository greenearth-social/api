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
        default=None, description="The AT URI of the post (e.g. at://...)")
    content: str | None = Field(default=None, description="The post text content")
    minilm_l12_embedding: str | None = Field(
        default=None, description="Base64-encoded float32 MiniLM L12 embedding (384-d)"
    )
    score: float | None = Field(
        default=None, description="Relevance score (e.g. from ES or a model)"
    )
    generator_name: str | None = Field(
        default=None, description="Name of the candidate generator that produced this post"
    )
    author_did: str | None = Field(
        default=None, description="AT Protocol DID of the post author"
    )
    author_username: str | None = Field(
        default=None,
        description="AT Protocol handle of the post author (resolved from author_did; "
        "not stored in Elasticsearch, populated lazily where needed)",
    )
    contains_images: bool | None = Field(
        default=None, description="Whether the post embeds one or more images"
    )
    contains_video: bool | None = Field(
        default=None, description="Whether the post embeds video"
    )
    image_count: int | None = Field(
        default=None, description="Number of images embedded in the post"
    )
    video_count: int | None = Field(
        default=None, description="Number of videos embedded in the post"
    )
    external_uri: str | None = Field(
        default=None, description="URI of an external link embed, when present"
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


class RankModelSpec(BaseModel):
    """Specifies a rank model and its relative weight in score combination."""

    name: str = Field(..., description="Name of the registered ranker")
    weight: float = Field(
        1.0, gt=0, description="Relative weight — proportional influence on the combined score"
    )


class RankPredictRequest(BaseModel):
    """Describes a ranking job over a set of candidate posts."""

    candidates: list[CandidatePost] = Field(
        ...,
        description="Candidates to rank in the same shape returned by /candidates/generate",
    )
    models: list[RankModelSpec] = Field(
        ...,
        min_length=1,
        description=(
            "Rank models to run and combine. Each model's scores are normalized "
            "to [-1, 1] using its theoretical bounds, then combined via a "
            "weighted average using the configured relative weights."
        ),
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
    candidates are ranked by the configured ``models`` (each normalized and
    combined via weighted average) before URIs are returned.  Whether
    Perspective API scoring participates is controlled by including a
    ``perspective`` entry in ``models`` — there is no separate toggle.
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
    accepts_interactions: bool = Field(
        True,
        description="When True, the published record declares acceptsInteractions so the "
        "AppView forwards interaction signals to sendInteractions.",
    )
    exclude_seen_posts: bool = Field(
        True,
        description="When True, posts the user has already seen (reported via "
        "interactionSeen) are excluded from generation, and seen post URIs are "
        "denormalized onto the user record. When False, neither happens (the raw "
        "interactions are still stored).",
    )
    pinned_post_uri: str | None = Field(
        None,
        description="AT URI of a post to pin at the top of the first page of this feed.",
    )
    max_render_share: float | None = Field(
        None,
        gt=0.0,
        le=1.0,
        description="Cap on the share of retrieved candidates that may be rendered "
        "(e.g. 0.5 → at most 50% of the retrieved candidates are returned per slate). "
        "None disables the cap.",
    )
    min_rank_score: float | None = Field(
        None,
        description="Combined rank-score floor in [-1, 1]. Candidates scoring below it "
        "are cut from the slate and recorded as discarded so future generation "
        "excludes them. Only applies when rank_request_template is set. None disables.",
    )
    min_mmr_score: float | None = Field(
        None,
        description="MMR per-pick penalized-score floor. The slate is cut at the first "
        "pick scoring below it. MMR relevance is normalized per slate, so this "
        "threshold is slate-relative rather than an absolute quality bar. Only "
        "applies when diversify is True. None disables.",
    )
    avatar: str | None = Field(
        None,
        description="Path to avatar image relative to repo root (e.g. 'assets/icons/your-feed.png'). "
        "Used by publish_feed.py at publish time; not read at runtime.",
    )

"""Pydantic models for Firestore documents.

Each model represents a document type in a Firestore collection.  Models
provide validation on read/write and a consistent schema across the codebase.

Convention:
    - Model names end with ``Document`` (e.g. ``UserDocument``).
    - The Firestore collection name is derived by lower-casing the prefix
      and pluralising (e.g. ``UserDocument`` → ``users``).
    - ``created_at`` / ``updated_at`` are present on every document.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from .lib.candidates.base import CandidateResult
from .models import CandidateGenerateRequest, CandidatePost, RankPredictResult


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UserDocument(BaseModel):
    """A registered feed user.

    The document ID in Firestore is the user's DID (``user_did``).
    """

    user_did: str = Field(..., description="AT Protocol DID of the user (also the document ID)")
    username: str | None = Field(
        default=None,
        description="AT Protocol handle (e.g. foobar.bsky.app)",
    )
    created_at: datetime = Field(
        default_factory=_utcnow, description="When the user was first seen"
    )
    updated_at: datetime = Field(
        default_factory=_utcnow, description="Last time the document was modified"
    )
    last_seen_at: datetime = Field(
        default_factory=_utcnow, description="Most recent feed request from this user"
    )
    debug_feeds: bool = Field(
        default=False,
        description="When True, feed loads for this user capture pipeline debugging "
        "information into the feed_debug subcollection (has a perf cost).",
    )
    social_radius: int = Field(
        default=2,
        ge=0,
        le=4,
        description="Social radius preference: 0=friends only, 2=balanced, 4=everyone.  "
        "Used to override the generator weights in your-feed.",
    )
    freshness: int = Field(
        default=2,
        ge=0,
        le=5,
        description="Freshness preference: 0=6h, 1=12h, 2=24h, 3=48h, 4=72h, 5=7d.  "
        "Used to filter posts by age.",
    )
    politics: float = Field(
        default=1.0,
        ge=0.5,
        le=1.5,
        description="Politics multiplier: 0.5-1.5.  "
        "Applied to political content scores.",
    )
    purpose: float = Field(
        default=0.5,
        ge=0.2,
        le=0.8,
        description="Purpose preference: 0.2=engaging, 0.5=balanced, 0.8=constructive.  "
        "Used to weight engaging vs constructive content.",
    )


class FeedCacheDocument(BaseModel):
    """Cached feed result set used by cursor pagination.

    The document ID is an opaque cache key generated per feed request.
    """

    items: list[str] = Field(default_factory=list, description="Cached AT URI list")
    expires_at: datetime = Field(..., description="UTC expiration timestamp for this cache entry")
    items_meta: list["PipelineItemMeta"] = Field(default_factory=list)
    generator_diagnostics: list["GeneratorDiagnostic"] = Field(default_factory=list)
    applied_social_radius: int | None = None
    feed_name: str | None = None
    generated_at: datetime | None = None


class SeenPostsDocument(BaseModel):
    """Post URIs a user has seen on a given UTC day.

    One document per user per day under the ``seen_posts`` subcollection; the
    document ID is the ``YYYY-MM-DD`` date.  ``expires_at`` anchors the native
    Firestore TTL policy so buckets self-delete ~5 days after the day they cover.
    """

    post_uris: list[str] = Field(default_factory=list, description="Seen post AT URIs for this day")
    expires_at: datetime = Field(..., description="UTC expiration timestamp; drives native TTL")


class FeedActivityDocument(BaseModel):
    feed_name: str = Field(..., description="AT Protocol rkey of the feed (also the document ID)")
    first_seen_at: datetime = Field(
        default_factory=_utcnow, description="When the user first loaded this feed"
    )
    last_seen_at: datetime = Field(
        default_factory=_utcnow, description="Most recent time the user loaded this feed"
    )


class ApiKeyDocument(BaseModel):
    """An issued API key stored in the ``api_keys`` collection.

    The document ID in Firestore is ``key_id``.
    The plaintext key is never stored — only the SHA-256 hash.
    """

    key_id: str = Field(..., description="8 hex chars; also the Firestore document ID")
    key_hash: str = Field(..., description="SHA-256(full_key.encode()) as hex")
    email: str = Field(..., description="Owner email address")
    is_active: bool = Field(default=True, description="Whether this API key is valid and usable")
    created_at: datetime = Field(default_factory=_utcnow, description="When the key was created")
    last_used_at: datetime = Field(
        default_factory=_utcnow, description="Last time this key was used for an API request"
    )
    monthly_call_count: int = Field(
        default=0, description="Number of API calls made this billing month"
    )
    monthly_period: str = Field(
        default="", description="YYYY-MM of the current call counters; resets each month"
    )


class InteractionDocument(BaseModel):
    """A single user interaction reported via ``app.bsky.feed.sendInteractions``.

    Stored as an auto-ID document in the top-level ``interactions`` collection —
    one document per interaction event.  ``created_at`` is the server receive
    time and is the anchor for any future TTL/expiry policy.
    """

    user_did: str = Field(
        ..., description="AT Protocol DID of the user (from the signed feedContext)"
    )
    item_uri: str | None = Field(
        default=None, description="AT URI of the post the interaction relates to"
    )
    event: str = Field(
        ..., description="Interaction event name, e.g. 'interactionLike' (defs# prefix stripped)"
    )
    feed_name: str = Field(..., description="Feed rkey the interaction originated from")
    request_id: str = Field(
        ..., description="Request id of the feed response (also the feed-cache key)"
    )
    feed_generated_at: datetime | None = Field(
        default=None, description="When the feed response was served (from the feedContext iat)"
    )
    created_at: datetime = Field(
        default_factory=_utcnow, description="When the interaction was received"
    )


class FeedDebugUserFeatures(BaseModel):
    """Inputs used to assemble a user-side representation during a feed load.

    Captured per pipeline stage that builds user features (e.g. the
    ``post_similarity`` search vector and the two-tower user embedding) so we
    can see which liked posts drove the result.
    """

    source: str = Field(
        ..., description="Pipeline stage that built these features, e.g. 'two_tower'"
    )
    liked_post_uris: list[str] = Field(
        default_factory=list, description="Liked-post AT URIs used as user history"
    )
    num_embeddings: int = Field(
        default=0, description="How many of the liked posts had usable embeddings"
    )


class FeedDebugScoreEntry(BaseModel):
    """A single candidate's score, keyed by AT URI."""

    at_uri: str = Field(..., description="AT URI of the post")
    score: float = Field(..., description="Score for this post")


class FeedDebugModelScoreEntry(BaseModel):
    """One rank model's contribution to the combined ranking.

    Captures the model's normalized (to [-1, 1]) per-candidate scores and its
    configured relative weight — i.e. the inputs to the weighted-average
    combination — so the combined score in ``ranking`` can be explained.
    The final combined score is intentionally *not* duplicated here.
    """

    model_name: str = Field(..., description="Name of the rank model, e.g. 'two_tower'")
    weight: float = Field(..., description="Configured relative weight for this model")
    scores: list[FeedDebugScoreEntry] = Field(
        default_factory=list,
        description="Per-candidate scores after normalization to [-1, 1]",
    )


class FeedDebugDiversificationEntry(BaseModel):
    """Per-item diversification breakdown, in final selection order.

    Explains why an item's final position differs from its ranked order: the
    diversification penalty is split into the portion driven by same-author
    similarity vs. embedding (content) similarity to already-selected items.
    Fields are algorithm-agnostic (currently produced by MMR reranking).
    """

    at_uri: str = Field(..., description="AT URI of the post")
    relevance: float = Field(
        ..., description="Normalized relevance entering diversification (0..1)"
    )
    score: float = Field(
        ..., description="Score the item was selected on (relevance minus diversity penalties)"
    )
    author_penalty: float = Field(
        default=0.0, description="Penalty from same-author similarity to selected items"
    )
    content_penalty: float = Field(
        default=0.0, description="Penalty from embedding (content) similarity to selected items"
    )


class FeedDebugDocument(BaseModel):
    """Captured debugging information for a single feed load.

    Stored at ``users/{user_did}/feed_debug/{request_id}``.  This is a thin
    container around the real pipeline objects (``CandidateGenerateRequest``,
    ``CandidateResult``, ``RankPredictResult``, ``CandidatePost``) so it keeps
    capturing new fields as the ranking pipeline evolves.  Embeddings are
    stripped and post content is truncated before storage.

    The per-item "why this item?" view is assembled at display time by joining
    ``generator_outputs``, ``ranking``, and ``final_order`` on ``at_uri``.
    """

    request_id: str = Field(
        ..., description="Feed-cache key / feedContext id (also the document ID)"
    )
    user_did: str = Field(..., description="AT Protocol DID of the user the feed was served to")
    username: str | None = Field(default=None, description="Resolved handle of the user")
    feed_name: str = Field(..., description="Feed rkey that was loaded")
    regenerated: bool = Field(
        default=False,
        description="True when this capture came from the cursor-regeneration path rather than a fresh load",
    )

    # Inputs
    generate_request: CandidateGenerateRequest = Field(
        ..., description="The candidate-generation request used"
    )
    ranker_model: str | None = Field(default=None, description="Ranking model applied, if any")
    diversify: bool = Field(default=False, description="Whether diversification was applied")
    user_features: list[FeedDebugUserFeatures] = Field(
        default_factory=list, description="User-side feature inputs captured per stage"
    )

    # Stage outputs (reused pipeline types)
    generator_outputs: list[CandidateResult] = Field(
        default_factory=list,
        description="Raw per-generator output (embeddings stripped, content truncated)",
    )
    final_candidates: list[CandidatePost] = Field(
        default_factory=list,
        description="Deduped candidate set that entered ranking (embeddings stripped)",
    )
    ranking: RankPredictResult | None = Field(
        default=None, description="Ranker output, when a ranker ran"
    )
    model_scores: list[FeedDebugModelScoreEntry] = Field(
        default_factory=list,
        description="Per-model normalized ([-1, 1]) scores and configured weight, "
        "in the order rank models ran (empty when no ranking ran)",
    )
    order_after_rank: list[str] = Field(
        default_factory=list, description="AT URIs in order after ranking, before diversification"
    )
    final_order: list[str] = Field(
        default_factory=list, description="AT URIs in final served order, after diversification"
    )
    diversification: list[FeedDebugDiversificationEntry] = Field(
        default_factory=list,
        description="Per-item diversification breakdown in final order (empty when diversify was off)",
    )

    created_at: datetime = Field(
        default_factory=_utcnow, description="When this debug record was written"
    )
    generated_at: datetime = Field(default_factory=_utcnow, description="When the feed was served")
    expires_at: datetime = Field(..., description="UTC expiration timestamp; drives native TTL")


# ---------------------------------------------------------------------------
# Feed snapshot — lightweight pipeline metadata stored for every feed load
# ---------------------------------------------------------------------------


class DiversificationMeta(BaseModel):
    """Per-item diversification breakdown (lightweight, no content)."""

    relevance: float
    score: float
    author_penalty: float = 0.0
    content_penalty: float = 0.0


class GeneratorMeta(BaseModel):
    """Generator contribution for a single item or the feed legend."""

    name: str
    weight: float = 1.0
    score: float | None = None


class GeneratorDiagnostic(BaseModel):
    """Snapshot-level outcome for one configured candidate generator."""

    name: str
    weight: float
    requested_count: int
    returned_count: int
    contributed_count: int = 0
    status: str = "success"
    reason: str | None = None
    mode: str = "primary"


class ModelScoreMeta(BaseModel):
    """One rank model's normalized score for a single item."""

    name: str
    weight: float
    score: float


class PipelineItemMeta(BaseModel):
    """Per-URI pipeline metadata — already joined so readers don't need to."""

    at_uri: str
    rank: int | None = None
    rank_score: float | None = None
    after_rank_position: int | None = None
    generators: list[GeneratorMeta] = Field(default_factory=list)
    model_scores: list[ModelScoreMeta] = Field(default_factory=list)
    diversification: DiversificationMeta | None = None


class FeedSnapshotDocument(BaseModel):
    """Lightweight pipeline metadata stored for every feed load.

    Written to ``users/{user_did}/feed_snapshots/{request_id}`` in a background
    task so the transparency API can re-render any served feed
    regardless of whether ``debug_feeds`` is enabled.

    Separate from :class:`FeedDebugDocument` — that captures full
    pipeline objects (content, author info, user features) for the CLI
    debug tool and is only written for debug-flagged users.
    """

    request_id: str = Field(..., description="Feed-cache key / feedContext id (also the document ID)")
    items: list[str] = Field(default_factory=list, description="AT URIs in final served order")
    feed_name: str
    generated_at: datetime
    expires_at: datetime = Field(..., description="UTC expiration; drives native TTL")
    ranker_model: str | None = None
    diversify: bool = False
    generator_legend: list[GeneratorMeta] = Field(default_factory=list)
    generator_diagnostics: list[GeneratorDiagnostic] = Field(default_factory=list)
    applied_social_radius: int | None = None
    items_meta: list[PipelineItemMeta] = Field(default_factory=list)

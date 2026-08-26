"""Pydantic response models for the feed-transparency API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# GET /api/feeds
# ---------------------------------------------------------------------------


class FeedSummary(BaseModel):
    request_id: str
    generated_at: datetime
    feed_name: str
    api_release_sha: str | None = None
    applied_social_radius: int | None = None
    generator_diagnostics: list[GeneratorDiagnosticView] = Field(default_factory=list)


class FeedListResponse(BaseModel):
    feeds: list[FeedSummary]


class GeneratorDiagnosticView(BaseModel):
    name: str
    weight: float
    requested_count: int
    returned_count: int
    contributed_count: int
    status: str
    reason: str | None = None
    mode: str = "primary"


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id}
# ---------------------------------------------------------------------------


class AuthorView(BaseModel):
    handle: str | None = None
    display_name: str | None = None
    avatar_url: str | None = None


class GeneratorView(BaseModel):
    name: str
    score: float | None = None


class ModelScoreView(BaseModel):
    name: str
    weight: float
    score: float


class DiversificationView(BaseModel):
    relevance: float
    score: float
    author_penalty: float = 0.0
    content_penalty: float = 0.0


class MediaView(BaseModel):
    image_urls: list[str] = Field(default_factory=list)
    video_url: str | None = None
    link_card_url: str | None = None
    link_card_title: str | None = None
    link_card_description: str | None = None
    labels: list[str] = Field(default_factory=list)


class EngagementView(BaseModel):
    reply_count: int = 0
    repost_count: int = 0
    like_count: int = 0


class FeedItemView(BaseModel):
    at_uri: str
    rank: int | None = None
    rank_score: float | None = None
    after_rank_position: int | None = None
    author: AuthorView | None = None
    created_at: datetime | None = None
    content: str | None = None
    generators: list[GeneratorView] = Field(default_factory=list)
    model_scores: list[ModelScoreView] = Field(default_factory=list)
    diversification: DiversificationView | None = None
    media: MediaView | None = None
    engagement: EngagementView | None = None
    post_url: str | None = None


class FeedDetailResponse(BaseModel):
    request_id: str
    generated_at: datetime
    api_release_sha: str | None = None
    items: list[FeedItemView]
    stored_item_count: int = 0
    displayed_item_count: int = 0
    publicly_filtered_count: int = 0
    unavailable_count: int = 0


class FeedPreviewResponse(BaseModel):
    request_id: str
    feed_name: str
    generated_at: datetime
    expires_at: datetime


# ---------------------------------------------------------------------------
# GET /api/feeds/preferences and PATCH /api/feeds/preferences/{feed_name}
# ---------------------------------------------------------------------------


class SourceWeights(BaseModel):
    model_config = {"extra": "forbid"}

    following: float = Field(ge=0.0, le=1.0)
    network_likes: float = Field(default=0.0, ge=0.0, le=1.0)
    authors_topics: float = Field(ge=0.0, le=1.0)
    popular: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def weights_sum_to_one(self) -> SourceWeights:
        total = self.following + self.network_likes + self.authors_topics + self.popular
        if abs(total - 1.0) > 1e-6:
            raise ValueError("source weights must sum to 1.0")
        return self


class FeedPreferences(BaseModel):
    model_config = {"extra": "forbid"}

    source_weights: SourceWeights | None = None
    freshness: int | None = Field(default=None, ge=0, le=5)
    politics: float | None = Field(default=None, ge=0.5, le=1.5)
    purpose: float | None = Field(default=None, ge=0.2, le=0.8)


class AcceptFeedPreviewRequest(BaseModel):
    model_config = {"extra": "forbid"}

    preferences: FeedPreferences
    displayed_item_uris: list[str] = Field(max_length=200)


class AcceptedFeedPreviewResponse(BaseModel):
    request_id: str
    preferences: FeedPreferences
    accepted_until: datetime | None = None


class PreferencesResponse(BaseModel):
    feeds: dict[str, FeedPreferences]

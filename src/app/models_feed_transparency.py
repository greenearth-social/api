"""Pydantic response models for the feed-transparency API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# GET /api/feeds
# ---------------------------------------------------------------------------


class FeedSummary(BaseModel):
    request_id: str
    generated_at: datetime
    feed_name: str
    applied_social_radius: int | None = None
    generator_diagnostics: list["GeneratorDiagnosticView"] = Field(default_factory=list)


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
    items: list[FeedItemView]
    stored_item_count: int = 0
    displayed_item_count: int = 0
    publicly_filtered_count: int = 0
    unavailable_count: int = 0


# ---------------------------------------------------------------------------
# GET / PUT /api/feeds/preferences
# ---------------------------------------------------------------------------


class Preferences(BaseModel):
    model_config = {"extra": "forbid"}

    social_radius: int = Field(default=3, ge=0, le=4)
    freshness: int = Field(default=2, ge=0, le=5)
    politics: float = Field(default=1.0, ge=0.5, le=1.5)
    purpose: float = Field(default=0.5, ge=0.2, le=0.8)

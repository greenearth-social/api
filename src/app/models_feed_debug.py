"""Pydantic response models for the feed-debug transparency API.

All models use camelCase JSON serialisation via ``CamelModel`` so the API
responses match TypeScript conventions without the frontend needing a
separate snake_case→camelCase mapping layer.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


def _to_camel(s: str) -> str:
    parts = s.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


class CamelModel(BaseModel):
    model_config = {"alias_generator": _to_camel, "populate_by_name": True}


# ---------------------------------------------------------------------------
# GET /api/feeds
# ---------------------------------------------------------------------------


class FeedSummary(CamelModel):
    request_id: str
    generated_at: datetime
    feed_name: str


class FeedListResponse(CamelModel):
    feeds: list[FeedSummary]


# ---------------------------------------------------------------------------
# GET /api/feeds/{requestId}
# ---------------------------------------------------------------------------


class AuthorView(CamelModel):
    handle: str | None = None
    display_name: str | None = None
    avatar_url: str | None = None


class GeneratorView(CamelModel):
    name: str
    score: float | None = None


class ModelScoreView(CamelModel):
    name: str
    weight: float
    score: float


class DiversificationView(CamelModel):
    relevance: float
    score: float
    author_penalty: float = 0.0
    content_penalty: float = 0.0


class MediaView(CamelModel):
    image_urls: list[str] = Field(default_factory=list)
    video_url: str | None = None
    link_card_url: str | None = None
    link_card_title: str | None = None
    link_card_description: str | None = None
    labels: list[str] = Field(default_factory=list)


class EngagementView(CamelModel):
    reply_count: int = 0
    repost_count: int = 0
    like_count: int = 0


class FeedItemView(CamelModel):
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


class FeedDetailResponse(CamelModel):
    request_id: str
    generated_at: datetime
    items: list[FeedItemView]


# ---------------------------------------------------------------------------
# GET / PUT /api/feeds/preferences
# ---------------------------------------------------------------------------


class SocialRadiusPreference(CamelModel):
    social_radius: int = Field(default=2, ge=0, le=4)

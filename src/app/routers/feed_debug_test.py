"""Tests for feed-debug transparency API endpoints."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..documents import (
    DiversificationMeta,
    FeedSnapshotDocument,
    GeneratorMeta,
    ModelScoreMeta,
    PipelineItemMeta,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> Generator[TestClient]:
    """Client with Firebase auth bypassed and Firestore mocked on app state."""
    from ..lib.firebase_auth import verify_firebase_auth

    app.dependency_overrides[verify_firebase_auth] = lambda: "test-user"
    app.state.firestore = MagicMock()
    app.state.id_resolver = MagicMock()
    client = TestClient(app)
    yield client
    app.dependency_overrides.pop(verify_firebase_auth, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot_doc(
    request_id: str = "req-abc",
    feed_name: str = "your-feed",
    generated_at: datetime | None = None,
    items_meta: list[PipelineItemMeta] | None = None,
    generator_legend: list[GeneratorMeta] | None = None,
    ranker_model: str | None = "two_tower",
    diversify: bool = True,
    **overrides,
) -> FeedSnapshotDocument:
    now = generated_at or datetime(2026, 7, 12, 15, 30, tzinfo=timezone.utc)
    meta = items_meta or [
        PipelineItemMeta(
            at_uri="at://did:plc:author/app.bsky.feed.post/post1",
            rank=1,
            rank_score=0.92,
            after_rank_position=1,
            generators=[GeneratorMeta(name="two_tower", score=0.85)],
            model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.92)],
            diversification=DiversificationMeta(
                relevance=0.95, score=0.80, author_penalty=0.0, content_penalty=0.0
            ),
        )
    ]
    legend = generator_legend or [GeneratorMeta(name="two_tower", weight=1.0)]

    defaults = dict(
        request_id=request_id,
        items=[m.at_uri for m in meta],
        feed_name=feed_name,
        generated_at=now,
        expires_at=now + timedelta(minutes=15),
        ranker_model=ranker_model,
        diversify=diversify,
        generator_legend=legend,
        items_meta=meta,
    )
    defaults.update(overrides)
    return FeedSnapshotDocument(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# GET /api/feeds
# ---------------------------------------------------------------------------


@patch("app.routers.feed_debug.get_recent_feed_snapshots")
def test_list_feeds_returns_summaries(mock_query, client):
    mock_query.return_value = [
        _snapshot_doc(request_id="req-1", generated_at=datetime.now(timezone.utc)),
        _snapshot_doc(
            request_id="req-2", generated_at=datetime.now(timezone.utc) - timedelta(minutes=5)
        ),
    ]

    response = client.get("/api/feeds")
    assert response.status_code == 200
    data = response.json()
    assert len(data["feeds"]) == 2
    assert data["feeds"][0]["requestId"] == "req-1"
    assert data["feeds"][0]["feedName"] == "your-feed"


@patch("app.routers.feed_debug.get_recent_feed_snapshots")
def test_list_feeds_empty(mock_query, client):
    mock_query.return_value = []

    response = client.get("/api/feeds")
    assert response.status_code == 200
    assert response.json()["feeds"] == []


def test_list_feeds_returns_401_without_auth():
    from ..lib.firebase_auth import verify_firebase_auth

    app.dependency_overrides.pop(verify_firebase_auth, None)
    try:
        client = TestClient(app)
        response = client.get("/api/feeds")
        assert response.status_code == 401
    finally:
        app.dependency_overrides.setdefault(verify_firebase_auth, lambda: "test-user")


# ---------------------------------------------------------------------------
# GET /api/feeds/{requestId}
# ---------------------------------------------------------------------------


@patch("app.routers.feed_debug.hydrate_posts")
@patch("app.routers.feed_debug.get_feed_snapshot")
def test_get_feed_detail_returns_merged_data(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc()
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {
                "handle": "alice.bsky.social",
                "display_name": "Alice Chen",
                "avatar_url": "https://cdn.bsky.app/avatar.jpg",
            },
            "content": "Hello world",
            "created_at": datetime(2026, 7, 12, 10, 0, tzinfo=timezone.utc),
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": ["1 image"],
            },
            "engagement": {"reply_count": 3, "repost_count": 12, "like_count": 47},
        }
    }

    response = client.get("/api/feeds/req-abc")
    assert response.status_code == 200
    data = response.json()
    assert data["requestId"] == "req-abc"
    assert len(data["items"]) == 1

    item = data["items"][0]
    assert item["atUri"] == uri
    assert item["rank"] == 1
    assert item["rankScore"] == 0.92
    assert item["author"]["handle"] == "alice.bsky.social"
    assert item["author"]["displayName"] == "Alice Chen"
    assert item["content"] == "Hello world"
    assert item["postUrl"] == "https://bsky.app/profile/alice.bsky.social/post/post1"
    assert item["engagement"]["replyCount"] == 3
    assert len(item["generators"]) == 1
    assert item["generators"][0]["name"] == "two_tower"
    assert item["generators"][0]["score"] == 0.85
    assert len(item["modelScores"]) == 1
    assert item["modelScores"][0]["name"] == "two_tower"
    assert item["modelScores"][0]["score"] == 0.92
    assert item["diversification"]["relevance"] == 0.95


@patch("app.routers.feed_debug.get_feed_snapshot")
def test_get_feed_detail_not_found(mock_get_snapshot, client):
    mock_get_snapshot.return_value = None

    response = client.get("/api/feeds/nonexistent")
    assert response.status_code == 404


@patch("app.routers.feed_debug.hydrate_posts")
@patch("app.routers.feed_debug.get_feed_snapshot")
def test_get_feed_detail_camel_case_keys(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc()
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "alice.bsky.social", "display_name": None, "avatar_url": None},
            "content": "",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    for key in data:
        assert "_" not in key, f"Top-level key {key} has snake_case"

    item = data["items"][0]
    assert "atUri" in item
    assert "rankScore" in item
    assert "afterRankPosition" in item
    assert "modelScores" in item
    assert "postUrl" in item


@patch("app.routers.feed_debug.hydrate_posts")
@patch("app.routers.feed_debug.get_feed_snapshot")
def test_get_feed_detail_diversification_null_when_absent(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc(
        diversify=False,
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                rank=1,
                rank_score=None,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[],
                diversification=None,
            )
        ],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "x", "display_name": None, "avatar_url": None},
            "content": "",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()
    assert data["items"][0]["diversification"] is None


@patch("app.routers.feed_debug.hydrate_posts")
@patch("app.routers.feed_debug.get_feed_snapshot")
def test_get_feed_detail_multiple_items(mock_get_snapshot, mock_hydrate, client):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"

    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2,
                rank=2,
                rank_score=0.88,
                after_rank_position=2,
                generators=[GeneratorMeta(name="two_tower", score=0.80)],
                model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri1: {
            "author": {"handle": "alice.bsky.social", "display_name": None, "avatar_url": None},
            "content": "first",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
        uri2: {
            "author": {"handle": "bob.bsky.social", "display_name": None, "avatar_url": None},
            "content": "second",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()
    assert len(data["items"]) == 2
    assert data["items"][0]["atUri"] == uri1
    assert data["items"][0]["content"] == "first"
    assert data["items"][1]["atUri"] == uri2
    assert data["items"][1]["content"] == "second"


# ---------------------------------------------------------------------------
# _at_uri_to_bsky_url
# ---------------------------------------------------------------------------


def test_at_uri_to_bsky_url():
    from .feed_debug import _at_uri_to_bsky_url

    assert (
        _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.post/post1")
        == "https://bsky.app/profile/did:plc:abc/post/post1"
    )
    assert (
        _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.post/post1", "alice.bsky.social")
        == "https://bsky.app/profile/alice.bsky.social/post/post1"
    )
    assert _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.like/xyz") is None
    assert _at_uri_to_bsky_url("not-a-uri") is None

"""Tests for feed-transparency API endpoints."""

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


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_returns_summaries(mock_query, client):
    mock_query.return_value = [
        _snapshot_doc(
            request_id="req-1",
            generated_at=datetime.now(timezone.utc),
            items=["at://a"],
            items_meta=[PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1)],
        ),
        _snapshot_doc(
            request_id="req-2",
            generated_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            items=["at://b"],
            items_meta=[PipelineItemMeta(at_uri="at://b", rank=1, rank_score=1.0, after_rank_position=1)],
        ),
    ]

    response = client.get("/api/feeds")
    assert response.status_code == 200
    data = response.json()
    assert len(data["feeds"]) == 2
    assert data["feeds"][0]["request_id"] == "req-1"
    assert data["feeds"][0]["feed_name"] == "your-feed"


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_covers_every_feed_not_one_hardcoded_name(mock_query, client):
    """Snapshots are returned whatever feed produced them.

    Each summary carries its own feed_name, so choosing which to surface is the
    client's call. Filtering server-side would also reintroduce the
    (feed_name, generated_at) composite index this query no longer needs.
    """
    now = datetime.now(timezone.utc)
    mock_query.return_value = [
        _snapshot_doc(request_id="req-1", generated_at=now, items=["at://a"], feed_name="your-feed"),
        _snapshot_doc(
            request_id="req-2",
            generated_at=now - timedelta(minutes=1),
            items=["at://b"],
            feed_name="popularity",
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert [f["feed_name"] for f in response.json()["feeds"]] == ["your-feed", "popularity"]
    # No feed_name filter reaches the query.
    assert "feed_name" not in mock_query.call_args.kwargs


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
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


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_preserves_overlapping_snapshots(mock_query, client):
    now = datetime.now(timezone.utc)
    newer = _snapshot_doc(
        request_id="req-1",
        generated_at=now,
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    older = _snapshot_doc(
        request_id="req-2",
        generated_at=now - timedelta(minutes=5),
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    mock_query.return_value = [newer, older]

    response = client.get("/api/feeds")
    data = response.json()
    assert len(data["feeds"]) == 2
    assert data["feeds"][0]["request_id"] == "req-1"
    assert data["feeds"][1]["request_id"] == "req-2"


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_newest_first_order(mock_query, client):
    now = datetime.now(timezone.utc)
    newest = _snapshot_doc(
        request_id="req-3", generated_at=now,
        items=["at://c"],
        items_meta=[PipelineItemMeta(at_uri="at://c", rank=1, rank_score=1.0, after_rank_position=1)],
    )
    middle = _snapshot_doc(
        request_id="req-2", generated_at=now - timedelta(minutes=3),
        items=["at://b"],
        items_meta=[PipelineItemMeta(at_uri="at://b", rank=1, rank_score=1.0, after_rank_position=1)],
    )
    oldest = _snapshot_doc(
        request_id="req-1", generated_at=now - timedelta(minutes=6),
        items=["at://a"],
        items_meta=[PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1)],
    )
    mock_query.return_value = [newest, middle, oldest]

    response = client.get("/api/feeds")
    data = response.json()
    assert len(data["feeds"]) == 3
    assert data["feeds"][0]["request_id"] == "req-3"
    assert data["feeds"][1]["request_id"] == "req-2"
    assert data["feeds"][2]["request_id"] == "req-1"


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_preserves_fully_overlapping_middle_snapshot(mock_query, client):
    now = datetime.now(timezone.utc)
    newest = _snapshot_doc(
        request_id="req-3", generated_at=now,
        items=["at://a", "at://b", "at://c"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
            PipelineItemMeta(at_uri="at://c", rank=3, rank_score=0.8, after_rank_position=3),
        ],
    )
    middle = _snapshot_doc(
        request_id="req-2", generated_at=now - timedelta(minutes=3),
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    oldest = _snapshot_doc(
        request_id="req-1", generated_at=now - timedelta(minutes=6),
        items=["at://d"],
        items_meta=[PipelineItemMeta(at_uri="at://d", rank=1, rank_score=1.0, after_rank_position=1)],
    )
    mock_query.return_value = [newest, middle, oldest]

    response = client.get("/api/feeds")
    data = response.json()
    assert len(data["feeds"]) == 3
    assert data["feeds"][0]["request_id"] == "req-3"
    assert data["feeds"][1]["request_id"] == "req-2"
    assert data["feeds"][2]["request_id"] == "req-1"


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id}
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
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
    assert data["request_id"] == "req-abc"
    assert len(data["items"]) == 1

    item = data["items"][0]
    assert item["at_uri"] == uri
    assert item["rank"] == 1
    assert item["rank_score"] == 0.92
    assert item["author"]["handle"] == "alice.bsky.social"
    assert item["author"]["display_name"] == "Alice Chen"
    assert item["content"] == "Hello world"
    assert item["post_url"] == "https://bsky.app/profile/alice.bsky.social/post/post1"
    assert item["engagement"]["reply_count"] == 3
    assert len(item["generators"]) == 1
    assert item["generators"][0]["name"] == "two_tower"
    assert item["generators"][0]["score"] == 0.85
    assert len(item["model_scores"]) == 1
    assert item["model_scores"][0]["name"] == "two_tower"
    assert item["model_scores"][0]["score"] == 0.92
    assert item["diversification"]["relevance"] == 0.95


@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_not_found(mock_get_snapshot, client):
    mock_get_snapshot.return_value = None

    response = client.get("/api/feeds/nonexistent")
    assert response.status_code == 404


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_uses_snake_case_keys(mock_get_snapshot, mock_hydrate, client):
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

    assert "request_id" in data
    assert "requestId" not in data
    item = data["items"][0]
    assert "at_uri" in item
    assert "rank_score" in item
    assert "after_rank_position" in item
    assert "model_scores" in item
    assert "post_url" in item
    assert "atUri" not in item


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
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


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
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
    assert data["items"][0]["at_uri"] == uri1
    assert data["items"][0]["content"] == "first"
    assert data["items"][1]["at_uri"] == uri2
    assert data["items"][1]["content"] == "second"


# ---------------------------------------------------------------------------
# GET / PUT /api/feeds/preferences
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.get_user")
def test_get_preferences_returns_default_for_new_user(mock_get_user, client):
    from ..documents import UserDocument

    mock_get_user.return_value = UserDocument(
        user_did="did:plc:test-user",
        username="test.bsky.social",
    )

    response = client.get("/api/feeds/preferences")
    assert response.status_code == 200
    data = response.json()
    assert data["social_radius"] == 3  # default
    assert data["freshness"] == 2  # default
    assert data["politics"] == 1.0  # default
    assert data["purpose"] == 0.5  # default


@patch("app.routers.feed_transparency.get_user")
def test_get_preferences_returns_stored_value(mock_get_user, client):
    from ..documents import UserDocument

    mock_get_user.return_value = UserDocument(
        user_did="did:plc:test-user",
        username="test.bsky.social",
        social_radius=0,
        freshness=3,
        politics=1.25,
        purpose=0.65,
    )

    response = client.get("/api/feeds/preferences")
    assert response.status_code == 200
    data = response.json()
    assert data["social_radius"] == 0
    assert data["freshness"] == 3
    assert data["politics"] == 1.25
    assert data["purpose"] == 0.65


@patch("app.routers.feed_transparency.set_user_preferences")
def test_put_preferences_updates_value(mock_set_prefs, client):
    response = client.put(
        "/api/feeds/preferences",
        json={
            "social_radius": 3,
            "freshness": 4,
            "politics": 1.5,
            "purpose": 0.8,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["social_radius"] == 3
    assert data["freshness"] == 4
    assert data["politics"] == 1.5
    assert data["purpose"] == 0.8
    mock_set_prefs.assert_awaited_once()


@patch("app.routers.feed_transparency.set_user_preferences")
def test_put_preferences_rejects_out_of_range(mock_set_prefs, client):
    response = client.put(
        "/api/feeds/preferences",
        json={
            "social_radius": 10,
            "freshness": 2,
            "politics": 1.0,
            "purpose": 0.5,
        },
    )
    assert response.status_code == 422


@patch("app.routers.feed_transparency.set_user_preferences")
def test_put_preferences_rejects_camel_case_body(mock_set_prefs, client):
    response = client.put(
        "/api/feeds/preferences",
        json={
            "socialRadius": 3,
            "freshness": 2,
            "politics": 1.0,
            "purpose": 0.5,
        },
    )

    assert response.status_code == 422
    mock_set_prefs.assert_not_awaited()


@patch("app.routers.feed_transparency.set_user_preferences")
def test_put_preferences_creates_user_doc_if_missing(mock_set_prefs, client):
    response = client.put(
        "/api/feeds/preferences",
        json={
            "social_radius": 1,
            "freshness": 2,
            "politics": 1.0,
            "purpose": 0.5,
        },
    )
    assert response.status_code == 200
    assert response.json()["social_radius"] == 1
    mock_set_prefs.assert_awaited_once()


# ---------------------------------------------------------------------------
# _at_uri_to_bsky_url
# ---------------------------------------------------------------------------


def test_at_uri_to_bsky_url():
    from .feed_transparency import _at_uri_to_bsky_url

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


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id} — deduplication across newer snapshots
# ---------------------------------------------------------------------------


def _hydrated(uri: str, handle: str = "alice.bsky.social") -> dict:
    return {
        uri: {
            "author": {"handle": handle, "display_name": None, "avatar_url": None},
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


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_preserves_items_seen_in_newer_snapshots(
    mock_get_snapshot, mock_hydrate, client
):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1, rank=1, rank_score=0.92, after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2, rank=2, rank_score=0.88, after_rank_position=2,
                generators=[GeneratorMeta(name="popularity", score=0.80)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {**_hydrated(uri2, "bob.bsky.social")}

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert len(data["items"]) == 1
    assert [item["at_uri"] for item in data["items"]] == [uri2]


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_hides_unavailable_posts_but_preserves_valid_order(
    mock_get_snapshot, mock_hydrate, client
):
    unavailable = "at://did:plc:a/app.bsky.feed.post/deleted"
    first = "at://did:plc:b/app.bsky.feed.post/p1"
    second = "at://did:plc:c/app.bsky.feed.post/p2"
    mock_get_snapshot.return_value = _snapshot_doc(
        items=[unavailable, first, second],
        items_meta=[
            PipelineItemMeta(at_uri=unavailable),
            PipelineItemMeta(at_uri=first),
            PipelineItemMeta(at_uri=second),
        ],
    )
    mock_hydrate.return_value = {
        unavailable: {
            "author": {"handle": None, "display_name": None, "avatar_url": None},
            "content": None,
            "created_at": None,
            "media": {"image_urls": [], "labels": []},
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
        **_hydrated(first, "first.bsky.social"),
        **_hydrated(second, "second.bsky.social"),
    }

    response = client.get("/api/feeds/req-abc")

    assert response.status_code == 200
    assert [item["at_uri"] for item in response.json()["items"]] == [first, second]
    assert response.json()["stored_item_count"] == 3
    assert response.json()["displayed_item_count"] == 2
    assert response.json()["publicly_filtered_count"] == 0
    assert response.json()["unavailable_count"] == 1


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_filters_public_post_and_author_labels(
    mock_get_snapshot, mock_hydrate, client
):
    safe = "at://did:plc:a/app.bsky.feed.post/safe"
    labeled_post = "at://did:plc:b/app.bsky.feed.post/labeled"
    labeled_author = "at://did:plc:c/app.bsky.feed.post/author-labeled"
    mock_get_snapshot.return_value = _snapshot_doc(
        items=[safe, labeled_post, labeled_author],
        items_meta=[PipelineItemMeta(at_uri=uri) for uri in [safe, labeled_post, labeled_author]],
    )
    hydrated = {
        **_hydrated(safe, "safe.bsky.social"),
        **_hydrated(labeled_post, "post.bsky.social"),
        **_hydrated(labeled_author, "author.bsky.social"),
    }
    hydrated[safe]["moderation"] = {"post_labels": [], "author_labels": []}
    hydrated[labeled_post]["moderation"] = {
        "post_labels": ["graphic-media"], "author_labels": []
    }
    hydrated[labeled_author]["moderation"] = {
        "post_labels": [], "author_labels": ["porn"]
    }
    mock_hydrate.return_value = hydrated

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert [item["at_uri"] for item in data["items"]] == [safe]
    assert data["stored_item_count"] == 3
    assert data["displayed_item_count"] == 1
    assert data["publicly_filtered_count"] == 2
    assert data["unavailable_count"] == 0


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_returns_all_when_no_newer_snapshots(
    mock_get_snapshot, mock_hydrate, client
):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1, rank=1, rank_score=0.92, after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2, rank=2, rank_score=0.88, after_rank_position=2,
                generators=[GeneratorMeta(name="popularity", score=0.80)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {**_hydrated(uri1), **_hydrated(uri2, "bob.bsky.social")}

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert len(data["items"]) == 2


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id} — diverse pipeline metadata
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_diverse_pipeline_metadata(
    mock_get_snapshot, mock_hydrate, client
):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[
                    GeneratorMeta(name="two_tower", score=0.85),
                    GeneratorMeta(name="followed_users", score=0.70),
                    GeneratorMeta(name="popularity", score=0.60),
                ],
                model_scores=[
                    ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92),
                    ModelScoreMeta(name="perspective", weight=1.0, score=0.425),
                ],
            )
        ],
        generator_legend=[
            GeneratorMeta(name="two_tower", weight=0.35),
            GeneratorMeta(name="followed_users", weight=0.35),
            GeneratorMeta(name="popularity", weight=0.3),
        ],
        ranker_model="heavy_ranker, perspective",
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "alice.bsky.social", "display_name": "Alice", "avatar_url": None},
            "content": "hello",
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

    assert response.status_code == 200
    item = data["items"][0]

    gen_names = [g["name"] for g in item["generators"]]
    assert gen_names == ["two_tower", "followed_users", "popularity"]

    model_names = [m["name"] for m in item["model_scores"]]
    assert model_names == ["heavy_ranker", "perspective"]
    assert item["model_scores"][0]["weight"] == 1.0
    assert item["model_scores"][0]["score"] == 0.92
    assert item["model_scores"][1]["weight"] == 1.0
    assert item["model_scores"][1]["score"] == 0.425

"""Tests for post hydration — Bluesky API calls + Firestore cache."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

async def _async_iter(items):
    for item in items:
        yield item


from .post_hydration import (
    _empty_hydration,
    _fetch_posts_batch,
    _parse_bsky_post,
    _post_rkey,
    cache_hydrated_posts,
    get_cached_hydrated_posts,
    hydrate_posts,
)


# ---------------------------------------------------------------------------
# _post_rkey
# ---------------------------------------------------------------------------


def test_post_rkey_is_deterministic():
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    assert _post_rkey(uri) == _post_rkey(uri)


def test_post_rkey_differs_for_different_uris():
    a = _post_rkey("at://did:plc:a/app.bsky.feed.post/p1")
    b = _post_rkey("at://did:plc:b/app.bsky.feed.post/p2")
    assert a != b


# ---------------------------------------------------------------------------
# _parse_bsky_post
# ---------------------------------------------------------------------------


def _post(**overrides):
    base = {
        "uri": "at://did:plc:author/app.bsky.feed.post/post1",
        "author": {
            "handle": "alice.bsky.social",
            "displayName": "Alice Chen",
            "avatar": "https://cdn.bsky.app/img/avatar.jpg",
        },
        "record": {
            "text": "Hello world",
            "createdAt": "2026-07-12T10:00:00.000Z",
        },
        "replyCount": 3,
        "repostCount": 12,
        "likeCount": 47,
    }
    # Allow overrides to replace nested dicts as well.
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = {**base[key], **value}
        else:
            base[key] = value
    return base


def test_parse_basic_post():
    uri, data = _parse_bsky_post(_post())
    assert uri == "at://did:plc:author/app.bsky.feed.post/post1"
    assert data["author"]["handle"] == "alice.bsky.social"
    assert data["author"]["display_name"] == "Alice Chen"
    assert data["author"]["avatar_url"] == "https://cdn.bsky.app/img/avatar.jpg"
    assert data["content"] == "Hello world"
    assert data["created_at"] == datetime(2026, 7, 12, 10, 0, tzinfo=timezone.utc)
    assert data["engagement"]["reply_count"] == 3
    assert data["engagement"]["repost_count"] == 12
    assert data["engagement"]["like_count"] == 47
    assert data["moderation"] == {"post_labels": [], "author_labels": []}


def test_parse_public_moderation_labels_separately_from_media_labels():
    _uri, data = _parse_bsky_post(
        _post(
            labels=[{"src": "did:plc:labeler", "val": "graphic-media"}],
            author={"labels": [{"src": "did:plc:labeler", "val": "porn"}]},
            embed={"images": [{"fullsize": "https://cdn.bsky.app/img/x.jpg"}]},
        )
    )

    assert data["moderation"] == {
        "post_labels": ["graphic-media"],
        "author_labels": ["porn"],
    }
    assert data["media"]["labels"] == ["1 image"]


def test_parse_post_with_images():
    uri, data = _parse_bsky_post(
        _post(
            embed={
                "images": [
                    {"fullsize": "https://cdn.bsky.app/img/a.jpg", "thumb": ""},
                    {"fullsize": "https://cdn.bsky.app/img/b.jpg", "thumb": ""},
                ]
            }
        )
    )
    assert data["media"]["image_urls"] == [
        "https://cdn.bsky.app/img/a.jpg",
        "https://cdn.bsky.app/img/b.jpg",
    ]
    assert data["media"]["labels"] == ["2 images"]


def test_parse_post_with_single_image():
    uri, data = _parse_bsky_post(
        _post(embed={"images": [{"fullsize": "https://cdn.bsky.app/img/x.jpg"}]})
    )
    assert data["media"]["labels"] == ["1 image"]


def test_parse_post_with_external_link():
    uri, data = _parse_bsky_post(
        _post(
            embed={
                "external": {
                    "uri": "https://example.com",
                    "title": "Example",
                    "description": "A great link",
                    "thumb": "https://cdn.bsky.app/img/thumb.jpg",
                }
            }
        )
    )
    assert data["media"]["link_card_url"] == "https://example.com"
    assert data["media"]["link_card_title"] == "Example"
    assert data["media"]["link_card_description"] == "A great link"
    assert data["media"]["image_urls"] == ["https://cdn.bsky.app/img/thumb.jpg"]
    assert data["media"]["labels"] == ["link"]


def test_parse_post_with_video():
    uri, data = _parse_bsky_post(
        _post(
            embed={
                "$type": "app.bsky.embed.video",
                "playlist": "https://video.bsky.app/hls/playlist.m3u8",
            }
        )
    )
    assert data["media"]["video_url"] == "https://video.bsky.app/hls/playlist.m3u8"
    assert data["media"]["labels"] == ["video"]


def test_parse_post_missing_author():
    uri, data = _parse_bsky_post({"uri": "at://a/app.bsky.feed.post/p1", "record": {}, "author": {}})
    assert data["author"]["handle"] is None
    assert data["author"]["display_name"] is None
    assert data["author"]["avatar_url"] is None


def test_parse_post_missing_engagement():
    uri, data = _parse_bsky_post({"uri": "at://a/app.bsky.feed.post/p1", "record": {}, "author": {}})
    assert data["engagement"]["reply_count"] == 0
    assert data["engagement"]["repost_count"] == 0
    assert data["engagement"]["like_count"] == 0


def test_parse_post_invalid_created_at():
    uri, data = _parse_bsky_post(
        _post(record={"text": "x", "createdAt": "not-a-date"})
    )
    assert data["created_at"] is None


# ---------------------------------------------------------------------------
# _empty_hydration
# ---------------------------------------------------------------------------


def test_empty_hydration_has_all_keys():
    empty = _empty_hydration()
    assert "author" in empty
    assert "content" in empty
    assert "created_at" in empty
    assert "media" in empty
    assert "engagement" in empty
    assert empty["author"]["handle"] is None


# ---------------------------------------------------------------------------
# _fetch_posts_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_posts_batch_returns_posts():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "posts": [_post(record={"text": "fetched post"})]
    }

    with patch("app.lib.post_hydration.get_http_client") as mock_client:
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        result = await _fetch_posts_batch(["at://a/app.bsky.feed.post/p1"])
        assert len(result) == 1
        assert result[0]["record"]["text"] == "fetched post"


@pytest.mark.asyncio
async def test_fetch_posts_batch_http_error_returns_empty():
    with patch("app.lib.post_hydration.get_http_client") as mock_client:
        mock_client.return_value.get = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "error", request=MagicMock(), response=MagicMock(status_code=500)
            )
        )
        result = await _fetch_posts_batch(["at://a/app.bsky.feed.post/p1"])
        assert result == []


@pytest.mark.asyncio
async def test_fetch_posts_batch_timeout_returns_empty():
    with patch("app.lib.post_hydration.get_http_client") as mock_client:
        mock_client.return_value.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        result = await _fetch_posts_batch(["at://a/app.bsky.feed.post/p1"])
        assert result == []


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_cached_hydrated_posts_hit():
    from .post_hydration import _post_rkey
    db = MagicMock()
    uri = "at://a/app.bsky.feed.post/p1"
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.id = _post_rkey(uri)
    mock_doc.to_dict.return_value = {
        "data": {"author": {"handle": "cached.bsky.social"}},
        "expires_at": datetime(2099, 1, 1, tzinfo=timezone.utc),
        "version": 2,
    }
    db.get_all = MagicMock(return_value=_async_iter([mock_doc]))

    cached, missing = await get_cached_hydrated_posts(db, [uri])
    assert uri in cached
    assert cached[uri]["author"]["handle"] == "cached.bsky.social"
    assert missing == []


@pytest.mark.asyncio
async def test_get_cached_hydrated_posts_expired():
    from .post_hydration import _post_rkey
    db = MagicMock()
    uri = "at://a/app.bsky.feed.post/p1"
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.id = _post_rkey(uri)
    mock_doc.to_dict.return_value = {
        "data": {"author": {"handle": "stale.bsky.social"}},
        "expires_at": datetime(2000, 1, 1, tzinfo=timezone.utc),
        "version": 2,
    }
    db.get_all = MagicMock(return_value=_async_iter([mock_doc]))

    cached, missing = await get_cached_hydrated_posts(db, [uri])
    assert cached == {}
    assert missing == [uri]


@pytest.mark.asyncio
async def test_get_cached_hydrated_posts_reloads_legacy_shape_without_moderation():
    from .post_hydration import _post_rkey
    db = MagicMock()
    uri = "at://a/app.bsky.feed.post/p1"
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.id = _post_rkey(uri)
    mock_doc.to_dict.return_value = {
        "data": {"author": {"handle": "legacy.bsky.social"}},
        "expires_at": datetime(2099, 1, 1, tzinfo=timezone.utc),
    }
    db.get_all = MagicMock(return_value=_async_iter([mock_doc]))

    cached, missing = await get_cached_hydrated_posts(db, [uri])

    assert cached == {}
    assert missing == [uri]


@pytest.mark.asyncio
async def test_get_cached_hydrated_posts_miss():
    from .post_hydration import _post_rkey
    db = MagicMock()
    uri = "at://a/app.bsky.feed.post/p1"
    mock_doc = MagicMock()
    mock_doc.exists = False
    mock_doc.id = _post_rkey(uri)
    db.get_all = MagicMock(return_value=_async_iter([mock_doc]))

    cached, missing = await get_cached_hydrated_posts(db, [uri])
    assert cached == {}
    assert missing == [uri]


@pytest.mark.asyncio
async def test_cache_hydrated_posts_writes():
    db = MagicMock()
    db.collection.return_value.document.return_value.set = AsyncMock()

    posts = {"at://a/app.bsky.feed.post/p1": {"author": {"handle": "new.bsky.social"}}}
    await cache_hydrated_posts(db, posts)

    assert db.collection.called


@pytest.mark.asyncio
async def test_cache_hydrated_posts_catches_errors():
    db = MagicMock()
    db.collection.return_value.document.return_value.set = AsyncMock(side_effect=RuntimeError("fail"))

    # Should not raise.
    await cache_hydrated_posts(db, {"at://a/app.bsky.feed.post/p1": {"author": {}}})


# ---------------------------------------------------------------------------
# hydrate_posts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hydrate_posts_empty_list():
    db = MagicMock()
    result = await hydrate_posts(db, [])
    assert result == {}


@pytest.mark.asyncio
async def test_hydrate_posts_all_cached():
    from .post_hydration import _post_rkey
    db = MagicMock()
    uri = "at://a/app.bsky.feed.post/p1"
    mock_doc = MagicMock()
    mock_doc.exists = True
    mock_doc.id = _post_rkey(uri)
    mock_doc.to_dict.return_value = {
        "data": {"author": {"handle": "cached.bsky.social"}, "content": "cached content"},
        "expires_at": datetime(2099, 1, 1, tzinfo=timezone.utc),
        "version": 2,
    }
    db.get_all = MagicMock(return_value=_async_iter([mock_doc]))

    result = await hydrate_posts(db, [uri])
    assert result[uri]["author"]["handle"] == "cached.bsky.social"


@pytest.mark.asyncio
async def test_hydrate_posts_miss_fetches_and_caches():
    db = MagicMock()
    # Cache miss
    mock_doc = MagicMock()
    mock_doc.exists = False
    db.collection.return_value.document.return_value.get = AsyncMock(return_value=mock_doc)

    # API returns data
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"posts": [_post()]}

    with patch("app.lib.post_hydration.get_http_client") as mock_client:
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        result = await hydrate_posts(db, ["at://did:plc:author/app.bsky.feed.post/post1"])

    assert result["at://did:plc:author/app.bsky.feed.post/post1"]["content"] == "Hello world"
    assert result["at://did:plc:author/app.bsky.feed.post/post1"]["author"]["handle"] == "alice.bsky.social"

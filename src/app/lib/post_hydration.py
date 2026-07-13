"""Bluesky post hydration with a Firestore-backed cache.

``hydrate_posts`` fetches post metadata (author profile, media URLs,
engagement counts) from the Bluesky public API, batching requests and
caching results in Firestore so repeated views of the same debug record
don't re-fetch.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone

import httpx
from google.cloud.firestore import AsyncClient

from .http_client import get_http_client

logger = logging.getLogger(__name__)

BSKY_GET_POSTS_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"
BSKY_BATCH_LIMIT = 25
HYDRATION_CACHE_TTL_HOURS = 1
HYDRATED_POSTS_COLLECTION = "hydrated_posts"

_GET_POSTS_TIMEOUT = httpx.Timeout(5.0, connect=1.0, read=5.0, write=2.0, pool=1.0)


def _post_rkey(at_uri: str) -> str:
    """Deterministic Firestore-safe document ID for an AT URI."""
    return hashlib.sha256(at_uri.encode()).hexdigest()


async def _fetch_posts_batch(uris: list[str]) -> list[dict]:
    """Fetch a batch of posts from the public Bluesky API (max 25 URIs).

    Retries once on transient failures (429, 5xx, timeouts). Returns an
    empty list on failure — the caller falls back to empty data.
    """
    client = get_http_client()
    params = {"uris": uris}

    for attempt in range(2):
        try:
            resp = await client.get(
                BSKY_GET_POSTS_URL,
                params=params,
                timeout=_GET_POSTS_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("posts", []) if isinstance(data, dict) else []
        except httpx.HTTPStatusError as exc:
            if attempt == 0 and (exc.response.status_code == 429 or 500 <= exc.response.status_code < 600):
                await asyncio.sleep(0.5)
                continue
            logger.warning(
                "Bluesky getPosts returned %d for %d URIs",
                exc.response.status_code,
                len(uris),
            )
            return []
        except httpx.TimeoutException:
            if attempt == 0:
                await asyncio.sleep(0.5)
                continue
            logger.warning("Bluesky getPosts timed out for %d URIs", len(uris))
            return []
        except Exception:
            logger.exception("Unexpected error fetching posts from Bluesky")
            return []

    return []


def _parse_bsky_post(post: dict) -> tuple[str, dict]:
    """Parse a single Bluesky ``getPosts`` entry into a flat dict."""
    author_data = post.get("author", {})
    record = post.get("record", {})
    embed = post.get("embed") or {}

    author = {
        "handle": author_data.get("handle"),
        "display_name": author_data.get("displayName"),
        "avatar_url": author_data.get("avatar"),
    }

    image_urls: list[str] = []
    video_url: str | None = None
    link_card_url: str | None = None
    link_card_title: str | None = None
    link_card_description: str | None = None
    labels: list[str] = []

    embed_type = embed.get("$type", "")
    if "images" in embed:
        images = embed.get("images", [])
        for img in images:
            url = img.get("fullsize") or img.get("thumb")
            if url:
                image_urls.append(url)
        if image_urls:
            labels.append(f"{len(image_urls)} image{'s' if len(image_urls) != 1 else ''}")
    elif "external" in embed:
        ext = embed.get("external", {})
        link_card_url = ext.get("uri")
        link_card_title = ext.get("title")
        link_card_description = ext.get("description")
        if ext.get("thumb"):
            image_urls.append(ext["thumb"])
        labels.append("link")
    elif "Playlist" in embed_type or "video" in embed_type.lower():
        video_url = embed.get("playlist")
        labels.append("video")

    engagement = {
        "reply_count": post.get("replyCount", 0) or 0,
        "repost_count": post.get("repostCount", 0) or 0,
        "like_count": post.get("likeCount", 0) or 0,
    }

    content = record.get("text", "")
    created_at = None
    created_at_str = record.get("createdAt")
    if created_at_str:
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return post.get("uri", ""), {
        "author": author,
        "content": content,
        "created_at": created_at,
        "media": {
            "image_urls": image_urls,
            "video_url": video_url,
            "link_card_url": link_card_url,
            "link_card_title": link_card_title,
            "link_card_description": link_card_description,
            "labels": labels,
        },
        "engagement": engagement,
    }


def _empty_hydration() -> dict:
    return {
        "author": {"handle": None, "display_name": None, "avatar_url": None},
        "content": None,
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


async def get_cached_hydrated_posts(
    db: AsyncClient, at_uris: list[str]
) -> tuple[dict[str, dict], list[str]]:
    """Check the hydration cache.  Returns ``(cached_posts, missing_uris)``."""
    now = datetime.now(timezone.utc)
    cached: dict[str, dict] = {}
    missing: list[str] = []

    uri_to_rkey = {uri: _post_rkey(uri) for uri in at_uris}
    refs = [db.collection(HYDRATED_POSTS_COLLECTION).document(rkey) for rkey in uri_to_rkey.values()]

    try:
        docs = [doc async for doc in db.get_all(refs)]
        rkey_to_doc = {doc.id: doc for doc in docs}

        for uri, rkey in uri_to_rkey.items():
            doc = rkey_to_doc.get(rkey)
            if doc and doc.exists:
                data = doc.to_dict()
                if data is not None:
                    expires = data.get("expires_at")
                    if isinstance(expires, datetime) and expires > now:
                        cached[uri] = data["data"]
                        continue
            missing.append(uri)
    except Exception:
        missing = list(at_uris)

    return cached, missing


async def cache_hydrated_posts(db: AsyncClient, posts: dict[str, dict]) -> None:
    """Write hydrated post data to Firestore with a TTL anchor.

    Failures are logged but not surfaced — the cache is a best-effort optimisation.
    """
    expires = datetime.now(timezone.utc) + timedelta(hours=HYDRATION_CACHE_TTL_HOURS)
    batch = db.batch()
    for uri, data in posts.items():
        ref = db.collection(HYDRATED_POSTS_COLLECTION).document(_post_rkey(uri))
        batch.set(ref, {"data": data, "expires_at": expires})
    try:
        await batch.commit()
    except Exception:
        logger.warning("Failed to commit hydration batch")


async def hydrate_posts(db: AsyncClient, at_uris: list[str]) -> dict[str, dict]:
    """Hydrate a list of AT URIs — cache-first, Bluesky API for misses.

    Returns a dict mapping ``at_uri`` → flat dict with ``author``,
    ``content``, ``created_at``, ``media``, and ``engagement`` keys.
    """
    if not at_uris:
        return {}

    # 1. Check cache.
    cached, missing = await get_cached_hydrated_posts(db, at_uris)
    if not missing:
        return cached

    # 2. Fetch from Bluesky in batches.
    fetched: dict[str, dict] = {}
    for i in range(0, len(missing), BSKY_BATCH_LIMIT):
        batch = missing[i : i + BSKY_BATCH_LIMIT]
        try:
            posts = await _fetch_posts_batch(batch)
        except Exception:
            logger.exception("Failed to fetch posts batch (size=%d)", len(batch))
            posts = []
        for post in posts:
            uri, parsed = _parse_bsky_post(post)
            fetched[uri] = parsed

    # 3. Cache fetched results (best-effort).
    if fetched:
        try:
            await cache_hydrated_posts(db, fetched)
        except Exception:
            logger.warning("Failed to cache %d hydrated posts", len(fetched))

    # 4. Merge.  URIs that Bluesky didn't return get empty data.
    result = dict(cached)
    for uri in at_uris:
        if uri in fetched:
            result[uri] = fetched[uri]
        elif uri not in result:
            result[uri] = _empty_hydration()

    return result

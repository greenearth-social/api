"""Formatting helpers shared by the feed CLIs.

``feed_debug.py`` (Firestore debug records) and ``feed_view.py`` (a terminal
feed client) present the same concepts — timestamps, scores, media badges — so
they render them identically from here rather than drifting apart.
"""

from __future__ import annotations

from datetime import datetime, timezone

from rich.text import Text


def relative_time(dt: datetime) -> str:
    """Compact relative-time string for a tz-aware datetime."""
    try:
        delta = datetime.now(timezone.utc) - dt
        secs = delta.total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3_600:
            return f"{int(secs / 60)}m ago"
        if secs < 86_400:
            return f"{int(secs / 3_600)}h ago"
        if delta.days < 30:
            return f"{delta.days}d ago"
        return dt.strftime("%b %d, %Y")
    except Exception:
        return str(dt)


def fmt_score(score: float | None) -> str:
    return f"{score:.4f}" if score is not None else "—"


def media_badges(
    *,
    image_count: int | None = None,
    contains_images: bool = False,
    video_count: int | None = None,
    contains_video: bool = False,
    external_uri: str | None = None,
) -> Text | None:
    """Yellow media badges for a post, or None when it has no media.

    Counts win over the booleans when present: a post carrying
    ``image_count=3`` renders "3 images" rather than a bare "image".
    """
    parts = []
    if image_count:
        parts.append(f"{image_count} image{'s' if image_count != 1 else ''}")
    elif contains_images:
        parts.append("image")
    if video_count:
        parts.append(f"{video_count} video{'s' if video_count != 1 else ''}")
    elif contains_video:
        parts.append("video")
    if external_uri:
        parts.append("link")
    if not parts:
        return None
    return Text(f"[{', '.join(parts)}]", style="dim yellow")

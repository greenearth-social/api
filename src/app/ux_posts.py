"""Registry of repository-managed UX posts injected into feeds.

UX posts are the Bluesky posts we insert into feeds for product reasons rather than
because they were ranked: the SETTINGS pin at the top of every public feed, the
"you must be logged in" explainer, the user-interview survey post. Their *content*
is versioned in ``assets/ux_posts/`` and published to the notifications account by
``scripts/manage_ux_posts.py``; their *URIs* are resolved at deploy time into the manifest
this module reads.

The manifest is generated, not committed (see ``.gitignore``). Keeping it out of the
repo means a pull request contains only content and code — no deployment state to
conflict between concurrent branches, and no credentialed sync before a merge. It
still ships inside the Cloud Run image, so a rolled-back revision keeps the URIs it
was built with.

This module is deliberately free of FastAPI and app-state imports so that ``feeds.py``
and the deployment scripts can both import it.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# The notifications account. UX posts deliberately do not live on the brand account
# (`mysky.social`), whose followers would otherwise see every republished revision
# in their timeline (issue #404).
PUBLISHER_DID = "did:plc:66mudnfk2p4olwpaskmrw2vq"
PUBLISHER_HANDLE = "notify.mysky.social"

# Content lives outside src/ because only the resolved URIs are needed at runtime;
# `.gcloudignore` keeps assets/ out of the deployed image.
CONTENT_DIR = Path(__file__).resolve().parents[2] / "assets" / "ux_posts"
MANIFEST_PATH = Path(__file__).parent / "ux_posts_resolved.json"

MANIFEST_SCHEMA_VERSION = 1

# Bluesky's post length limit, counted in graphemes.
MAX_POST_GRAPHEMES = 300
DEFAULT_SETTINGS_APP_ORIGIN = "https://app.greenearth.social"
SETTINGS_PATH_PREFIX = "/#/settings/"

# Named constants rather than bare strings at call sites: a typo becomes an
# AttributeError at import instead of a silently unresolved post.
PIN_YOUR_FEED = "pin-your-feed.md"
PIN_YOUR_FEED_EXPLORE = "pin-your-feed-explore.md"
PIN_YOUR_FEED_RETURNING = "pin-your-feed-returning.md"
PIN_BEST_OF_FRIENDS = "pin-best-of-friends.md"
PIN_RANDOM = "pin-random.md"
SURVEY_YOUR_FEED = "survey-your-feed.md"
LOGGED_OUT = "logged-out.md"
PLACEHOLDER = "placeholder.md"

MANAGED_POSTS: tuple[str, ...] = (
    LOGGED_OUT,
    PIN_BEST_OF_FRIENDS,
    PIN_RANDOM,
    PIN_YOUR_FEED,
    PIN_YOUR_FEED_EXPLORE,
    PIN_YOUR_FEED_RETURNING,
    PLACEHOLDER,
    SURVEY_YOUR_FEED,
)

# Posts that accept replies. Everything else is a one-way notice and gets a
# threadgate allowing nobody. Quote posts are disabled on every UX post, and likes
# cannot be disabled at all -- atproto has no like-gating.
REPLIES_ALLOWED: frozenset[str] = frozenset({SURVEY_YOUR_FEED})

@dataclass(frozen=True)
class VideoSpec:
    """The native video attached to a managed UX post."""

    filename: str
    alt: str


VIDEO_POSTS: dict[str, VideoSpec] = {
    PIN_YOUR_FEED: VideoSpec(
        filename="pin-your-feed.mp4",
        alt="A demonstration of how to customize the MySky feed.",
    ),
    PIN_YOUR_FEED_EXPLORE: VideoSpec(
        filename="pin-your-feed-explore.mp4",
        alt="A demonstration of how to pin the MySky feed.",
    ),
}

# Optional override, for pinning a URI by hand without a redeploy. Nothing sets this
# in normal operation; the manifest is the usual source.
URI_ENV_VAR = "GE_UX_POST_URIS"


def _load_manifest() -> dict[str, str]:
    """Read the deploy-generated name -> URI mapping.

    Returns an empty mapping when the manifest is absent, which is the normal state
    of a fresh checkout that has not run ``scripts/manage_ux_posts.py resolve``.
    """
    try:
        raw = MANIFEST_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning(
            "No UX post manifest at %s; feeds will fall back to the placeholder. "
            "Run: pipenv run python scripts/manage_ux_posts.py resolve",
            MANIFEST_PATH,
        )
        return {}
    except OSError:
        logger.exception("Could not read the UX post manifest at %s", MANIFEST_PATH)
        return {}

    try:
        data = json.loads(raw)
        posts = data["posts"]
    except (ValueError, KeyError, TypeError):
        logger.exception("Malformed UX post manifest at %s", MANIFEST_PATH)
        return {}

    return {name: uri for name, uri in posts.items() if isinstance(uri, str) and uri}


def _load_env_overrides() -> dict[str, str]:
    """Parse the optional GE_UX_POST_URIS override, tolerating a malformed value."""
    raw = os.environ.get(URI_ENV_VAR, "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError:
        logger.error("%s is not valid JSON; ignoring it", URI_ENV_VAR)
        return {}
    if not isinstance(data, dict):
        logger.error("%s must be a JSON object; ignoring it", URI_ENV_VAR)
        return {}
    return {name: uri for name, uri in data.items() if isinstance(uri, str) and uri}


def _resolve_all() -> dict[str, str]:
    """Build the name -> URI mapping once, at import."""
    resolved = _load_manifest()
    resolved.update(_load_env_overrides())
    return resolved


# Resolved once at import. Content can only change via a deploy, which restarts the
# service, so nothing needs to re-read these files per request.
_URIS: dict[str, str] = _resolve_all()

_missing = [name for name in MANAGED_POSTS if name not in _URIS]
if _missing:
    logger.warning(
        "Unresolved UX posts (%s); they will render as the placeholder post",
        ", ".join(_missing),
    )
logger.info("UX posts resolved: %d of %d", len(MANAGED_POSTS) - len(_missing), len(MANAGED_POSTS))


def ux_post_uri(name: str) -> str | None:
    """Return the published URI for a managed UX post.

    Falls back to the placeholder post when *name* has not been published yet, so a
    newly added post is visible in a local feed as "something belongs here" rather
    than silently missing. Deployment validation refuses to ship an unresolved post,
    so the placeholder is only reachable in development.

    Returns None only when nothing at all has been resolved, i.e. a checkout with no
    manifest.
    """
    uri = _URIS.get(name)
    if uri:
        return uri
    if name != PLACEHOLDER:
        return _URIS.get(PLACEHOLDER)
    return None


def content_path(name: str) -> Path:
    """Return the on-disk content file for a managed post (publish time only)."""
    return CONTENT_DIR / name


def video_path(name: str) -> Path | None:
    """Return the video asset for a managed post, if it has one."""
    spec = VIDEO_POSTS.get(name)
    return CONTENT_DIR / spec.filename if spec else None


def settings_url(feed_name: str) -> str:
    """Build a Settings deep link for the frontend paired with this deployment."""
    redirect_origin = os.environ.get("GE_SETTINGS_LINK_ORIGIN", "").strip().rstrip("/")
    if redirect_origin:
        return f"{redirect_origin}/settings/{feed_name}"
    origin = os.environ.get("GE_SETTINGS_APP_ORIGIN", DEFAULT_SETTINGS_APP_ORIGIN).strip()
    return f"{(origin or DEFAULT_SETTINGS_APP_ORIGIN).rstrip('/')}{SETTINGS_PATH_PREFIX}{feed_name}"


def read_content(name: str) -> str:
    """Read a managed post's markdown source. Not available in the deployed image."""
    content = content_path(name).read_text(encoding="utf-8")
    canonical_prefix = f"{DEFAULT_SETTINGS_APP_ORIGIN}{SETTINGS_PATH_PREFIX}"
    configured_prefix = settings_url("")
    return content.replace(canonical_prefix, configured_prefix)


def resolved_uris() -> dict[str, str]:
    """Return a copy of the resolved mapping, for diagnostics and tests."""
    return dict(_URIS)


def _reset_cache_for_tests() -> None:
    """Re-resolve after a test changes MANIFEST_PATH or the environment."""
    global _URIS
    _URIS = _resolve_all()

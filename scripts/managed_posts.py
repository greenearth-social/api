#!/usr/bin/env python3
"""Shared primitives for building and matching repository-managed Bluesky posts.

Extracted from ``manage_post.py`` and the former ``manage_pinned_posts.py`` so the
UX-post tooling in ``ux_posts.py`` and the manual publishing CLI share one
implementation of markdown parsing, record construction, and content matching.

Content format is plain text with markdown links::

    Hello world. [Click here](https://example.com) for more.
    To show brackets in the display text: [[link]](https://example.com)

Matching is by *signature* — the visible text plus the ordered link targets. Two
records with the same signature are the same post as far as we are concerned, which
is what lets a deployment recognize an already-published post instead of
republishing it.
"""

from __future__ import annotations

import base64
import hashlib
import re
import subprocess
import sys

from atproto import Client, client_utils, models

POST_COLLECTION = "app.bsky.feed.post"
# Gates are separate records keyed by the rkey of the post they govern, so they can
# be applied to an already-published post without changing its URI.
THREADGATE_COLLECTION = "app.bsky.feed.threadgate"
POSTGATE_COLLECTION = "app.bsky.feed.postgate"

LINK_RE = re.compile(r"\[(\[[^\]]+\]|[^\]]+)\]\((https?://[^)]+)\)")


def parse_content(text: str) -> list[dict]:
    """Parse plain text with [label](url) markdown links into segments."""
    segments = []
    last = 0
    for m in LINK_RE.finditer(text):
        if m.start() > last:
            segments.append({"type": "text", "text": text[last : m.start()]})
        segments.append({"type": "link", "text": m.group(1), "url": m.group(2)})
        last = m.end()
    if last < len(text):
        segments.append({"type": "text", "text": text[last:]})
    return segments


def build_text_builder(segments: list[dict]) -> client_utils.TextBuilder:
    """Build a TextBuilder with rich-text facets from parsed content segments."""
    tb = client_utils.TextBuilder()
    for seg in segments:
        if seg["type"] == "text":
            tb.text(seg["text"])
        elif seg["type"] == "link":
            tb.link(seg["text"], seg["url"])
    return tb


def normalize_content(text: str) -> str:
    """Normalize a content file to exactly the bytes we publish."""
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def content_sha(text: str) -> str:
    """Fingerprint normalized post content."""
    return hashlib.sha256(normalize_content(text).encode()).hexdigest()


def blob_cid(data: bytes) -> str:
    """Return the CIDv1/raw/sha256 identifier AT Proto assigns to blob bytes."""
    # CIDv1 + raw codec + sha2-256 multihash code/length + digest. AT Proto blob
    # references use this content-addressed representation, which lets an offline
    # deploy match a checked-in video to an existing post without uploading it.
    cid = b"\x01\x55\x12\x20" + hashlib.sha256(data).digest()
    return "b" + base64.b32encode(cid).decode("ascii").lower().rstrip("=")


def video_blob_cid(record: object) -> str | None:
    """Extract the video blob CID from a post record, if one is embedded."""
    embed = _field(record, "embed")
    if not embed:
        return None
    embed_type = _field(embed, "$type") or _field(embed, "py_type")
    if embed_type != "app.bsky.embed.video":
        return None
    video = _field(embed, "video")
    ref = _field(video, "ref")
    link = _field(ref, "$link") or _field(ref, "link")
    return link if isinstance(link, str) and link else None


def build_post_record(content: str, created_at: str) -> models.AppBskyFeedPost.Record:
    """Convert managed markdown into a Bluesky post record."""
    builder = build_text_builder(parse_content(normalize_content(content)))
    return models.AppBskyFeedPost.Record(
        text=builder.build_text(),
        facets=builder.build_facets(),
        created_at=created_at,
    )


def _field(value: object, name: str, default=None):
    """Read a field from either a parsed model or a raw dict."""
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def post_signature(record: object) -> tuple[str, tuple[str, ...]]:
    """Return visible text and link targets, for exact managed-post matching."""
    links: list[str] = []
    for facet in _field(record, "facets", []) or []:
        for feature in _field(facet, "features", []) or []:
            uri = _field(feature, "uri")
            if isinstance(uri, str) and uri:
                links.append(uri)
    text = _field(record, "text", "")
    return text if isinstance(text, str) else "", tuple(links)


def content_signature(content: str) -> tuple[str, tuple[str, ...]]:
    """Return the signature a given content file would publish as.

    Builds the record the same way ``build_post_record`` does so that comparing
    against a live record's signature is exact.
    """
    builder = build_text_builder(parse_content(normalize_content(content)))
    links = tuple(
        feature.uri
        for facet in (builder.build_facets() or [])
        for feature in (facet.features or [])
        if getattr(feature, "uri", None)
    )
    return builder.build_text(), links


def list_repo_posts(client: Client, repo_did: str) -> list[object]:
    """Read a repository's post records, following pagination."""
    records: list[object] = []
    cursor: str | None = None
    while True:
        response = client.com.atproto.repo.list_records(
            models.ComAtprotoRepoListRecords.Params(
                repo=repo_did,
                collection=POST_COLLECTION,
                limit=100,
                cursor=cursor,
                reverse=True,
            )
        )
        records.extend(response.records)
        cursor = response.cursor
        if not cursor:
            return records


def login(handle: str, password: str) -> Client:
    """Authenticate against Bluesky and return the client."""
    client = Client()
    client.login(handle, password)
    return client


def password_from_secret(project_id: str, secret: str) -> str | None:
    """Read an app password out of Secret Manager, or return None if unavailable."""
    try:
        result = subprocess.run(
            [
                "gcloud",
                "secrets",
                "versions",
                "access",
                "latest",
                f"--secret={secret}",
                f"--project={project_id}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"Could not read secret {secret}: {exc}", file=sys.stderr)
        return None
    return result.stdout.strip() or None


def build_threadgate_record(post_uri: str, created_at: str) -> models.AppBskyFeedThreadgate.Record:
    """Build a threadgate that allows nobody to reply.

    An empty ``allow`` list means "nobody"; omitting the field entirely would mean
    "everybody", so the empty list is load-bearing.
    """
    return models.AppBskyFeedThreadgate.Record(
        post=post_uri,
        allow=[],
        created_at=created_at,
    )


def build_postgate_record(post_uri: str, created_at: str) -> models.AppBskyFeedPostgate.Record:
    """Build a postgate that disallows quote posts."""
    return models.AppBskyFeedPostgate.Record(
        post=post_uri,
        embedding_rules=[models.AppBskyFeedPostgate.DisableRule()],
        created_at=created_at,
    )

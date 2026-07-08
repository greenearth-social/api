#!/usr/bin/env python3
"""Publish or update Bluesky posts with rich text support.

Usage:
    cd api

    # Publish a new post
    pipenv run python scripts/manage_post.py publish \\
        --handle you.bsky.social --file post.txt

    # Update an existing post (preserves AT URI)
    pipenv run python scripts/manage_post.py update "at://did:plc:.../app.bsky.feed.post/3abc" \\
        --handle you.bsky.social --file updated_post.txt

post.txt format — plain text with markdown links:
    Hello world. [Click here](https://example.com) for more.

Reads GE_BSKY_APP_PASSWORD from .env or environment, or prompts.
"""

import argparse
import getpass
import os
import re
import sys

from dotenv import load_dotenv

LINK_RE = re.compile(r'\[([^\]]+)\]\((https?://[^)]+)\)')


def parse_content(text: str) -> list[dict]:
    """Parse plain text with [label](url) markdown links into segments."""
    segments = []
    last = 0
    for m in LINK_RE.finditer(text):
        if m.start() > last:
            segments.append({"type": "text", "text": text[last:m.start()]})
        segments.append({"type": "link", "text": m.group(1), "url": m.group(2)})
        last = m.end()
    if last < len(text):
        segments.append({"type": "text", "text": text[last:]})
    return segments

#!/usr/bin/env python3
"""Publish or update Bluesky posts with rich text support.

Usage:
    cd api

    # Publish a new post
    pipenv run python scripts/manage_post.py publish \\
        --handle you.bsky.social --file post.txt

    # Update an existing post (preserves AT URI)
    pipenv run python scripts/manage_post.py --handle you.bsky.social update "at://did:plc:.../app.bsky.feed.post/3abc" \\
        --file updated_post.txt

post.txt format — plain text with markdown links:
    Hello world. [Click here](https://example.com) for more.

Reads GE_BSKY_APP_PASSWORD from .env or environment, or prompts.
"""

import argparse
import getpass
import os
import re
import sys

from atproto import Client, client_utils
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


def build_text_builder(segments: list[dict]) -> client_utils.TextBuilder:
    """Build a TextBuilder from parsed content segments.

    Converts a list of text and link segments into an atproto TextBuilder
    with proper rich text markup.

    Args:
        segments: List of dicts with type="text" or "link" from parse_content()

    Returns:
        client_utils.TextBuilder with all segments added
    """
    tb = client_utils.TextBuilder()
    for seg in segments:
        if seg["type"] == "text":
            tb.text(seg["text"])
        elif seg["type"] == "link":
            tb.link(seg["text"], seg["url"])
    return tb


def _login(handle: str) -> Client:
    load_dotenv()
    password = os.environ.get("GE_BSKY_APP_PASSWORD") or getpass.getpass("App password: ")
    client = Client()
    client.login(handle, password)
    return client


def _load_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)


def cmd_publish(args) -> None:
    content = _load_file(args.file)
    if not content:
        print("Error: file is empty", file=sys.stderr)
        sys.exit(1)
    segments = parse_content(content)

    if args.dry_run:
        print("=== DRY RUN — post content ===")
        for seg in segments:
            if seg["type"] == "text":
                print(repr(seg["text"]))
            else:
                print(f'  [link] "{seg["text"]}" → {seg["url"]}')
        return

    tb = build_text_builder(segments)
    client = _login(args.handle)
    post = client.send_post(tb)
    rkey = post.uri.split("/")[-1]
    print("Published!")
    print(f"  AT URI: {post.uri}")
    print(f"  View:   https://bsky.app/profile/{args.handle}/post/{rkey}")


def cmd_update(args) -> None:
    if not args.at_uri.startswith("at://"):
        print(f"Not an AT URI: {args.at_uri}", file=sys.stderr)
        sys.exit(1)
    parts = args.at_uri[5:].split("/", 2)
    if len(parts) != 3:
        print("AT URI must be at://repo/collection/rkey", file=sys.stderr)
        sys.exit(1)
    repo, collection, rkey = parts

    content = _load_file(args.file)
    if not content:
        print("Error: file is empty", file=sys.stderr)
        sys.exit(1)
    segments = parse_content(content)
    tb = build_text_builder(segments)

    if args.dry_run:
        print(f"=== DRY RUN — would update {args.at_uri} ===")
        for seg in segments:
            if seg["type"] == "text":
                print(repr(seg["text"]))
            else:
                print(f'  [link] "{seg["text"]}" → {seg["url"]}')
        return

    client = _login(args.handle)
    if repo != client.me.did:
        print(f"Error: AT URI repo ({repo}) does not match your DID ({client.me.did})", file=sys.stderr)
        sys.exit(1)

    # Fetch existing record to preserve fields we're not changing (e.g. createdAt)
    existing = client.com.atproto.repo.get_record({
        "repo": repo,
        "collection": collection,
        "rkey": rkey,
    })
    val = existing.value
    base = val.model_dump(by_alias=True) if hasattr(val, "model_dump") else dict(val)

    # Build updated record: keep all existing fields, replace text + facets
    new_text = tb.build_text()
    new_facets = [f.model_dump(by_alias=True) for f in tb.build_facets()]
    updated_record = {**base, "text": new_text, "facets": new_facets}

    client.com.atproto.repo.put_record({
        "repo": client.me.did,
        "collection": collection,
        "rkey": rkey,
        "record": updated_record,
    })
    print("Updated!")
    print(f"  AT URI: {args.at_uri}  (preserved)")
    print(f"  View:   https://bsky.app/profile/{args.handle}/post/{rkey}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish or update Bluesky posts.")
    parser.add_argument("--handle", required=True, help="Bluesky handle (e.g. you.bsky.social)")
    sub = parser.add_subparsers(dest="command", required=True)

    pub = sub.add_parser("publish", help="Publish a new post")
    pub.add_argument("--file", required=True, help="Path to post content file")
    pub.add_argument("--dry-run", action="store_true")

    upd = sub.add_parser("update", help="Update an existing post in-place")
    upd.add_argument("at_uri", help="AT URI of the post to update")
    upd.add_argument("--file", required=True, help="Path to updated content file")
    upd.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if args.command == "publish":
        cmd_publish(args)
    elif args.command == "update":
        cmd_update(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Publish (or update) an AT Protocol feed generator record.

Creates an ``app.bsky.feed.generator`` record in the publisher's repo that
points Bluesky clients at our feed generator service.  Re-running the script
with the same feed name will update the existing record in place.

Usage:
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --feed-name greenearth-dev \\
        --generator-did did:web:xxxx-xxx-xxx.ngrok-free.app

    You will be prompted for your app password (or set BSKY_APP_PASSWORD).

Dependencies (already in the API Pipfile):
    pip install httpx
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys

import httpx

DEFAULT_PDS = "https://bsky.social"

# Feed metadata — extend this as we add more feeds.
FEED_METADATA: dict[str, dict[str, str]] = {
    "greenearth-dev": {
        "displayName": "GreenEarth Dev",
        "description": "Development feed — post-similarity candidates with popularity infill.",
    },
}


def _create_session(
    client: httpx.Client,
    pds: str,
    handle: str,
    password: str,
) -> dict:
    """Authenticate with the PDS and return the session object."""
    resp = client.post(
        f"{pds}/xrpc/com.atproto.server.createSession",
        json={"identifier": handle, "password": password},
    )
    if resp.status_code != 200:
        print(f"Login failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)
    return resp.json()


def _put_record(
    client: httpx.Client,
    pds: str,
    access_jwt: str,
    repo_did: str,
    rkey: str,
    record: dict,
) -> dict:
    """Create or update a feed generator record via ``com.atproto.repo.putRecord``."""
    resp = client.post(
        f"{pds}/xrpc/com.atproto.repo.putRecord",
        headers={"Authorization": f"Bearer {access_jwt}"},
        json={
            "repo": repo_did,
            "collection": "app.bsky.feed.generator",
            "rkey": rkey,
            "record": record,
        },
    )
    if resp.status_code != 200:
        print(f"putRecord failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)
    return resp.json()


def publish_feed(
    *,
    handle: str,
    password: str,
    feed_name: str,
    generator_did: str,
    display_name: str | None = None,
    description: str | None = None,
    pds: str = DEFAULT_PDS,
) -> dict:
    """Publish or update a feed generator record.

    Args:
        handle: Bluesky handle of the publishing account.
        password: App password for the publishing account.
        feed_name: Short name / rkey for the feed (e.g. ``greenearth-dev``).
        generator_did: DID of the feed generator service
            (e.g. ``did:web:xxxx.ngrok-free.app``).
        display_name: Human-readable name shown in the app.
            Falls back to ``FEED_METADATA`` then ``feed_name``.
        description: Optional feed description.
            Falls back to ``FEED_METADATA``.
        pds: PDS endpoint to authenticate against.

    Returns:
        The ``putRecord`` response dict (contains ``uri`` and ``cid``).
    """
    meta = FEED_METADATA.get(feed_name, {})
    display_name = display_name or meta.get("displayName", feed_name)
    description = description or meta.get("description", "")

    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]

        from datetime import datetime, timezone

        record: dict = {
            "$type": "app.bsky.feed.generator",
            "did": generator_did,
            "displayName": display_name,
            "description": description,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }

        result = _put_record(
            client, pds, access_jwt, repo_did, feed_name, record
        )

    feed_uri = f"at://{repo_did}/app.bsky.feed.generator/{feed_name}"
    print(f"Published feed record:")
    print(f"  URI:  {feed_uri}")
    print(f"  CID:  {result.get('cid', '?')}")
    print(f"  DID:  {generator_did}")
    print(f"  Name: {display_name}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish a feed generator record to Bluesky.",
    )
    parser.add_argument(
        "--handle",
        required=True,
        help="Bluesky handle of the publishing account (e.g. alice.bsky.social)",
    )
    parser.add_argument(
        "--feed-name",
        required=True,
        help=(
            "Short name (rkey) for the feed. Must match a key in the API's "
            "FEEDS config (e.g. greenearth-dev)."
        ),
    )
    parser.add_argument(
        "--generator-did",
        required=True,
        help="DID of the feed generator service (e.g. did:web:xxxx.ngrok-free.app)",
    )
    parser.add_argument(
        "--display-name",
        default=None,
        help="Display name (defaults to built-in metadata for known feeds)",
    )
    parser.add_argument(
        "--description",
        default=None,
        help="Feed description (defaults to built-in metadata for known feeds)",
    )
    parser.add_argument(
        "--pds",
        default=DEFAULT_PDS,
        help=f"PDS endpoint to authenticate against (default: {DEFAULT_PDS})",
    )
    args = parser.parse_args()

    import os

    password = os.environ.get("BSKY_APP_PASSWORD")
    if not password:
        password = getpass.getpass("App password: ")

    publish_feed(
        handle=args.handle,
        password=password,
        feed_name=args.feed_name,
        generator_did=args.generator_did,
        display_name=args.display_name,
        description=args.description,
        pds=args.pds,
    )


if __name__ == "__main__":
    main()

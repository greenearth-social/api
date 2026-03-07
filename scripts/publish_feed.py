#!/usr/bin/env python3
"""Publish (or update) an AT Protocol feed generator record.

Creates an ``app.bsky.feed.generator`` record in the publisher's repo that
points Bluesky clients at our feed generator service.  Re-running the script
with the same feed name will update the existing record in place.

The script can also delete feed records using ``--delete`` or ``--delete-all``.

Usage:
    # Publish a feed (reads GE_FEED_GENERATOR_DID from .env)
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --feed-name greenearth-dev

    # Publish all configured feeds
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --all

    # Delete a specific feed
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --feed-name greenearth-dev \\
        --delete

    # Delete all feeds
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --delete-all

    # List all published feeds
    python scripts/publish_feed.py \\
        --handle  alice.bsky.social \\
        --list

    You will be prompted for your app password (or set GE_BSKY_APP_PASSWORD).
    Use --app-password to explicitly provide a password at the command line.

Dependencies (already in the API Pipfile):
    pip install httpx python-dotenv
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys

import httpx
from dotenv import load_dotenv

# Add the repo's src/ directory to the path so we can import app.*
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent / "src"))  # type: ignore
from app.feeds import FEEDS  # type: ignore

DEFAULT_PDS = "https://bsky.social"

ENV_DISPLAY_PREFIX: dict[str, str] = {
    "dev": "GE Dev",
    "stage": "GE Stg",
    "prod": "GreenEarth",
}


def _prefixed_display_name(display_name: str, environment: str | None) -> str:
    """Prepend an environment-specific prefix to *display_name*.

    Returns *display_name* unchanged when *environment* is ``None`` or not
    recognised.
    """
    if not environment:
        return display_name
    prefix = ENV_DISPLAY_PREFIX.get(environment)
    if not prefix:
        return display_name
    return f"{prefix} {display_name}"


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


def _delete_record(
    client: httpx.Client,
    pds: str,
    access_jwt: str,
    repo_did: str,
    rkey: str,
) -> None:
    """Delete a feed generator record via ``com.atproto.repo.deleteRecord``."""
    resp = client.post(
        f"{pds}/xrpc/com.atproto.repo.deleteRecord",
        headers={"Authorization": f"Bearer {access_jwt}"},
        json={
            "repo": repo_did,
            "collection": "app.bsky.feed.generator",
            "rkey": rkey,
        },
    )
    if resp.status_code != 200:
        print(f"deleteRecord failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)


def _list_records(
    client: httpx.Client,
    pds: str,
    access_jwt: str,
    repo_did: str,
) -> list[dict]:
    """List all feed generator records via ``com.atproto.repo.listRecords``."""
    resp = client.get(
        f"{pds}/xrpc/com.atproto.repo.listRecords",
        headers={"Authorization": f"Bearer {access_jwt}"},
        params={
            "repo": repo_did,
            "collection": "app.bsky.feed.generator",
        },
    )
    if resp.status_code != 200:
        print(f"listRecords failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)
    return resp.json().get("records", [])


def publish_feed(
    *,
    handle: str,
    password: str,
    feed_name: str,
    generator_did: str,
    display_name: str | None = None,
    description: str | None = None,
    environment: str | None = None,
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
    feed_cfg = FEEDS.get(feed_name)
    display_name = display_name or (feed_cfg.display_name if feed_cfg else feed_name)
    display_name = _prefixed_display_name(display_name, environment)
    description = description or (feed_cfg.description if feed_cfg else "")

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


def delete_feed(
    *,
    handle: str,
    password: str,
    feed_name: str,
    pds: str = DEFAULT_PDS,
) -> None:
    """Delete a feed generator record.

    Args:
        handle: Bluesky handle of the publishing account.
        password: App password for the publishing account.
        feed_name: Short name / rkey for the feed (e.g. ``greenearth-dev``).
        pds: PDS endpoint to authenticate against.
    """
    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]

        _delete_record(client, pds, access_jwt, repo_did, feed_name)

    feed_uri = f"at://{repo_did}/app.bsky.feed.generator/{feed_name}"
    print(f"Deleted feed record:")
    print(f"  URI:  {feed_uri}")
    print(f"  Name: {feed_name}")


def delete_all_feeds(
    *,
    handle: str,
    password: str,
    pds: str = DEFAULT_PDS,
) -> None:
    """Delete all feed generator records for the given handle.

    Args:
        handle: Bluesky handle of the publishing account.
        password: App password for the publishing account.
        pds: PDS endpoint to authenticate against.
    """
    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]

        records = _list_records(client, pds, access_jwt, repo_did)

        if not records:
            print("No feed records found.")
            return

        print(f"Found {len(records)} feed record(s). Deleting...")
        for record in records:
            rkey = record["uri"].split("/")[-1]
            _delete_record(client, pds, access_jwt, repo_did, rkey)
            print(f"  Deleted: {rkey}")

        print(f"\nDeleted {len(records)} feed record(s).")


def list_feeds(
    *,
    handle: str,
    password: str,
    pds: str = DEFAULT_PDS,
) -> None:
    """List all feed generator records for the given handle.

    Args:
        handle: Bluesky handle of the publishing account.
        password: App password for the publishing account.
        pds: PDS endpoint to authenticate against.
    """
    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]

        records = _list_records(client, pds, access_jwt, repo_did)

        if not records:
            print("No feed records found.")
            return

        print(f"Found {len(records)} feed record(s) for {handle}:\n")
        for record in records:
            rkey = record["uri"].split("/")[-1]
            value = record.get("value", {})
            display_name = value.get("displayName", "?")
            description = value.get("description", "")
            created_at = value.get("createdAt", "?")

            print(f"  • {rkey}")
            print(f"    Name: {display_name}")
            if description:
                print(f"    Desc: {description}")
            print(f"    URI:  {record['uri']}")
            print(f"    Created: {created_at}")
            print()


def sync_feeds(
    *,
    handle: str,
    password: str,
    generator_did: str,
    environment: str | None = None,
    pds: str = DEFAULT_PDS,
) -> None:
    """Sync published feed records with the FEEDS config.

    Creates or updates a record for every feed in ``FEEDS`` and deletes any
    existing records whose rkey is *not* present in ``FEEDS``.  Because
    ``putRecord`` is an upsert, feeds that already exist are updated in place
    without a visible gap.
    """
    if not FEEDS:
        print("No feeds configured in FEEDS.", file=sys.stderr)
        sys.exit(1)

    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]

        # Discover existing records
        existing_records = _list_records(client, pds, access_jwt, repo_did)
        existing_rkeys = {r["uri"].split("/")[-1] for r in existing_records}
        desired_rkeys = set(FEEDS.keys())

        from datetime import datetime, timezone

        # Publish / update every feed in FEEDS
        for rkey, feed_cfg in FEEDS.items():
            display_name = _prefixed_display_name(feed_cfg.display_name, environment)
            record: dict = {
                "$type": "app.bsky.feed.generator",
                "did": generator_did,
                "displayName": display_name,
                "description": feed_cfg.description,
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }
            _put_record(client, pds, access_jwt, repo_did, rkey, record)
            print(f"  Published: {rkey} ({display_name})")

        # Remove stale records not in FEEDS
        stale = existing_rkeys - desired_rkeys
        for rkey in sorted(stale):
            _delete_record(client, pds, access_jwt, repo_did, rkey)
            print(f"  Deleted stale: {rkey}")

        print(f"\nSync complete: {len(desired_rkeys)} published, {len(stale)} deleted.")


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
        default=None,
        help=(
            "Short name (rkey) for the feed. Must match a key in the API's "
            "FEEDS config (e.g. greenearth-dev). Required unless --all is used."
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="publish_all",
        help="Publish all feeds defined in the FEEDS config.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the specified feed instead of publishing it. Requires --feed-name.",
    )
    parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete all feed generator records under the given handle.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help=(
            "Sync mode: publish/update all feeds from FEEDS config and delete "
            "any stale records not present in FEEDS. Avoids delete-recreate."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all feed generator records under the given handle.",
    )
    parser.add_argument(
        "--generator-did",
        default=None,
        help=(
            "DID of the feed generator service (e.g. did:web:xxxx.ngrok-free.app). "
            "Falls back to GE_FEED_GENERATOR_DID environment variable if not specified."
        ),
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
        "--app-password",
        default=None,
        help=(
            "Bluesky app password. Takes precedence over GE_BSKY_APP_PASSWORD "
            "from environment or .env."
        ),
    )
    parser.add_argument(
        "--environment",
        default=None,
        choices=["dev", "stage", "prod"],
        help="Environment name — prefixes feed display names (dev/stage/prod).",
    )
    parser.add_argument(
        "--pds",
        default=DEFAULT_PDS,
        help=f"PDS endpoint to authenticate against (default: {DEFAULT_PDS})",
    )
    args = parser.parse_args()

    # Load .env file to pick up GE_FEED_GENERATOR_DID and other env vars
    load_dotenv()

    password = args.app_password or os.environ.get("GE_BSKY_APP_PASSWORD")
    if not password:
        password = getpass.getpass("App password: ")

    # Validate mutually exclusive flags
    mode_flags = sum([args.publish_all, args.delete, args.delete_all, args.list, args.sync])
    if mode_flags > 1:
        parser.error("Only one of --all, --delete, --delete-all, --list, or --sync can be used at a time.")

    # Handle list mode
    if args.list:
        list_feeds(
            handle=args.handle,
            password=password,
            pds=args.pds,
        )
        return

    # Handle sync mode
    if args.sync:
        generator_did = args.generator_did or os.environ.get("GE_FEED_GENERATOR_DID")
        if not generator_did:
            parser.error(
                "--generator-did is required for --sync (or set GE_FEED_GENERATOR_DID)"
            )
        sync_feeds(
            handle=args.handle,
            password=password,
            generator_did=generator_did,
            environment=args.environment,
            pds=args.pds,
        )
        return

    # Handle deletion modes
    if args.delete_all:
        delete_all_feeds(
            handle=args.handle,
            password=password,
            pds=args.pds,
        )
        return

    if args.delete:
        if not args.feed_name:
            parser.error("--delete requires --feed-name to specify which feed to delete.")
        delete_feed(
            handle=args.handle,
            password=password,
            feed_name=args.feed_name,
            pds=args.pds,
        )
        return

    # Publishing mode (default)
    generator_did = args.generator_did or os.environ.get("GE_FEED_GENERATOR_DID")
    if not generator_did:
        parser.error(
            "--generator-did is required (or set GE_FEED_GENERATOR_DID environment variable)"
        )

    if args.publish_all:
        if not FEEDS:
            print("No feeds configured in FEEDS.", file=sys.stderr)
            sys.exit(1)
        feed_names = list(FEEDS.keys())
        print(f"Publishing {len(feed_names)} feed(s): {', '.join(feed_names)}")
    elif args.feed_name:
        feed_names = [args.feed_name]
    else:
        parser.error("Either --feed-name or --all is required.")

    for name in feed_names:
        publish_feed(
            handle=args.handle,
            password=password,
            feed_name=name,
            generator_did=generator_did,
            display_name=args.display_name,
            description=args.description,
            environment=args.environment,
            pds=args.pds,
        )
        if len(feed_names) > 1:
            print()


if __name__ == "__main__":
    main()

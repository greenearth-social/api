#!/usr/bin/env python3
"""Publish repository-managed posts used as the first item in public feeds.

An unchanged deployment is skipped by the deploy-time configuration fingerprint.
When synchronization is needed, this script reuses an exact text-and-link match
from the publisher's repository or creates a normal TID-keyed Bluesky post.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
from datetime import UTC, datetime

from atproto import Client, client_utils, models
from dotenv import load_dotenv

# Allow imports from both src/ and scripts/ when run from the repository root.
REPO_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from manage_post import parse_content  # type: ignore  # noqa: E402

from app.feeds import FEEDS  # type: ignore  # noqa: E402

POST_COLLECTION = "app.bsky.feed.post"
# Bump when managed-post construction or matching changes and deployment should
# perform an authenticated verification even if the configured copy is unchanged.
MANAGED_POST_SCHEMA_VERSION = 1
DEPLOYED_PIN_ENV_NAMES = (
    "GE_PINNED_POST_CONFIG_SHA",
    "GE_PINNED_POST_YOUR_FEED_URI",
    "GE_PINNED_POST_BEST_OF_FRIENDS_URI",
    "GE_PINNED_POST_RANDOM_URI",
)


def managed_pinned_post_config() -> dict[str, str]:
    """Return the public feed-name/content mapping owned by this script."""
    return {
        name: cfg.pinned_post_content
        for name, cfg in FEEDS.items()
        if cfg.public and cfg.pinned_post_content
    }


def pinned_post_config_sha(config: dict[str, str] | None = None) -> str:
    """Fingerprint only the inputs that can change managed post URIs."""
    posts = managed_pinned_post_config() if config is None else config
    payload = json.dumps(
        {"schema_version": MANAGED_POST_SCHEMA_VERSION, "posts": posts},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def deployed_pin_state(service: dict) -> dict[str, str]:
    """Extract managed-pin environment values from a Cloud Run service document."""
    containers = service.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    env = containers[0].get("env", []) if containers else []
    values = {item.get("name"): item.get("value") for item in env if item.get("name")}
    return {name: values[name] for name in DEPLOYED_PIN_ENV_NAMES if values.get(name)}


def build_post_record(content: str) -> models.AppBskyFeedPost.Record:
    """Convert configured markdown links into a Bluesky post record."""
    builder = client_utils.TextBuilder()
    for segment in parse_content(content):
        if segment["type"] == "link":
            builder.link(segment["text"], segment["url"])
        else:
            builder.text(segment["text"])
    return models.AppBskyFeedPost.Record(
        text=builder.build_text(),
        facets=builder.build_facets(),
        created_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )


def _field(value: object, name: str, default=None):
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _post_signature(record: object) -> tuple[str, tuple[str, ...]]:
    """Return visible text and link targets for exact managed-post matching."""
    links: list[str] = []
    for facet in _field(record, "facets", []) or []:
        for feature in _field(facet, "features", []) or []:
            uri = _field(feature, "uri")
            if isinstance(uri, str) and uri:
                links.append(uri)
    text = _field(record, "text", "")
    return text if isinstance(text, str) else "", tuple(links)


def list_repo_posts(client: Client, repo_did: str) -> list[object]:
    """Read the publisher's post records, newest first, following pagination."""
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


def ensure_pinned_post(
    client: Client,
    repo_did: str,
    feed_name: str,
    content: str,
    existing_posts: list[object] | None = None,
) -> str:
    """Return the desired post URI, creating a normal Bluesky post only when absent."""
    desired = build_post_record(content)
    posts = list_repo_posts(client, repo_did) if existing_posts is None else existing_posts
    desired_signature = _post_signature(desired)
    for existing in posts:
        if _post_signature(_field(existing, "value")) == desired_signature:
            uri = _field(existing, "uri")
            if uri:
                print(f"Reusing managed pin for {feed_name}: {uri}", file=sys.stderr)
                return uri

    builder = client_utils.TextBuilder()
    for segment in parse_content(content):
        if segment["type"] == "link":
            builder.link(segment["text"], segment["url"])
        else:
            builder.text(segment["text"])
    result = client.send_post(builder)
    print(f"Published managed pin for {feed_name}: {result.uri}", file=sys.stderr)
    return result.uri


def sync_pinned_posts(handle: str, password: str) -> dict[str, str]:
    """Ensure every public feed's configured pin exists under *handle*."""
    client = Client()
    profile = client.login(handle, password)
    repo_did = profile.did

    configured = managed_pinned_post_config()
    if not configured:
        raise RuntimeError("No public managed pinned posts are configured")

    existing_posts = list_repo_posts(client, repo_did)
    return {
        name: ensure_pinned_post(client, repo_did, name, content, existing_posts)
        for name, content in configured.items()
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish repository-managed feed pins.")
    parser.add_argument("--handle")
    parser.add_argument("--app-password", default=None)
    parser.add_argument(
        "--config-sha",
        action="store_true",
        help="Print the managed-post configuration fingerprint without contacting Bluesky.",
    )
    parser.add_argument(
        "--extract-deployed-state",
        action="store_true",
        help="Read Cloud Run service JSON from stdin and print managed pin values as TSV.",
    )
    parser.add_argument(
        "--format",
        choices=["human", "tsv"],
        default="human",
        help="Use tsv for deploy.sh machine-readable feed-name/URI output.",
    )
    args = parser.parse_args()

    if args.config_sha and args.extract_deployed_state:
        parser.error("--config-sha and --extract-deployed-state are mutually exclusive")
    if args.config_sha:
        print(pinned_post_config_sha())
        return
    if args.extract_deployed_state:
        for name, value in deployed_pin_state(json.load(sys.stdin)).items():
            print(f"{name}\t{value}")
        return
    if not args.handle:
        parser.error("--handle is required when publishing pinned posts")

    load_dotenv()
    password = args.app_password or os.environ.get("GE_BSKY_APP_PASSWORD")
    if not password:
        parser.error("--app-password is required (or set GE_BSKY_APP_PASSWORD)")

    posts = sync_pinned_posts(args.handle, password)
    if args.format == "tsv":
        for feed_name, uri in posts.items():
            print(f"{feed_name}\t{uri}")
    else:
        print("Managed pinned posts:")
        for feed_name, uri in posts.items():
            print(f"  {feed_name}: {uri}")


if __name__ == "__main__":
    main()

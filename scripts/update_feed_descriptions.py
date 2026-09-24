#!/usr/bin/env python3
"""Synchronize public feed descriptions from feeds.py without deploying.

Each existing generator record receives the complete configured description.
Other metadata is preserved, except for description facets whose byte offsets
would become stale when the description changes.
"""

from __future__ import annotations

import argparse
import getpass
import os
import subprocess
import sys
from dataclasses import dataclass

import httpx
from dotenv import load_dotenv
from publish_feed import (
    DEFAULT_PDS,
    FEEDS,
    _create_session,
    _list_records,
    _put_record,
    _resolve_feed_publish_params,
)


@dataclass(frozen=True)
class EnvironmentTarget:
    handle: str
    secret: str


# Authenticate with stable account DIDs so handle changes do not break publishing.
ENVIRONMENT_TARGETS = {
    "prod": EnvironmentTarget(
        handle="did:plc:wrmpulygwvuhjn2c3jbalgqj",
        secret="bsky-app-password-prod",
    ),
    "stage": EnvironmentTarget(
        handle="did:plc:s4tl2ajfsnstzuxtegl7r33g",
        secret="bsky-app-password-caterpie",
    ),
}


@dataclass(frozen=True)
class UpdateSummary:
    updated: int
    already_current: int
    skipped: int
    missing: int

    @property
    def needs_attention(self) -> bool:
        return self.skipped > 0 or self.missing > 0


def _target_descriptions(environment: str, feed_name: str | None = None) -> dict[str, str]:
    if environment not in ENVIRONMENT_TARGETS:
        raise ValueError(f"Unknown environment: {environment}")
    if feed_name is not None and (feed_name not in FEEDS or not FEEDS[feed_name].public):
        raise ValueError(f"Not a public feed: {feed_name}")
    descriptions: dict[str, str] = {}
    for canonical_rkey, feed_config in FEEDS.items():
        if not feed_config.public or (feed_name is not None and canonical_rkey != feed_name):
            continue
        published_rkey, _, _ = _resolve_feed_publish_params(
            canonical_rkey,
            feed_config,
            environment,
        )
        descriptions[published_rkey] = feed_config.description
    return descriptions


def update_feed_descriptions(
    *,
    handle: str,
    password: str,
    environment: str,
    feed_name: str | None = None,
    pds: str = DEFAULT_PDS,
    dry_run: bool = False,
) -> UpdateSummary:
    """Apply configured descriptions to existing public generator records."""
    targets = _target_descriptions(environment, feed_name)
    updated = 0
    already_current = 0
    skipped = 0

    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]
        records = {
            record["uri"].split("/")[-1]: record.get("value")
            for record in _list_records(client, pds, access_jwt, repo_did)
        }

        missing_rkeys = targets.keys() - records.keys()
        for rkey in sorted(missing_rkeys):
            print(f"  Missing: {rkey}", file=sys.stderr)

        for rkey in sorted(targets.keys() & records.keys()):
            value = records[rkey]
            if not isinstance(value, dict):
                print(f"  No valid record value: {rkey}", file=sys.stderr)
                skipped += 1
                continue
            description = value.get("description", "")
            if not isinstance(description, str):
                print(f"  Invalid description; left unchanged: {rkey}", file=sys.stderr)
                skipped += 1
                continue

            configured_description = targets[rkey]
            if description == configured_description:
                print(f"  Already current: {rkey}")
                already_current += 1
                continue
            record = dict(value)
            record["description"] = configured_description
            record.pop("descriptionFacets", None)
            if dry_run:
                print(f"  Would update: {rkey}")
                print(f"    Before: {description!r}")
                print(f"    After: {configured_description!r}")
            else:
                _put_record(client, pds, access_jwt, repo_did, rkey, record)
                print(f"  Updated: {rkey}")
            updated += 1

    return UpdateSummary(
        updated=updated,
        already_current=already_current,
        skipped=skipped,
        missing=len(missing_rkeys),
    )


def _password_from_secret(project_id: str, secret: str) -> str:
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
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise RuntimeError(
            f"Could not read app password from Secret Manager secret {secret!r}: {detail.strip()}"
        ) from exc
    password = result.stdout.strip()
    if not password:
        raise RuntimeError(f"Secret Manager secret {secret!r} was empty")
    return password


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize existing public feed descriptions from feeds.py without deploying."
        )
    )
    parser.add_argument(
        "--environment",
        required=True,
        choices=["stage", "prod"],
        help="Environment/account whose public feed records should be updated.",
    )
    parser.add_argument(
        "--feed-name",
        choices=sorted(rkey for rkey, config in FEEDS.items() if config.public),
        help="Update only this public feed's canonical key (default: all public feeds).",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("PROJECT_ID", "greenearth-471522"),
        help="GCP project containing the Bluesky app-password secret.",
    )
    parser.add_argument("--handle", help="Override the environment's publisher handle.")
    parser.add_argument(
        "--app-password",
        help=(
            "Override the app password. Otherwise GE_BSKY_APP_PASSWORD is used, "
            "then the environment's Secret Manager secret."
        ),
    )
    parser.add_argument(
        "--pds",
        default=DEFAULT_PDS,
        help=f"PDS endpoint (default: {DEFAULT_PDS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Authenticate and report the changes without writing records.",
    )
    args = parser.parse_args()
    load_dotenv()

    target = ENVIRONMENT_TARGETS[args.environment]
    password = args.app_password or os.environ.get("GE_BSKY_APP_PASSWORD")
    if not password:
        try:
            password = _password_from_secret(args.project_id, target.secret)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            password = getpass.getpass("App password: ")

    summary = update_feed_descriptions(
        handle=args.handle or target.handle,
        password=password,
        environment=args.environment,
        feed_name=args.feed_name,
        pds=args.pds,
        dry_run=args.dry_run,
    )
    mode = "dry run" if args.dry_run else "sync"
    print(
        f"{args.environment} {mode} complete: {summary.updated} updated, "
        f"{summary.already_current} already current, "
        f"{summary.skipped} skipped, {summary.missing} missing."
    )
    if summary.needs_attention:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

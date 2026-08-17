#!/usr/bin/env python3
"""One-time migration for the public Green Earth feed descriptions.

This script reads each existing generator record and replaces only the legacy
attribution text. It does not append a footer or reconstruct the rest of the
description. Normal deployments preserve the resulting description.
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

NEW_ATTRIBUTION = (
    "Part of the Green Earth feed family. Built by GreenEarth (https://www.greenearth.social)."
)
LEGACY_ATTRIBUTIONS = (
    "Built by GreenEarth (www.greenearth.social).",
    "Built by GreenEarth (https://www.greenearth.social).",
)


@dataclass(frozen=True)
class EnvironmentTarget:
    handle: str
    secret: str


ENVIRONMENT_TARGETS = {
    "prod": EnvironmentTarget(
        handle="greenearth-social.bsky.social",
        secret="bsky-app-password-prod",
    ),
    "stage": EnvironmentTarget(
        handle="caterpie-internal.bsky.social",
        secret="bsky-app-password-caterpie",
    ),
}


@dataclass(frozen=True)
class UpdateSummary:
    updated: int
    already_current: int
    no_legacy_text: int
    missing: int

    @property
    def needs_attention(self) -> bool:
        return self.no_legacy_text > 0 or self.missing > 0


def replace_legacy_attribution(description: str) -> tuple[str, str]:
    """Return the migrated text and ``updated``/``current``/``not_found``."""
    # The current copy intentionally contains one of the legacy attribution
    # strings. Recognize the complete current value before looking for a legacy
    # substring so the migration remains idempotent and never nests its prefix.
    if NEW_ATTRIBUTION in description:
        return description, "current"
    for legacy in LEGACY_ATTRIBUTIONS:
        if legacy in description:
            return description.replace(legacy, NEW_ATTRIBUTION), "updated"
    return description, "not_found"


def _target_rkeys(environment: str) -> set[str]:
    rkeys: set[str] = set()
    for canonical_rkey, feed_config in FEEDS.items():
        if not feed_config.public:
            continue
        published_rkey, _, _ = _resolve_feed_publish_params(
            canonical_rkey,
            feed_config,
            environment,
        )
        rkeys.add(published_rkey)
    return rkeys


def update_feed_descriptions(
    *,
    handle: str,
    password: str,
    environment: str,
    pds: str = DEFAULT_PDS,
    dry_run: bool = False,
) -> UpdateSummary:
    """Replace the legacy attribution on existing public generator records."""
    targets = _target_rkeys(environment)
    updated = 0
    already_current = 0
    no_legacy_text = 0

    with httpx.Client(timeout=30) as client:
        session = _create_session(client, pds, handle, password)
        access_jwt = session["accessJwt"]
        repo_did = session["did"]
        records = {
            record["uri"].split("/")[-1]: record.get("value", {})
            for record in _list_records(client, pds, access_jwt, repo_did)
        }

        missing_rkeys = targets - records.keys()
        for rkey in sorted(missing_rkeys):
            print(f"  Missing: {rkey}", file=sys.stderr)

        for rkey in sorted(targets & records.keys()):
            value = records[rkey]
            if not isinstance(value, dict):
                print(f"  No valid record value: {rkey}", file=sys.stderr)
                no_legacy_text += 1
                continue
            description = value.get("description")
            if not isinstance(description, str):
                print(f"  No description: {rkey}", file=sys.stderr)
                no_legacy_text += 1
                continue

            migrated, outcome = replace_legacy_attribution(description)
            if outcome == "current":
                print(f"  Already current: {rkey}")
                already_current += 1
                continue
            if outcome == "not_found":
                print(
                    f"  Legacy attribution not found; left unchanged: {rkey}",
                    file=sys.stderr,
                )
                no_legacy_text += 1
                continue

            # This repository has never created description facets. Refuse to
            # shift unknown byte offsets rather than silently corrupting them.
            if value.get("descriptionFacets"):
                print(
                    f"  Description facets require manual migration: {rkey}",
                    file=sys.stderr,
                )
                no_legacy_text += 1
                continue

            record = dict(value)
            record["description"] = migrated
            if dry_run:
                print(f"  Would update: {rkey}")
            else:
                _put_record(client, pds, access_jwt, repo_did, rkey, record)
                print(f"  Updated: {rkey}")
            updated += 1

    return UpdateSummary(
        updated=updated,
        already_current=already_current,
        no_legacy_text=no_legacy_text,
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
            "Replace the legacy GreenEarth attribution in existing public feed "
            "descriptions without changing any other description text."
        )
    )
    parser.add_argument(
        "--environment",
        required=True,
        choices=["stage", "prod"],
        help="Environment/account whose public feed records should be migrated.",
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
        pds=args.pds,
        dry_run=args.dry_run,
    )
    mode = "dry run" if args.dry_run else "migration"
    print(
        f"{args.environment} {mode} complete: {summary.updated} updated, "
        f"{summary.already_current} already current, "
        f"{summary.no_legacy_text} unmatched, {summary.missing} missing."
    )
    if summary.needs_attention:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

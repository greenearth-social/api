#!/usr/bin/env python3
"""Publish and resolve the repository-managed UX posts injected into feeds.

Content lives in ``assets/ux_posts/`` and is versioned like any other source. This
script reconciles text, links, and any attached native video against what is actually
posted on the notifications account and writes ``src/app/ux_posts_resolved.json``,
the generated manifest the running API reads.

The manifest is gitignored on purpose. A pull request then carries only content and
code, so concurrent branches cannot conflict on deployment state and nothing has to
be published before a merge. The manifest still ships inside the Cloud Run image, so
a rolled-back revision keeps the URIs it was built with.

Commands::

    check                 validate content offline (no network)
    resolve               match content against the account, write the manifest
    sync                  resolve, publishing anything missing (needs credentials)
    list                  print the resolved mapping
    cleanup               delete superseded posts from the account

Typical flows::

    # local development, no credentials needed
    pipenv run python scripts/manage_ux_posts.py resolve

    # what deploy.sh runs
    pipenv run python scripts/manage_ux_posts.py check
    pipenv run python scripts/manage_ux_posts.py sync

Matching is by exact content signature (visible text, link targets, and video blob
CID), so editing copy or replacing a video publishes a new record and leaves the old
one alone — an older Cloud Run revision keeps resolving to the post it shipped with.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import UTC, datetime, timedelta

import httpx
from dotenv import load_dotenv

# Allow imports from both src/ and scripts/ when run from the repository root.
REPO_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import managed_posts  # type: ignore  # noqa: E402

from app import ux_posts as registry  # type: ignore  # noqa: E402

PLC_DIRECTORY = "https://plc.directory"
DEFAULT_PROJECT_ID = "greenearth-471522"
NOTIFY_SECRET = "bsky-app-password-notify-prod"

# Posts older than this that nothing references are eligible for cleanup. Generous
# because a Cloud Run revision from weeks ago is still a plausible rollback target.
DEFAULT_CLEANUP_AGE_DAYS = 30


# --- content ---------------------------------------------------------------


def content_files() -> dict[str, str]:
    """Read every managed post's content, keyed by filename."""
    return {name: registry.read_content(name) for name in registry.MANAGED_POSTS}


def check_content() -> list[str]:
    """Validate the content directory offline. Returns a list of problems."""
    problems: list[str] = []

    present = {p.name for p in registry.CONTENT_DIR.glob("*.md")}
    expected = set(registry.MANAGED_POSTS)
    for extra in sorted(present - expected):
        problems.append(
            f"{extra} is in {registry.CONTENT_DIR.name}/ but not in MANAGED_POSTS "
            f"(add it to src/app/ux_posts.py)"
        )
    for missing in sorted(expected - present):
        problems.append(f"{missing} is in MANAGED_POSTS but missing from {registry.CONTENT_DIR}")

    unknown_video_posts = set(registry.VIDEO_POSTS) - expected
    for name in sorted(unknown_video_posts):
        problems.append(f"{name} has a video but is not in MANAGED_POSTS")

    expected_videos = {spec.filename for spec in registry.VIDEO_POSTS.values()}
    present_videos = {p.name for p in registry.CONTENT_DIR.glob("*.mp4")}
    for extra in sorted(present_videos - expected_videos):
        problems.append(f"{extra} is in {registry.CONTENT_DIR.name}/ but is not registered")
    for missing in sorted(expected_videos - present_videos):
        problems.append(f"{missing} is registered but missing from {registry.CONTENT_DIR}")
    for filename in sorted(expected_videos & present_videos):
        if (registry.CONTENT_DIR / filename).stat().st_size == 0:
            problems.append(f"{filename} is empty")

    for name in sorted(expected & present):
        content = managed_posts.normalize_content(registry.read_content(name))
        if not content:
            problems.append(f"{name} is empty")
            continue
        text, _links = managed_posts.content_signature(content)
        if len(text) > registry.MAX_POST_GRAPHEMES:
            problems.append(
                f"{name} renders to {len(text)} characters, over the "
                f"{registry.MAX_POST_GRAPHEMES} limit"
            )
    return problems


# --- account state ---------------------------------------------------------


def pds_endpoint(did: str) -> str:
    """Resolve a DID's PDS host so listRecords can be read without credentials."""
    response = httpx.get(f"{PLC_DIRECTORY}/{did}", timeout=10.0)
    response.raise_for_status()
    for service in response.json().get("service", []):
        if service.get("type") == "AtprotoPersonalDataServer":
            return service["serviceEndpoint"].rstrip("/")
    raise RuntimeError(f"No PDS endpoint in the DID document for {did}")


def fetch_repo_posts(did: str) -> list[dict]:
    """Read every post record on an account. Unauthenticated."""
    endpoint = pds_endpoint(did)
    records: list[dict] = []
    cursor: str | None = None
    with httpx.Client(timeout=20.0) as client:
        while True:
            params: dict[str, object] = {
                "repo": did,
                "collection": managed_posts.POST_COLLECTION,
                "limit": 100,
            }
            if cursor:
                params["cursor"] = cursor
            response = client.get(f"{endpoint}/xrpc/com.atproto.repo.listRecords", params=params)
            response.raise_for_status()
            payload = response.json()
            records.extend(payload.get("records", []))
            cursor = payload.get("cursor")
            if not cursor:
                return records


def fetch_gate_rkeys(did: str, collection: str) -> set[str]:
    """Return the rkeys that already have a gate record in *collection*."""
    endpoint = pds_endpoint(did)
    rkeys: set[str] = set()
    cursor: str | None = None
    with httpx.Client(timeout=20.0) as client:
        while True:
            params: dict[str, object] = {"repo": did, "collection": collection, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            response = client.get(f"{endpoint}/xrpc/com.atproto.repo.listRecords", params=params)
            if response.status_code == 400:
                # The collection has no records yet.
                return rkeys
            response.raise_for_status()
            payload = response.json()
            rkeys.update(r["uri"].rsplit("/", 1)[-1] for r in payload.get("records", []))
            cursor = payload.get("cursor")
            if not cursor:
                return rkeys


def _wants_threadgate(name: str) -> bool:
    """Whether *name* should be closed to replies."""
    return name not in registry.REPLIES_ALLOWED


def misgated_posts(resolved: dict[str, str]) -> list[str]:
    """Return managed posts whose gate records don't match the intended policy.

    Gates are persistent records, so this has to converge in both directions: a post
    moved into ``REPLIES_ALLOWED`` needs its existing threadgate removed, not merely
    left unwritten. Quote posts are disabled on every UX post. Likes cannot be
    disabled -- atproto has no like-gating.
    """
    threadgated = fetch_gate_rkeys(registry.PUBLISHER_DID, managed_posts.THREADGATE_COLLECTION)
    postgated = fetch_gate_rkeys(registry.PUBLISHER_DID, managed_posts.POSTGATE_COLLECTION)
    wrong = []
    for name, uri in sorted(resolved.items()):
        rkey = uri.rsplit("/", 1)[-1]
        if (rkey in threadgated) != _wants_threadgate(name) or rkey not in postgated:
            wrong.append(name)
    return wrong


def apply_gates(client, resolved: dict[str, str], names: list[str]) -> None:
    """Converge *names* on the intended gate records."""
    from atproto import models

    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    for name in names:
        uri = resolved[name]
        rkey = uri.rsplit("/", 1)[-1]

        if _wants_threadgate(name):
            client.com.atproto.repo.put_record(
                models.ComAtprotoRepoPutRecord.Data(
                    repo=registry.PUBLISHER_DID,
                    collection=managed_posts.THREADGATE_COLLECTION,
                    rkey=rkey,
                    record=managed_posts.build_threadgate_record(uri, now),
                )
            )
            replies = "replies off"
        else:
            # deleteRecord is defined as "delete a record, or ensure it doesn't
            # exist", so this is a no-op when the post was never gated.
            client.com.atproto.repo.delete_record(
                models.ComAtprotoRepoDeleteRecord.Data(
                    repo=registry.PUBLISHER_DID,
                    collection=managed_posts.THREADGATE_COLLECTION,
                    rkey=rkey,
                )
            )
            replies = "replies ON"

        client.com.atproto.repo.put_record(
            models.ComAtprotoRepoPutRecord.Data(
                repo=registry.PUBLISHER_DID,
                collection=managed_posts.POSTGATE_COLLECTION,
                rkey=rkey,
                record=managed_posts.build_postgate_record(uri, now),
            )
        )
        print(f"  gated {name} ({replies}, quotes off)")


ManagedPostSignature = tuple[str, tuple[str, ...], str | None]


def managed_post_signature(record: object) -> ManagedPostSignature:
    """Return the text, links, and optional native-video CID from a live post."""
    text, links = managed_posts.post_signature(record)
    return text, links, managed_posts.video_blob_cid(record)


def managed_content_signature(name: str, content: str) -> ManagedPostSignature:
    """Return the signature expected for one repository-managed post."""
    text, links = managed_posts.content_signature(content)
    path = registry.video_path(name)
    video_cid = managed_posts.blob_cid(path.read_bytes()) if path else None
    return text, links, video_cid


def signature_index(records: list[dict]) -> dict[ManagedPostSignature, str]:
    """Map each existing post's content signature to its URI.

    Later records win, so if the same content was published twice the newest URI is
    the one we adopt.
    """
    index: dict[ManagedPostSignature, str] = {}
    for record in records:
        index[managed_post_signature(record.get("value", {}))] = record["uri"]
    return index


def match_content(records: list[dict]) -> tuple[dict[str, str], list[str]]:
    """Resolve each managed post against the account. Returns (resolved, missing)."""
    index = signature_index(records)
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for name, content in content_files().items():
        uri = index.get(managed_content_signature(name, content))
        if uri:
            resolved[name] = uri
        else:
            missing.append(name)
    return resolved, missing


# --- manifest --------------------------------------------------------------


def write_manifest(resolved: dict[str, str]) -> None:
    """Write the generated manifest the running API reads."""
    payload = {
        "schema_version": registry.MANIFEST_SCHEMA_VERSION,
        "publisher": registry.PUBLISHER_DID,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "posts": dict(sorted(resolved.items())),
    }
    registry.MANIFEST_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


# --- commands --------------------------------------------------------------


def cmd_check(_args) -> int:
    problems = check_content()
    if problems:
        print("UX post content is invalid:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print(f"UX post content OK ({len(registry.MANAGED_POSTS)} posts)")
    return 0


def cmd_resolve(args) -> int:
    records = fetch_repo_posts(registry.PUBLISHER_DID)
    resolved, missing = match_content(records)
    write_manifest(resolved)

    for name in sorted(resolved):
        print(f"  resolved {name} -> {resolved[name]}")
    for name in missing:
        print(f"  MISSING  {name} (not published yet)", file=sys.stderr)
    print(
        f"Wrote {registry.MANIFEST_PATH} ({len(resolved)}/{len(registry.MANAGED_POSTS)} resolved)"
    )

    misgated = misgated_posts(resolved) if resolved else []
    for name in misgated:
        print(f"  MISGATED {name} (reply/quote gates don't match policy)", file=sys.stderr)

    if (missing or misgated) and args.require_complete:
        print(
            "UX posts need publishing or gating. Run: "
            "pipenv run python scripts/manage_ux_posts.py sync",
            file=sys.stderr,
        )
        return 1
    return 0


def _credentials(args) -> tuple[str, str] | None:
    handle = args.handle or registry.PUBLISHER_DID
    load_dotenv()
    password = args.app_password or os.environ.get("GE_UX_POST_APP_PASSWORD")
    if not password:
        password = managed_posts.password_from_secret(args.project_id, NOTIFY_SECRET)
    if not password:
        print(
            f"No app password for {registry.PUBLISHER_HANDLE}. Pass --app-password, set "
            f"GE_UX_POST_APP_PASSWORD, or create the {NOTIFY_SECRET} secret.",
            file=sys.stderr,
        )
        return None
    return handle, password


def publish_post(client, name: str, content: str):
    """Publish one managed text or native-video post."""
    builder = managed_posts.build_text_builder(
        managed_posts.parse_content(managed_posts.normalize_content(content))
    )
    spec = registry.VIDEO_POSTS.get(name)
    path = registry.video_path(name)
    if spec and path:
        return client.send_video(builder, path.read_bytes(), video_alt=spec.alt)
    return client.send_post(builder)


def cmd_sync(args) -> int:
    problems = check_content()
    if problems:
        return cmd_check(args)

    records = fetch_repo_posts(registry.PUBLISHER_DID)
    resolved, missing = match_content(records)

    for name in sorted(resolved):
        print(f"  unchanged {name} -> {resolved[name]}")

    misgated = misgated_posts(resolved) if resolved else []

    if not missing and not misgated:
        write_manifest(resolved)
        print(
            f"All {len(resolved)} UX posts are published and gated; wrote {registry.MANIFEST_PATH}"
        )
        return 0

    if args.dry_run:
        for name in missing:
            print(f"  would publish {name}")
        for name in misgated:
            print(f"  would gate {name}")
        print(f"Dry run: {len(missing)} post(s) would be published, {len(misgated)} gated.")
        return 0

    creds = _credentials(args)
    if creds is None:
        return 1
    handle, password = creds
    client = managed_posts.login(handle, password)

    for name in missing:
        content = registry.read_content(name)
        result = publish_post(client, name, content)
        resolved[name] = result.uri
        print(f"  published {name} -> {result.uri}")

    # Newly published posts have no gates yet; existing ones may not match policy.
    apply_gates(client, resolved, missing + [n for n in misgated if n not in missing])

    write_manifest(resolved)
    print(f"Published {len(missing)} post(s); wrote {registry.MANIFEST_PATH}")
    return 0


def cmd_list(_args) -> int:
    resolved = registry.resolved_uris()
    if not resolved:
        print("No UX posts resolved. Run: scripts/manage_ux_posts.py resolve", file=sys.stderr)
        return 1
    for name in registry.MANAGED_POSTS:
        print(f"{name:26} {resolved.get(name, '(unresolved)')}")
    return 0


def cmd_cleanup(args) -> int:
    """Delete posts on the account that no current content file resolves to."""
    records = fetch_repo_posts(registry.PUBLISHER_DID)
    resolved, _missing = match_content(records)
    keep = set(resolved.values())
    cutoff = datetime.now(UTC) - timedelta(days=args.older_than_days)

    candidates = []
    for record in records:
        uri = record["uri"]
        if uri in keep:
            continue
        created_raw = record.get("value", {}).get("createdAt", "")
        try:
            created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        except ValueError:
            print(f"  skipping {uri}: unparseable createdAt {created_raw!r}", file=sys.stderr)
            continue
        if created >= cutoff:
            continue
        candidates.append((uri, created, record.get("value", {}).get("text", "")))

    if not candidates:
        print("Nothing to clean up.")
        return 0

    print(f"{len(candidates)} post(s) unreferenced and older than {args.older_than_days} days:")
    for uri, created, text in candidates:
        print(f"  {uri}  ({created.date()})  {text[:70]!r}")

    if not args.yes:
        print("\nRe-run with --yes to delete these permanently.")
        return 0

    creds = _credentials(args)
    if creds is None:
        return 1
    handle, password = creds
    client = managed_posts.login(handle, password)
    from atproto import models  # local import: only needed for deletion

    for uri, _created, _text in candidates:
        client.com.atproto.repo.delete_record(
            models.ComAtprotoRepoDeleteRecord.Data(
                repo=registry.PUBLISHER_DID,
                collection=managed_posts.POST_COLLECTION,
                rkey=uri.split("/")[-1],
            )
        )
        print(f"  deleted {uri}")
    print(f"Deleted {len(candidates)} post(s).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage repository-managed UX posts.")
    parser.add_argument("--handle", default=None, help="Publisher handle or DID to authenticate as")
    parser.add_argument("--app-password", default=None)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Validate content offline")

    resolve = sub.add_parser(
        "resolve", help="Match content against the account, write the manifest"
    )
    resolve.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit non-zero if any managed post is not published yet",
    )

    sync = sub.add_parser("sync", help="Publish anything missing, then write the manifest")
    sync.add_argument("--dry-run", action="store_true")

    sub.add_parser("list", help="Print the resolved mapping")

    cleanup = sub.add_parser("cleanup", help="Delete superseded posts from the account")
    cleanup.add_argument("--older-than-days", type=int, default=DEFAULT_CLEANUP_AGE_DAYS)
    cleanup.add_argument("--yes", action="store_true", help="Actually delete")

    args = parser.parse_args()
    handlers = {
        "check": cmd_check,
        "resolve": cmd_resolve,
        "sync": cmd_sync,
        "list": cmd_list,
        "cleanup": cmd_cleanup,
    }
    sys.exit(handlers[args.command](args))


if __name__ == "__main__":
    main()

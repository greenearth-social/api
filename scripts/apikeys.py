#!/usr/bin/env python3
"""Manage API keys in an explicitly selected Firestore database.

Run from api/: pipenv run python scripts/apikeys.py --environment prod generate EMAIL
Collection api_keys is fixed; --environment selects the database, not a collection.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ID = "greenearth-471522"
# Stage intentionally uses its Cloud Run URL; the old api-stage DNS is not configured.
DESTINATIONS = {
    "stage": ("greenearth-stage", "https://greenearth-api-stage-oef7fsaama-ue.a.run.app"),
    "prod": ("greenearth-prod", "https://api.greenearth.social"),
}


@dataclass(frozen=True)
class Destination:
    environment: str
    database: str
    api_url: str | None


def resolve_destination(args: argparse.Namespace) -> Destination:
    if args.legacy_default:
        if args.command not in {"deduplicate", "list", "usage", "revoke"}:
            raise ValueError("--legacy-default only supports inspection, revoke, and deduplicate")
        destination = Destination("legacy-default", "(default)", None)
    else:
        database, api_url = DESTINATIONS[args.environment]
        destination = Destination(args.environment, database, api_url)
    expected = {
        "GE_FIRESTORE_PROJECT": PROJECT_ID,
        "PROJECT_ID": PROJECT_ID,
        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
        "GE_FIRESTORE_DATABASE": destination.database,
    }
    for name, value in expected.items():
        if os.environ.get(name) and os.environ[name] != value:
            raise ValueError(
                f"{name} conflicts with selected {destination.environment} destination"
            )
    for name in ("GE_FIRESTORE_EMULATOR_HOST", "FIRESTORE_EMULATOR_HOST"):
        if os.environ.get(name):
            raise ValueError(f"{name} must be unset for cloud key administration")
    return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--environment", choices=DESTINATIONS)
    destination.add_argument(
        "--legacy-default",
        action="store_true",
        help="Explicitly inspect/clean the unused legacy (default) database; never issue keys",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="Issue a key only if the owner has no active key")
    gen.add_argument("email")
    gen.add_argument("--admin", action="store_true", help="Green Earth team only")
    gen.add_argument(
        "--scope",
        dest="scopes",
        action="append",
        help="Grant a named scope (repeatable; default is always included)",
    )
    rotate = sub.add_parser(
        "rotate", help="Atomically replace an active key; old key stops working"
    )
    rotate.add_argument("key_id")
    sub.add_parser("list", help="List key metadata (never plaintext or hashes)")
    for command in ("usage", "revoke"):
        sub.add_parser(command).add_argument("key_id")
    cleanup = sub.add_parser("deduplicate", help="Report latest-used keep/revoke decisions")
    cleanup.add_argument(
        "--apply", action="store_true", help="Revoke duplicates and seed owner index"
    )
    cleanup.add_argument(
        "--keep",
        action="append",
        default=[],
        metavar="KEY_ID",
        help="Explicit retained key for an ambiguous owner; repeat for other owners",
    )
    return parser


def report_destination(destination: Destination) -> None:
    print(f"Project     : {PROJECT_ID}")
    print(f"Environment : {destination.environment}")
    print(f"Database    : {destination.database}")
    print("Collection  : api_keys (fixed)")
    print(f"API URL     : {destination.api_url or 'none (legacy database)'}")


async def verify_key(api_url: str, full_key: str) -> bool:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(api_url + "/", headers={"X-API-Key": full_key})
        print(f"Verification: HTTP {response.status_code}")
        return response.status_code == 200
    except httpx.HTTPError as error:
        # Never log a request/headers: they contain the plaintext key.
        print(f"Verification failed: {type(error).__name__}", file=sys.stderr)
        return False


async def execute(args: argparse.Namespace, destination: Destination) -> int:
    from google.cloud.firestore import AsyncClient

    from app.lib import api_keys

    db = AsyncClient(project=PROJECT_ID, database=destination.database)
    try:
        if args.command in {"generate", "rotate"}:
            if args.command == "generate":
                doc, full_key = await api_keys.create_api_key(
                    db, args.email, is_admin=args.admin, scopes=args.scopes
                )
            else:
                doc, full_key = await api_keys.rotate_api_key(db, args.key_id)
            print(f"Key ID      : {doc.key_id}")
            print(f"Email       : {doc.email}")
            print(f"Admin       : {doc.is_admin}")
            print(f"Scopes      : {', '.join(doc.scopes)}")
            print(f"Key         : {full_key}", flush=True)
            print("Save securely: plaintext is displayed once and never stored.", flush=True)
            if not await verify_key(destination.api_url, full_key):
                print(
                    f"Key {doc.key_id} WAS CREATED. "
                    "Do not retry generate; investigate verification. "
                    "If plaintext was lost, explicitly rotate this key.",
                    file=sys.stderr,
                )
                return 1
        elif args.command == "list":
            for doc in await api_keys.list_api_keys(db):
                print(
                    f"{doc.key_id} {doc.email} active={doc.is_active} admin={doc.is_admin} "
                    f"calls={doc.monthly_call_count} period={doc.monthly_period} "
                    f"created={doc.created_at.isoformat()} "
                    f"last_used={doc.last_used_at.isoformat()} "
                    f"scopes={', '.join(doc.scopes)}"
                )
        elif args.command == "usage":
            doc = await api_keys.get_api_key_doc(db, args.key_id)
            if doc is None:
                raise ValueError(f"Key {args.key_id} not found")
            print(doc.model_dump(exclude={"key_hash"}))
        elif args.command == "revoke":
            if not await api_keys.revoke_api_key(db, args.key_id):
                raise ValueError(f"Key {args.key_id} not found")
            print(f"Revoked {args.key_id}")
        else:
            # Metadata only: never expose stored key hashes in reports.
            for doc in await api_keys.list_api_keys(db):
                print(
                    f"{doc.key_id} {doc.email} active={doc.is_active} admin={doc.is_admin} "
                    f"created={doc.created_at.isoformat()} "
                    f"last_used={doc.last_used_at.isoformat()} "
                    f"calls={doc.monthly_call_count} period={doc.monthly_period} "
                    f"scopes={', '.join(doc.scopes)}"
                )
            decisions = await api_keys.reconcile_api_keys(db, apply=args.apply, keep_ids=args.keep)
            print(
                "APPLIED" if args.apply else "DRY RUN (use --apply to revoke and seed owner index)"
            )
            for decision in decisions:
                print(
                    f"{decision.email}: keep={decision.keep_id} revoke={decision.revoke_ids} "
                    f"reason={decision.reason}"
                )
        return 0
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    # Help/argument errors need only the standard library. Load .env before validation,
    # using the same path on every invocation, including outside the repository.
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parents[1] / ".env")
        try:
            destination = resolve_destination(args)
        except ValueError as error:
            parser.error(str(error))
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        os.environ["GE_FIRESTORE_PROJECT"] = PROJECT_ID
        os.environ["GE_FIRESTORE_DATABASE"] = destination.database
        report_destination(destination)
        return asyncio.run(execute(args, destination))
    except ImportError as error:
        print(
            f"Missing CLI dependency: {error.name}. Run pipenv sync --dev (Python 3.13).",
            file=sys.stderr,
        )
        return 1
    except Exception as error:
        # SDK writes can succeed even if the client loses the acknowledgement.
        print(
            f"{type(error).__name__}: {error}. Check key metadata before retrying a mutation.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

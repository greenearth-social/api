#!/usr/bin/env python3
"""Delete a user's data from the Green Earth observability app.

Run from the api/ directory (dry-run is the default):
    pipenv run python scripts/delete_user.py --did did:plc:abc123
    pipenv run python scripts/delete_user.py --did did:plc:abc123 --execute

Defaults to the local Firestore emulator (``--environment dev``). Target a
deployed environment with ``--environment stage`` / ``--environment prod``
(requires GCP credentials, e.g. ``gcloud auth application-default login``);
``--env`` is accepted as an alias:
    pipenv run python scripts/delete_user.py --did did:plc:abc123 --environment stage

Reads Firestore connection from the same env vars as scripts/apikeys.py
(GE_FIRESTORE_PROJECT, GE_FIRESTORE_DATABASE, GE_FIRESTORE_EMULATOR_HOST). Production is
database "greenearth-prod"; ``--environment prod`` sets it explicitly. Firebase Auth uses
Application Default Credentials (or FIREBASE_AUTH_EMULATOR_HOST for the emulator).

This script deletes data only. It does not revoke the user's OAuth grant at the
authorization server — that is added by a later stacked change once the revoke
endpoint exists.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google.cloud.firestore import FieldFilter  # noqa: E402

from app.lib.did import is_valid_did  # noqa: E402
from app.lib.feed_cache import FEED_CACHE_COLLECTION  # noqa: E402
from app.lib.firestore import (  # noqa: E402
    FIRESTORE_WRITE_BATCH_LIMIT,
    INTERACTIONS_COLLECTION,
    USERS_COLLECTION,
    init_firestore_client,
    user_doc_id,
)
from app.lib.followed_users_cache import FOLLOWED_USERS_CACHE_COLLECTION  # noqa: E402
from app.lib.user_history_cache import (  # noqa: E402
    USER_HISTORY_CACHE_COLLECTION,
    _user_history_cache_key,
)

# GCP project + Firestore database per environment. Both environments live in
# the same project and are separated by database (see scripts/gcp_setup.sh).
GCP_PROJECT = "greenearth-471522"
_ENVIRONMENTS = {
    "stage": "greenearth-stage",
    "prod": "greenearth-prod",
}

NOT_DELETED_FROM_CODE = [
    "PostHog persons/events (distinct_id = DID): delete in the PostHog UI/API",
    "Elasticsearch posts/likes/inferences: public firehose mirror, not deleted by this script",
    "Parquet exports / ML training data (engagement-prediction repo): not deleted",
    "Firestore backups / point-in-time recovery: expire per retention policy",
    "Cloud Logging (api and function logs contain the DID): expire per retention policy",
    "feed_cache entries written before user_did was recorded: expire via TTL",
    "Firebase ID tokens already issued stay valid up to 1h and can re-create the user "
    "doc via upsert_user: re-run with --execute after an hour",
]


@dataclass
class StoreResult:
    store: str
    found: int


class FirebaseAuthBackend:
    def __init__(self) -> None:
        from app.lib.firebase_auth import init_firebase_auth

        init_firebase_auth()

    def exists(self, uid: str) -> bool:
        from firebase_admin import auth

        try:
            auth.get_user(uid)
            return True
        except auth.UserNotFoundError:
            return False

    def delete(self, uid: str) -> None:
        from firebase_admin import auth

        auth.revoke_refresh_tokens(uid)
        auth.delete_user(uid)


def _configure_environment(env: str) -> None:
    """Point Firestore at the chosen environment, in-process.

    ``dev`` (the default) leaves the environment untouched so the local
    ``.env`` (Firestore emulator) is used. ``stage``/``prod`` set the project
    and database explicitly and clear any emulator host — done here rather
    than via shell env vars because ``pipenv`` loads ``.env`` over inline vars.
    """
    if env == "dev":
        return
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = _ENVIRONMENTS[env]
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)
    print(f"→ {env} (database {_ENVIRONMENTS[env]})")


async def _count_tree(doc_ref) -> int:
    total = 1 if (await doc_ref.get()).exists else 0
    async for coll in doc_ref.collections():
        async for child in coll.list_documents():
            total += await _count_tree(child)
    return total


async def _delete_tree(db, doc_ref, execute: bool) -> int:
    count = await _count_tree(doc_ref)
    if execute and count:
        await db.recursive_delete(doc_ref)
    return count


async def _delete_doc(doc_ref, execute: bool) -> int:
    if not (await doc_ref.get()).exists:
        return 0
    if execute:
        await doc_ref.delete()
    return 1


async def _delete_query(db, query, execute: bool) -> int:
    count = pending = 0
    batch = db.batch()
    async for snap in query.select([]).stream():
        count += 1
        if execute:
            batch.delete(snap.reference)
            pending += 1
            if pending == FIRESTORE_WRITE_BATCH_LIMIT:
                await batch.commit()
                batch, pending = db.batch(), 0
    if execute and pending:
        await batch.commit()
    return count


async def delete_user_data(db, did: str, execute: bool, auth_backend) -> list[StoreResult]:
    """Delete every store this script owns for one user (dry-run unless ``execute``).

    Deletion order is Firebase Auth first (stops session refresh), then
    Firestore. Not one transaction: Firestore caps transactions at 500 writes
    and the stores span collections; every step is idempotent so a failed run
    is re-runnable.
    """
    key = user_doc_id(did)
    results: list[StoreResult] = []

    auth_exists = await asyncio.to_thread(auth_backend.exists, did)
    if auth_exists and execute:
        await asyncio.to_thread(auth_backend.delete, did)
    results.append(StoreResult("firebase auth user", 1 if auth_exists else 0))

    results.append(
        StoreResult(
            f"firestore {USERS_COLLECTION}/{key} (+ subcollections)",
            await _delete_tree(db, db.collection(USERS_COLLECTION).document(key), execute),
        )
    )
    results.append(
        StoreResult(
            f"firestore {FOLLOWED_USERS_CACHE_COLLECTION}/{key}",
            await _delete_doc(
                db.collection(FOLLOWED_USERS_CACHE_COLLECTION).document(key), execute
            ),
        )
    )
    results.append(
        StoreResult(
            f"firestore {USER_HISTORY_CACHE_COLLECTION}/{_user_history_cache_key(did)}",
            await _delete_doc(
                db.collection(USER_HISTORY_CACHE_COLLECTION).document(
                    _user_history_cache_key(did)
                ),
                execute,
            ),
        )
    )
    for name in (INTERACTIONS_COLLECTION, FEED_CACHE_COLLECTION):
        query = db.collection(name).where(filter=FieldFilter("user_did", "==", did))
        results.append(
            StoreResult(f"firestore {name} where user_did", await _delete_query(db, query, execute))
        )
    return results


def format_report(did: str, execute: bool, results: list[StoreResult]) -> str:
    verb = "deleted" if execute else "would delete"
    lines = [f"{'EXECUTE' if execute else 'DRY RUN'} for {did}", ""]
    lines += [f"  {r.store:<55} {r.found:>6} {verb if r.found else 'nothing to delete'}" for r in results]
    lines += ["", "NOT deleted by this script (handle separately):"]
    lines += [f"  - {item}" for item in NOT_DELETED_FROM_CODE]
    lines += ["  - OAuth grant at the authorization server: NOT revoked by this script"]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete a user's data (dry-run by default).")
    parser.add_argument("--did", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="report only (default)")
    mode.add_argument("--execute", action="store_true", help="actually delete")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["dev", "stage", "prod"],
        default="dev",
        help="Target environment: dev uses the local Firestore emulator (default); "
        "stage/prod connect to the corresponding Firestore database",
    )
    args = parser.parse_args(argv)
    if not is_valid_did(args.did):
        parser.error(f"--did is not a well-formed DID: {args.did!r}")
    return args


async def run(args: argparse.Namespace, db, auth_backend) -> int:
    try:
        results = await delete_user_data(db, args.did, args.execute, auth_backend)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(format_report(args.did, args.execute, results))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _configure_environment(args.environment)
    print(
        "Target: project="
        f"{os.environ.get('GE_FIRESTORE_PROJECT', os.environ.get('PROJECT_ID', '(default)'))} "
        f"database={os.environ.get('GE_FIRESTORE_DATABASE', '(default)')} "
        f"emulator={os.environ.get('GE_FIRESTORE_EMULATOR_HOST') or 'no'}"
    )
    return asyncio.run(run(args, init_firestore_client(), FirebaseAuthBackend()))


if __name__ == "__main__":
    sys.exit(main())

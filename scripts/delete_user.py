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

Before deleting anything, this script revokes the user's OAuth grant by
calling this api's own ``POST /api/oauth/revoke`` endpoint with an admin
``X-API-Key``. That requires two environment variables when ``--execute``
needs to revoke (i.e. unless ``--skip-oauth-revocation`` applies):

    GE_API_URL          base URL of the api to call, e.g. https://greenearth-api-stage-...run.app
    GE_ADMIN_API_KEY     an admin gea_... key (never logged or printed)

If revocation reports outcome ``failed``, no data is deleted and the script
exits non-zero — re-running is always safe, including after a ``failed``
caused by a concurrent login that replaced the grant mid-revoke.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import httpx  # noqa: E402
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

OAUTH_GRANTS_COLLECTION = "oauth_grants"
REVOKE_TIMEOUT_SECONDS = 30.0
SUCCESS_OUTCOMES = frozenset({"revoked", "already_revoked", "no_session"})

NOT_DELETED_FROM_CODE = [
    "PostHog persons/events (distinct_id = DID): delete in the PostHog UI/API",
    "Elasticsearch posts/likes/inferences: public firehose mirror, not deleted by this script",
    "Parquet exports / ML training data (engagement-prediction repo): not deleted",
    "Firestore backups / point-in-time recovery: expire per retention policy",
    "Cloud Logging (api and function logs contain the DID): expire per retention policy",
    "feed_cache entries written before user_did was recorded: expire via TTL",
    "Firebase ID tokens already issued stay valid up to 1h and can re-create the user "
    "doc via upsert_user: re-run with --execute after an hour",
    "OAuth grants created before revocation shipped have no stored token and cannot be "
    "revoked from code: the user must revoke the app in Bluesky settings",
    "oauth_revocations audit entries (DID, actor, outcome, timestamp): retained by design",
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


async def grant_state(db, did: str) -> str:
    """Read the stored OAuth grant doc's status: "active", "revoked", or "absent"."""
    snap = await db.collection(OAUTH_GRANTS_COLLECTION).document(did).get()
    if not snap.exists:
        return "absent"
    return "revoked" if (snap.to_dict() or {}).get("status") == "revoked" else "active"


async def revoke_via_api(did: str, client: httpx.AsyncClient | None = None) -> str:
    """Revoke ``did``'s OAuth grant via this api's own admin-authenticated endpoint.

    Never raises: any failure (missing config, transport error, unexpected
    status/body) maps to outcome "failed", same as the endpoint's own 502.
    """
    base, key = os.environ.get("GE_API_URL"), os.environ.get("GE_ADMIN_API_KEY")
    if not base or not key:
        return "failed"
    http = client or httpx.AsyncClient()
    try:
        response = await http.post(
            f"{base.rstrip('/')}/api/oauth/revoke",
            json={"did": did},
            headers={"X-API-Key": key},
            timeout=REVOKE_TIMEOUT_SECONDS,
        )
        outcome = response.json().get("outcome") if response.status_code in (200, 502) else None
        return outcome if response.status_code == 200 and outcome in SUCCESS_OUTCOMES else "failed"
    except Exception:  # noqa: BLE001
        return "failed"
    finally:
        await http.aclose()


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
    results.append(
        StoreResult(
            f"firestore {OAUTH_GRANTS_COLLECTION}/{did}",
            await _delete_doc(db.collection(OAUTH_GRANTS_COLLECTION).document(did), execute),
        )
    )
    return results


def format_report(did: str, execute: bool, results: list[StoreResult], grant_line: str) -> str:
    verb = "deleted" if execute else "would delete"
    lines = [f"{'EXECUTE' if execute else 'DRY RUN'} for {did}", ""]
    lines += [f"  {r.store:<55} {r.found:>6} {verb if r.found else 'nothing to delete'}" for r in results]
    lines += ["", f"OAuth grant: {grant_line}"]
    lines += ["", "NOT deleted by this script (handle separately):"]
    lines += [f"  - {item}" for item in NOT_DELETED_FROM_CODE]
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
    parser.add_argument(
        "--skip-oauth-revocation",
        action="store_true",
        help="skip revocation; only allowed when no active grant is stored",
    )
    args = parser.parse_args(argv)
    if not is_valid_did(args.did):
        parser.error(f"--did is not a well-formed DID: {args.did!r}")
    return args


async def run(args: argparse.Namespace, db, auth_backend, revoker=revoke_via_api) -> int:
    try:
        state = await grant_state(db, args.did)
        if args.skip_oauth_revocation and state == "active":
            print(
                "ERROR: an active OAuth grant is stored for this DID; refusing "
                "--skip-oauth-revocation. Run without it so the grant is revoked first.",
                file=sys.stderr,
            )
            return 1
        if not args.execute:
            grant_line = {
                "active": "would revoke (active grant)",
                "revoked": "already revoked",
                "absent": "none stored",
            }[state]
        elif args.skip_oauth_revocation:
            grant_line = f"revocation skipped (grant {state})"
        else:
            outcome = await revoker(args.did)
            if outcome not in SUCCESS_OUTCOMES:
                print(
                    "ERROR: OAuth revocation failed; no data was deleted. Fix and re-run "
                    "(re-running is always safe).",
                    file=sys.stderr,
                )
                return 1
            grant_line = outcome
        results = await delete_user_data(db, args.did, args.execute, auth_backend)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(format_report(args.did, args.execute, results, grant_line))
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

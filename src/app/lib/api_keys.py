"""API key generation, verification, and Firestore CRUD.

Key format: gea_<8-char key_id><48-char secret>
  - key_id  : 4 random bytes as hex (8 chars)
  - secret  : 24 random bytes as hex (48 chars)
  - total   : 60 chars including "gea_" prefix

Storage: only the SHA-256 hash is written to Firestore.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

from google.cloud.firestore import (  # type: ignore[import-untyped]
    AsyncClient,  # type: ignore[import-untyped]
    Increment,
    async_transactional,
)

from ..api_key_documents import ApiKeyDocument

logger = logging.getLogger(__name__)

KEY_PREFIX = "gea_"
KEY_ID_LEN = 8
SECRET_LEN = 48
FULL_KEY_LEN = len(KEY_PREFIX) + KEY_ID_LEN + SECRET_LEN  # 60

API_KEYS_COLLECTION = "api_keys"
API_KEY_OWNERS_COLLECTION = "api_key_owners"


def normalize_email(email: str) -> str:
    """Owner identity is trimmed and case-insensitive (no provider-specific alias rules)."""
    email = email.strip().lower()
    parts = email.split("@")
    if len(parts) != 2 or not all(parts) or any(c.isspace() for c in email):
        raise ValueError("Invalid owner email")
    return email


def owner_id(email: str) -> str:
    return hashlib.sha256(normalize_email(email).encode()).hexdigest()


def _owner_ref(db, email):
    return db.collection(API_KEY_OWNERS_COLLECTION).document(owner_id(email))


async def _read_keys(db, transaction=None) -> list[ApiKeyDocument]:
    # Administrative collections are small. Scan legacy records too: pre-index emails
    # can contain case/whitespace differences, so an equality query is insufficient.
    results = []
    async for snap in db.collection(API_KEYS_COLLECTION).stream(transaction=transaction):
        data = snap.to_dict()
        if data is not None:
            doc = ApiKeyDocument.model_validate(data)
            if doc.key_id != snap.id:
                raise ValueError(f"Key ID mismatch in record {snap.id}")
            results.append(doc)
    return results


def _active_for_owner(records, email):
    return [doc for doc in records if doc.is_active and normalize_email(doc.email) == email]


def _write_owner(transaction, ref, email, key_id):
    transaction.set(ref, {"email": email, "active_key_id": key_id, "updated_at": datetime.now(UTC)})


def _new_key(email, is_admin, scopes=None):
    key_id, full_key, key_hash = generate_key()
    now = datetime.now(UTC)
    return ApiKeyDocument(
        key_id=key_id,
        key_hash=key_hash,
        email=email,
        is_admin=is_admin,
        scopes=scopes if scopes is not None else [],
        created_at=now,
        last_used_at=now,
        monthly_period=now.strftime("%Y-%m"),
    ), full_key


@dataclass(frozen=True)
class CleanupDecision:
    email: str
    keep_id: str
    revoke_ids: tuple[str, ...]
    reason: str


def choose_retained_key(records: list[ApiKeyDocument], keep_ids=()) -> ApiKeyDocument:
    """Choose latest *used* key; never treat creation-time last_used_at as usage."""
    selected = [doc for doc in records if doc.key_id in keep_ids]
    if len(selected) > 1:
        raise ValueError("Only one --keep key is allowed per owner")
    if selected:
        return selected[0]
    if len(records) == 1:
        return records[0]
    if not records:
        raise ValueError("No active keys to retain")
    email = normalize_email(records[0].email)
    detail = (
        f"{email}: ambiguous active keys {[doc.key_id for doc in records]}; specify --keep KEY_ID"
    )
    if len({(doc.is_admin, frozenset(doc.scopes)) for doc in records}) > 1:
        raise ValueError(detail + " (different privileges)")
    if any(not {"created_at", "last_used_at"} <= doc.model_fields_set for doc in records):
        raise ValueError(detail + " (missing timestamp history)")
    used = [
        doc for doc in records if doc.monthly_call_count > 0 or doc.last_used_at > doc.created_at
    ]
    if not used:
        raise ValueError(detail + " (no recorded successful usage)")
    latest_time = max(doc.last_used_at for doc in used)
    latest = [doc for doc in used if doc.last_used_at == latest_time]
    if len(latest) != 1:
        raise ValueError(detail + " (tied last-used timestamps)")
    return latest[0]


# ---------------------------------------------------------------------------
# Pure functions (no I/O)
# ---------------------------------------------------------------------------


def generate_key() -> tuple[str, str, str]:
    """Return (key_id, full_key, key_hash). Call once; discard full_key after display."""
    key_id = os.urandom(4).hex()
    secret = os.urandom(24).hex()
    full_key = f"{KEY_PREFIX}{key_id}{secret}"
    key_hash = _hash_key(full_key)
    return key_id, full_key, key_hash


def _hash_key(full_key: str) -> str:
    return hashlib.sha256(full_key.encode()).hexdigest()


def parse_key_id(full_key: str | None) -> str | None:
    """Extract key_id from a full API key string, or None if the format is invalid."""
    if not full_key:
        return None
    if not full_key.startswith(KEY_PREFIX):
        return None
    if len(full_key) != FULL_KEY_LEN:
        return None
    return full_key[len(KEY_PREFIX) : len(KEY_PREFIX) + KEY_ID_LEN]


# ---------------------------------------------------------------------------
# Firestore operations
# ---------------------------------------------------------------------------


async def get_api_key_doc(db: AsyncClient, key_id: str) -> ApiKeyDocument | None:
    """Fetch an API key document by key_id, or None if not found."""
    doc = await db.collection(API_KEYS_COLLECTION).document(key_id).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return ApiKeyDocument.model_validate(data)


async def create_api_key(
    db: AsyncClient, email: str, is_admin: bool = False, *, scopes: list[str] | None = None
) -> tuple[ApiKeyDocument, str]:
    """Create one active key per normalized owner, including legacy unindexed keys."""
    email = normalize_email(email)
    doc, full_key = _new_key(email, is_admin, scopes)
    owner = _owner_ref(db, email)

    @async_transactional
    async def create(transaction):
        await owner.get(transaction=transaction)
        active = _active_for_owner(await _read_keys(db, transaction), email)
        if active:
            raise ValueError(
                f"Active keys already exist: {[d.key_id for d in active]}; "
                "use rotate KEY_ID (deduplicate first if needed)"
            )
        transaction.create(
            db.collection(API_KEYS_COLLECTION).document(doc.key_id), doc.model_dump()
        )
        _write_owner(transaction, owner, email, doc.key_id)

    await create(db.transaction())
    logger.info("Created API key %s", doc.key_id)
    return doc, full_key


async def rotate_api_key(db: AsyncClient, key_id: str) -> tuple[ApiKeyDocument, str]:
    """Replace the identified active key atomically, preserving its privileges."""
    old = await get_api_key_doc(db, key_id)
    if old is None:
        raise ValueError(f"Key {key_id} not found")
    email = normalize_email(old.email)
    owner = _owner_ref(db, email)
    replacement, full_key = _new_key(email, old.is_admin, old.scopes)

    @async_transactional
    async def rotate(transaction):
        await owner.get(transaction=transaction)
        active = _active_for_owner(await _read_keys(db, transaction), email)
        if len(active) != 1 or active[0].key_id != key_id:
            raise ValueError(f"Key {key_id} is not the sole active key; inspect/deduplicate first")
        # Revalidate privileges inside the transaction; don't rotate a stale snapshot.
        if active[0].is_admin != replacement.is_admin or active[0].scopes != replacement.scopes:
            raise ValueError("Key privileges changed; inspect before retrying rotation")
        transaction.update(
            db.collection(API_KEYS_COLLECTION).document(key_id), {"is_active": False}
        )
        transaction.create(
            db.collection(API_KEYS_COLLECTION).document(replacement.key_id),
            replacement.model_dump(),
        )
        _write_owner(transaction, owner, email, replacement.key_id)

    await rotate(db.transaction())
    return replacement, full_key


async def authenticate_api_key(db: AsyncClient, full_key: str | None) -> ApiKeyDocument | None:
    """Validate a full API key string. Returns the document if valid, None otherwise.

    On success, atomically increments monthly_call_count and updates last_used_at.
    Resets the counter when the calendar month changes.
    """
    if full_key is None:
        return None
    key_id = parse_key_id(full_key)
    if key_id is None:
        return None

    doc = await get_api_key_doc(db, key_id)
    if doc is None or not doc.is_active:
        return None

    expected_hash = _hash_key(full_key)
    if not hmac.compare_digest(expected_hash, doc.key_hash):
        return None

    now = datetime.now(UTC)
    current_period = now.strftime("%Y-%m")
    ref = db.collection(API_KEYS_COLLECTION).document(key_id)

    if doc.monthly_period != current_period:
        await ref.update(
            {
                "last_used_at": now,
                "monthly_call_count": 1,
                "monthly_period": current_period,
            }
        )
    else:
        await ref.update(
            {
                "last_used_at": now,
                "monthly_call_count": Increment(1),
            }
        )

    return doc


async def list_api_keys(db: AsyncClient) -> list[ApiKeyDocument]:
    """Return all API key documents, including inactive audit records."""
    return await _read_keys(db)


async def revoke_api_key(db: AsyncClient, key_id: str) -> bool:
    """Revoke the key and synchronize the owner index in the same transaction."""
    old = await get_api_key_doc(db, key_id)
    if old is None:
        return False
    email = normalize_email(old.email)
    owner = _owner_ref(db, email)

    @async_transactional
    async def revoke(transaction):
        await owner.get(transaction=transaction)
        records = await _read_keys(db, transaction)
        current = next((doc for doc in records if doc.key_id == key_id), None)
        if current is None:
            return False
        if normalize_email(current.email) != email:
            raise ValueError("Key owner changed; inspect before retrying revoke")
        remaining = [doc for doc in _active_for_owner(records, email) if doc.key_id != key_id]
        transaction.update(
            db.collection(API_KEYS_COLLECTION).document(key_id), {"is_active": False}
        )
        _write_owner(
            transaction, owner, email, remaining[0].key_id if len(remaining) == 1 else None
        )
        return True

    return await revoke(db.transaction())


async def reconcile_api_keys(db: AsyncClient, *, apply=False, keep_ids=()) -> list[CleanupDecision]:
    """Read-only report by default; apply recomputes choices in an atomic transaction.

    All owner reads precede writes. A single bounded transaction avoids partial
    cleanup when another owner is ambiguous, and retries on intervening usage.
    """
    keep_ids = set(keep_ids)

    async def reconcile(transaction=None):
        records = await _read_keys(db, transaction)
        active = [doc for doc in records if doc.is_active]
        if keep_ids - {doc.key_id for doc in active}:
            raise ValueError("--keep must identify an existing active key in the selected database")
        groups = defaultdict(list)
        for doc in active:
            groups[normalize_email(doc.email)].append(doc)
        decisions = []
        for email, docs in sorted(groups.items()):
            retained = choose_retained_key(docs, keep_ids)
            decisions.append(
                CleanupDecision(
                    email,
                    retained.key_id,
                    tuple(sorted(doc.key_id for doc in docs if doc.key_id != retained.key_id)),
                    "explicit selection"
                    if retained.key_id in keep_ids
                    else "sole active key"
                    if len(docs) == 1
                    else "latest successful usage",
                )
            )
        if transaction is not None:
            writes = len(decisions) + sum(len(d.revoke_ids) for d in decisions)
            if writes > 450:
                raise ValueError("Cleanup exceeds safe transaction size; split the migration")
            for decision in decisions:
                await _owner_ref(db, decision.email).get(transaction=transaction)
            for decision in decisions:
                for key_id in decision.revoke_ids:
                    transaction.update(
                        db.collection(API_KEYS_COLLECTION).document(key_id), {"is_active": False}
                    )
                _write_owner(
                    transaction, _owner_ref(db, decision.email), decision.email, decision.keep_id
                )
        return decisions

    if not apply:
        return await reconcile()
    return await async_transactional(reconcile)(db.transaction())

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
from datetime import datetime, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]
from google.cloud.firestore import Increment  # type: ignore[import-untyped]

from ..documents import ApiKeyDocument

logger = logging.getLogger(__name__)

KEY_PREFIX = "gea_"
KEY_ID_LEN = 8
SECRET_LEN = 48
FULL_KEY_LEN = len(KEY_PREFIX) + KEY_ID_LEN + SECRET_LEN  # 60

API_KEYS_COLLECTION = "api_keys"


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
    return full_key[len(KEY_PREFIX): len(KEY_PREFIX) + KEY_ID_LEN]


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


async def create_api_key(db: AsyncClient, email: str) -> tuple[ApiKeyDocument, str]:
    """Issue a new API key. Returns (document, plaintext_full_key).

    The plaintext key is returned exactly once and never stored.
    """
    key_id, full_key, key_hash = generate_key()
    now = datetime.now(timezone.utc)
    doc = ApiKeyDocument(
        key_id=key_id,
        key_hash=key_hash,
        email=email,
        is_active=True,
        created_at=now,
        last_used_at=now,
        monthly_call_count=0,
        monthly_period=now.strftime("%Y-%m"),
    )
    await db.collection(API_KEYS_COLLECTION).document(key_id).set(doc.model_dump())
    logger.info("Created API key %s for %s", key_id, email)
    return doc, full_key


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

    now = datetime.now(timezone.utc)
    current_period = now.strftime("%Y-%m")
    ref = db.collection(API_KEYS_COLLECTION).document(key_id)

    if doc.monthly_period != current_period:
        await ref.update({
            "last_used_at": now,
            "monthly_call_count": 1,
            "monthly_period": current_period,
        })
    else:
        await ref.update({
            "last_used_at": now,
            "monthly_call_count": Increment(1),
        })

    return doc


async def list_api_keys(db: AsyncClient) -> list[ApiKeyDocument]:
    """Return all API key documents."""
    results: list[ApiKeyDocument] = []
    async for snap in db.collection(API_KEYS_COLLECTION).stream():
        data = snap.to_dict()
        if data is not None:
            results.append(ApiKeyDocument.model_validate(data))
    return results


async def revoke_api_key(db: AsyncClient, key_id: str) -> bool:
    """Set is_active = False. Returns True if the key existed, False otherwise."""
    ref = db.collection(API_KEYS_COLLECTION).document(key_id)
    doc = await ref.get()
    if not doc.exists:
        return False
    await ref.update({"is_active": False})
    logger.info("Revoked API key %s", key_id)
    return True

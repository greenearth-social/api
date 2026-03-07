"""Firestore helpers for typed document access.

Provides ``init_firestore_client`` for application startup and thin typed
wrappers around common Firestore operations.  Each wrapper accepts and
returns Pydantic document models so callers never deal with raw dicts.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from google.cloud.firestore import AsyncClient  # type: ignore[import-untyped]

from ..documents import UserDocument

logger = logging.getLogger(__name__)

USERS_COLLECTION = "users"


def init_firestore_client() -> AsyncClient:
    """Create an async Firestore client.

    When ``GE_FIRESTORE_EMULATOR_HOST`` is set, the client connects to the
    local emulator instead of production Firestore.  The Google SDK
    natively reads ``FIRESTORE_EMULATOR_HOST``, so we copy the GE-prefixed
    variable into that standard name before creating the client.
    """
    emulator_host = os.environ.get("GE_FIRESTORE_EMULATOR_HOST")
    if emulator_host:
        os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host
        logger.info("Firestore emulator configured at %s", emulator_host)

    project = os.environ.get("GE_FIRESTORE_PROJECT", os.environ.get("PROJECT_ID"))
    database = os.environ.get("GE_FIRESTORE_DATABASE", "(default)")
    return AsyncClient(project=project, database=database)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


async def get_user(db: AsyncClient, user_did: str) -> UserDocument | None:
    """Fetch a user document by DID, or return ``None`` if not found."""
    doc = await db.collection(USERS_COLLECTION).document(user_did).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data is None:
        return None
    return UserDocument.model_validate(data)


async def upsert_user(db: AsyncClient, user_did: str) -> UserDocument:
    """Create or update a user document.

    On first visit the document is created with all timestamps set to now.
    On subsequent visits only ``last_seen_at`` is refreshed.  ``updated_at``
    is reserved for changes to the user's actual data fields.
    """
    ref = db.collection(USERS_COLLECTION).document(user_did)
    doc = await ref.get()

    now = datetime.now(timezone.utc)

    # TODO: update updated_at if the document is actually different, once we have more data fields

    if doc.exists:
        await ref.update({"last_seen_at": now})
        data = doc.to_dict()
        if data is None:
            raise ValueError(f"Firestore document exists but to_dict() returned None for {user_did}")
        data["last_seen_at"] = now
        return UserDocument.model_validate(data)

    user = UserDocument(user_did=user_did, created_at=now, updated_at=now, last_seen_at=now)
    await ref.set(user.model_dump())
    return user

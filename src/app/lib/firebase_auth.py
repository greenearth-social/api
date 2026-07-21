"""Firebase Auth token verification for the feed-debug transparency API.

Provides ``init_firebase_auth`` (called once at app startup) and
``verify_firebase_auth`` (FastAPI dependency) so a separate frontend can
authenticate with Firebase custom tokens and read its own feed-debug data.

Firebase token flow
-------------------
1. Frontend sends ``Authorization: Bearer <firebaseCustomToken>``.
2. ``firebase_admin.auth.verify_id_token(token)`` returns ``decoded.uid``
   which is the full DID (e.g. ``did:plc:abc123``).
3. ``user_doc_id(uid)`` strips the ``did:plc:`` prefix, yielding the
   Firestore document key (``abc123``).
4. The dependency returns the stripped document key so callers can
   construct Firestore paths directly.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import firebase_admin  # type: ignore[import-untyped]
from firebase_admin import auth, credentials  # type: ignore[import-untyped]
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .firestore import user_doc_id

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)


def init_firebase_auth() -> None:
    """Initialize the Firebase Admin SDK.  No-op if already initialized.

    Uses Application Default Credentials (same as Firestore).  Must be
    called once during app startup (lifespan) or the first call to
    ``verify_firebase_auth`` will fail.
    """
    if firebase_admin._apps:
        return
    project = os.environ.get("GE_FIRESTORE_PROJECT", os.environ.get("PROJECT_ID", ""))
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, options={"projectId": project} if project else None)
    logger.info("Firebase Admin SDK initialized for project %s", project or "(default)")


async def verify_firebase_auth(
    authorization: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> str:
    """FastAPI dependency: verify a Firebase ID token and return the Firestore
    user-document key.

    Returns
    -------
    str
        The stripped user document ID (DID without ``did:plc:`` prefix).

    Raises
    ------
    HTTPException 401
        When the header is missing, the token is invalid/expired, or the
        token's ``uid`` does not start with ``did:plc:``.
    """
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
        )

    try:
        decoded = auth.verify_id_token(authorization.credentials)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    uid: str = decoded.get("uid", "")
    if not uid or not uid.startswith("did:plc:"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing valid DID",
        )

    return user_doc_id(uid)


FirebaseUser = Annotated[str, Depends(verify_firebase_auth)]

"""AT Protocol inter-service JWT authentication.

When Bluesky's AppView calls ``getFeedSkeleton`` it includes an
``Authorization: Bearer <jwt>`` header.  The JWT is signed by the
requesting user's repo signing key.  Verifying it proves the caller's
identity and gives us the user's DID (``iss`` claim).

Typical usage inside a FastAPI endpoint::

    user_did = await verify_auth_header(request)
    # user_did is "" when no Authorization header is present

This module relies on the *atproto* Python SDK for JWT verification and
DID resolution (``atproto_server``, ``atproto_identity``).
"""

import logging
from typing import Optional

from atproto_identity.cache.in_memory_cache import AsyncDidInMemoryCache
from atproto_identity.resolver import AsyncIdResolver
from atproto_server.auth.jwt import verify_jwt_async
from atproto_server.exceptions import (
    InvalidTokenError,
    TokenDecodeError,
    TokenExpiredSignatureError,
    TokenInvalidAudienceError,
    TokenInvalidSignatureError,
)
from fastapi import Request

logger = logging.getLogger(__name__)

BEARER_PREFIX = "Bearer "

# ---------------------------------------------------------------------------
# Resolver lifecycle
# ---------------------------------------------------------------------------

# The resolver is initialised once at startup (via ``init_id_resolver``)
# and attached to ``app.state.id_resolver``.  Using an in-memory cache
# avoids redundant DID document fetches for the same user across requests.

def init_id_resolver() -> AsyncIdResolver:
    """Create an ``AsyncIdResolver`` with an in-memory DID cache."""
    cache = AsyncDidInMemoryCache()
    return AsyncIdResolver(cache=cache)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

async def verify_auth_header(
    request: Request,
    service_did: Optional[str] = None,
) -> str:
    """Extract and verify the AT Protocol JWT from the request.

    Parameters
    ----------
    request:
        The incoming FastAPI ``Request``.
    service_did:
        The DID of this feed generator (the expected ``aud`` claim).
        When ``None`` the audience is not checked.

    Returns
    -------
    str
        The requester's DID (``iss`` claim) if a valid JWT was provided,
        or ``""`` if no ``Authorization`` header is present (allows the
        feed to degrade gracefully to an unauthenticated response).
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return ""

    if not auth_header.startswith(BEARER_PREFIX):
        logger.warning("Authorization header present but not Bearer scheme")
        return ""

    token = auth_header[len(BEARER_PREFIX):].strip()
    if not token:
        return ""

    resolver: AsyncIdResolver | None = getattr(
        request.app.state, "id_resolver", None
    )
    if resolver is None:
        logger.error("id_resolver not initialised on app.state")
        return ""

    try:
        payload = await verify_jwt_async(
            token,
            resolver.did.resolve_atproto_key,
            own_did=service_did,
        )
        user_did = payload.iss or ""
        logger.debug("Authenticated feed request from %s", user_did)
        return user_did
    except TokenExpiredSignatureError:
        logger.warning("JWT expired")
    except TokenInvalidAudienceError:
        logger.warning("JWT audience mismatch (expected %s)", service_did)
    except TokenInvalidSignatureError:
        logger.warning("JWT signature invalid")
    except (TokenDecodeError, InvalidTokenError) as exc:
        logger.warning("JWT verification failed: %s", exc)
    except Exception:
        logger.exception("Unexpected error verifying JWT")

    # Auth failed — treat as unauthenticated rather than rejecting the request.
    return ""

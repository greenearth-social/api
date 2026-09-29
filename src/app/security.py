from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from .documents import ApiKeyDocument
from .lib.api_keys import authenticate_api_key
from .lib.firebase_auth import uid_from_credentials

API_KEY_HEADER_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)
_bearer = HTTPBearer(auto_error=False)


async def _authenticate(request: Request, api_key: str | None) -> ApiKeyDocument:
    db = request.app.state.firestore
    doc = await authenticate_api_key(db, api_key)
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return doc


async def verify_api_key(
    request: Request,
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> str:
    doc = await _authenticate(request, api_key)
    return doc.key_id


async def verify_admin_api_key(
    request: Request,
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> str:
    doc = await _authenticate(request, api_key)
    if not doc.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin API key required",
        )
    return doc.key_id


RequireApiKey = Annotated[str, Depends(verify_api_key)]
RequireAdminApiKey = Annotated[str, Depends(verify_admin_api_key)]


@dataclass(frozen=True)
class Caller:
    kind: Literal["admin", "user"]
    identity: str

    @property
    def actor(self) -> str:
        return f"{self.kind}:{self.identity}"


async def verify_admin_or_user(
    request: Request,
    api_key: Annotated[str | None, Depends(api_key_header)],
    authorization: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> Caller:
    """FastAPI dependency: resolve *who* is calling, admin API key or Firebase user.

    An API key always takes precedence and never falls back to the bearer
    token if invalid. Callers must separately enforce "a user may only act on
    their own DID" since this dependency cannot see the request body.
    """
    if api_key is not None:
        doc = await _authenticate(request, api_key)
        if not doc.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin API key required",
            )
        return Caller("admin", doc.key_id)
    return Caller("user", uid_from_credentials(authorization))


RequireAdminOrUser = Annotated[Caller, Depends(verify_admin_or_user)]

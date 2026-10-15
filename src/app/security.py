from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from .documents import ApiKeyDocument
from .lib.api_keys import authenticate_api_key

API_KEY_HEADER_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


async def _authenticate(
    request: Request,
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> ApiKeyDocument:
    # FastAPI reuses this result within a request, so multiple permission checks
    # authenticate the key and increment its usage counter only once.
    db = request.app.state.firestore
    doc = await authenticate_api_key(db, api_key)
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return doc


def require_api_scope(scope: str = "default") -> Callable[[ApiKeyDocument], Awaitable[str]]:
    """Build an endpoint dependency requiring a named API-key scope."""
    async def verify_scope(
        doc: Annotated[ApiKeyDocument, Depends(_authenticate)],
    ) -> str:
        if scope not in doc.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API key missing required scope: {scope}",
            )
        return doc.key_id

    return verify_scope


verify_api_key = require_api_scope()


async def verify_admin_api_key(
    doc: Annotated[ApiKeyDocument, Depends(_authenticate)],
) -> str:
    if not doc.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin API key required",
        )
    return doc.key_id


RequireApiKey = Annotated[str, Depends(verify_api_key)]
RequireAdminApiKey = Annotated[str, Depends(verify_admin_api_key)]

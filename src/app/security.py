from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from .documents import ApiKeyDocument
from .lib.api_keys import authenticate_api_key

API_KEY_HEADER_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


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

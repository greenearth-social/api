from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from .lib.api_keys import authenticate_api_key

API_KEY_HEADER_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


async def verify_api_key(
    request: Request,
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> str:
    db = request.app.state.firestore
    doc = await authenticate_api_key(db, api_key)
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return doc.key_id


RequireApiKey = Annotated[str, Depends(verify_api_key)]

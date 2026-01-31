import os
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def get_api_key() -> str | None:
    return os.environ.get("API_KEY")


async def verify_api_key(
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> str | None:
    expected_key = get_api_key()
    if not expected_key or api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return api_key


RequireApiKey = Annotated[str, Depends(verify_api_key)]

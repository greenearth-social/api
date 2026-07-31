import os

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str = Field(..., description="'ok' when the service is healthy")
    git_sha: str | None = Field(
        None,
        description="Short git sha of the deployed code (from GE_GIT_SHA), or null "
        "when running unstamped code (e.g. local dev). Useful for confirming which "
        "revision is live and for choosing rollback targets.",
    )


@router.get("/health", response_model=HealthResponse, status_code=200)
async def healthcheck() -> HealthResponse:
    """Returns 200 when the service is running."""
    return HealthResponse(status="ok", git_sha=os.environ.get("GE_GIT_SHA") or None)

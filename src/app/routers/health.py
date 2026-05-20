from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str = Field(..., description="'ok' when the service is healthy")


@router.get("/health", response_model=HealthResponse, status_code=200)
async def healthcheck() -> HealthResponse:
    """Returns 200 when the service is running."""
    return HealthResponse(status="ok")

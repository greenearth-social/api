from fastapi import FastAPI

from .routers import health
from .security import RequireApiKey

app = FastAPI(
    title="Green Earth API",
    description="An API server for handling bluesky content recommendation requests",
    version="0.1.0",
)

app.include_router(health.router)


@app.get("/")
async def root(_api_key: RequireApiKey):
    return {"message": "Green Earth API"}

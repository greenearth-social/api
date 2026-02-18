import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .routers import candidates, health, skylight
from .security import RequireApiKey

from elasticsearch import AsyncElasticsearch


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler that validates required environment variables and
    constructs an `AsyncElasticsearch` client attached to `app.state.es`.

    The client is closed on shutdown.
    """
    if not os.environ.get("GE_ELASTICSEARCH_API_KEY"):
        raise RuntimeError("GE_ELASTICSEARCH_API_KEY environment variable is required")

    es_url = os.environ.get("GE_ELASTICSEARCH_URL", "https://localhost:9200")
    es_api_key = os.environ.get("GE_ELASTICSEARCH_API_KEY")
    es_verify = os.environ.get("GE_ELASTICSEARCH_VERIFY_SSL", "false").lower() in (
        "1",
        "true",
        "yes",
    )

    es = AsyncElasticsearch(
        hosts=[es_url],
        api_key=es_api_key,
        verify_certs=es_verify,
        request_timeout=20,
    )

    app.state.es = es
    try:
        yield
    finally:
        try:
            await es.close()
        except Exception:
            pass


app = FastAPI(
    title="Green Earth API",
    description="An API server for handling bluesky content recommendation requests",
    version="0.1.0",
    lifespan=lifespan,
)


app.include_router(candidates.router)
app.include_router(health.router)
app.include_router(skylight.router)


@app.get("/")
async def root(_api_key: RequireApiKey):
    return {"message": "Green Earth API"}

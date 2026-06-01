import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

# Configure the root logger so app modules' messages reach stderr. Uvicorn only
# configures handlers on its own loggers (uvicorn.error, uvicorn.access), so
# without this our `timed()` spans and `profile_written` lines are silently
# dropped. Uvicorn's access logging is independent and keeps emitting per-request
# lines at its own level regardless of the root level set here.
def _default_log_level() -> str:
    """Pick a default level based on the deployment environment.

    Stage and prod default to WARNING so INFO spam (timed spans, HTTP client
    INFO, ES transport INFO) is suppressed in deployed services while warnings
    and errors still surface. Local dev and tests default to INFO.
    ``GE_LOG_LEVEL`` overrides this when set.
    """
    env = (os.environ.get("ENVIRONMENT") or os.environ.get("GE_ENVIRONMENT") or "").strip().lower()
    return "WARNING" if env in ("prod", "production", "stage", "staging") else "INFO"


logging.basicConfig(
    level=os.environ.get("GE_LOG_LEVEL", _default_log_level()).upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)

from .routers import candidates, diversify, health, rank, skylight, xrpc
from .security import RequireApiKey
from .lib.atproto_auth import init_id_resolver
from .lib.es_client import SlowQueryLoggingES
from .lib.feed_cache import FirestoreFeedCache
from .lib.firestore import init_firestore_client
from .lib.http_client import close_http_client, init_http_client
from .lib.metrics import MetricCollector, set_metric_collector
from .lib.profiling import install_profiling
from .lib.request_context import reset_request_id, set_request_id

from elasticsearch import AsyncElasticsearch


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler that validates required environment variables and
    constructs an `AsyncElasticsearch` client attached to `app.state.es`.

    The client is closed on shutdown.
    """
    if not os.environ.get("GE_ELASTICSEARCH_API_KEY"):
        raise RuntimeError("GE_ELASTICSEARCH_API_KEY environment variable is required")

    if not os.environ.get("GE_FEED_CONTEXT_SECRET"):
        raise RuntimeError("GE_FEED_CONTEXT_SECRET environment variable is required")

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

    metrics = MetricCollector(
        service_name=os.environ.get("GE_SERVICE_NAME", "greenearth-api"),
        env=os.environ.get("ENVIRONMENT", "local"),
        export_interval_sec=int(os.environ.get("GE_METRICS_EXPORT_INTERVAL_SEC", "60")),
    )
    set_metric_collector(metrics)

    app.state.es = SlowQueryLoggingES(es)
    app.state.id_resolver = init_id_resolver()
    app.state.firestore = init_firestore_client()
    app.state.feed_cache = FirestoreFeedCache(app.state.firestore)
    init_http_client()
    try:
        yield
    finally:
        try:
            await es.close()
        except Exception:
            pass
        try:
            app.state.firestore.close()
        except Exception:
            pass
        try:
            await close_http_client()
        except Exception:
            pass
        try:
            await metrics.shutdown()
        except Exception:
            pass
        set_metric_collector(None)


_DESCRIPTION = """\
The Green Earth API powers Bluesky content recommendation for the
[Green Earth](https://greenearth.social) feed generator.  External
Atmosphere Protocol developers can use it as a building block for their
own content-discovery pipelines.

## Pipeline overview

The core recommendation pipeline has three stages that can be used
independently or chained together:

1. **Candidates** – retrieve posts from one or more named generators
   (popularity, similarity, followed users, random …).
2. **Rank** – score the candidate list with a trained engagement-prediction
   model and return posts in descending score order.
3. **Diversify** – rerank the ordered list with Maximal Marginal Relevance
   (MMR) to reduce topical redundancy while preserving relevance.

A typical integration calls `/candidates/generate`, pipes the result into
`/rank/predict`, then optionally calls `/diversify` before presenting posts
to a user.

## Content search

The **Skylight** endpoints (`/skylight/search` and `/skylight/similar`)
provide standalone full-text and vector-similarity search over the post
index.  They are independent of the candidate/rank/diversify pipeline and
can be used on their own.

## Authentication

Most endpoints require an `X-API-Key` header.  Use the **Authorize**
button above to set your key.

The AT Protocol XRPC endpoints (`/xrpc/…`) use JWT authentication issued
by the Bluesky AppView and do not require an API key.
"""

_TAGS = [
    {
        "name": "candidates",
        "description": (
            "Retrieve candidate posts from registered generators. "
            "Each generator targets a different content source (popularity, "
            "post similarity, followed users, random …). Multiple generators "
            "can be combined with relative weights in a single request."
        ),
    },
    {
        "name": "rank",
        "description": (
            "Score and order candidate posts using trained engagement-prediction "
            "models. Pass the output of `/candidates/generate` directly as the "
            "request body and receive candidates back in descending score order."
        ),
    },
    {
        "name": "diversify",
        "description": (
            "Rerank an ordered candidate list using Maximal Marginal Relevance "
            "(MMR) to reduce topical redundancy while preserving relevance. "
            "Typically called as the final step after ranking."
        ),
    },
    {
        "name": "skylight",
        "description": (
            "Standalone full-text and vector-similarity search over the post "
            "index. These endpoints are independent of the candidate/rank/"
            "diversify pipeline and can be used on their own by applications "
            "that need direct content search (e.g. the Skylight app)."
        ),
    },
    {
        "name": "health",
        "description": "Service liveness check.",
    },
    {
        "name": "xrpc",
        "description": (
            "AT Protocol XRPC endpoints that implement the Bluesky feed "
            "generator specification (`app.bsky.feed.describeFeedGenerator` "
            "and `app.bsky.feed.getFeedSkeleton`). These endpoints are public "
            "and use AT Protocol JWT authentication rather than an API key."
        ),
    },
]

app = FastAPI(
    title="Green Earth API",
    description=_DESCRIPTION,
    version="0.1.0",
    contact={
        "name": "Green Earth community",
        "url": "https://discord.com/invite/8bWEyrkrJC",
    },
    openapi_tags=_TAGS,
    lifespan=lifespan,
)


# Register profiling middleware first so that when both are stacked it ends up
# *inside* request_id_mw. Starlette runs the last-registered middleware first
# (outermost); we want request_id_mw outer so the rid is set before profile_mw
# tries to read it for the output filename.
install_profiling(app)


@app.middleware("http")
async def request_id_mw(request: Request, call_next):
    """Stamp a server-generated request ID on every request.

    The ID is set as a ContextVar so log lines emitted on the request
    path (via ``timed()`` or otherwise) can include it. We deliberately
    ignore any inbound ``x-request-id`` header to avoid log forging or
    correlation poisoning from untrusted callers.
    """
    rid = uuid.uuid4().hex[:12]
    token = set_request_id(rid)
    try:
        response = await call_next(request)
    finally:
        reset_request_id(token)
    response.headers["x-request-id"] = rid
    return response


app.include_router(candidates.router)
app.include_router(diversify.router)
app.include_router(health.router)
app.include_router(rank.router)
app.include_router(skylight.router)
app.include_router(xrpc.router)


@app.get("/")
async def root(_api_key: RequireApiKey):
    return {"message": "Green Earth API"}

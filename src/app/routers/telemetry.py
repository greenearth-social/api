"""Frontend telemetry ingestion endpoint for Core Web Vitals and client errors.

Companion to greenearth-social/frontend#101 and greenearth-social/api#588.
Ingests metrics and critical errors from the web frontend and exports them to
GCP Cloud Monitoring and Cloud Logging.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections import defaultdict, deque
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from ..lib.metrics import get_metric_collector

logger = logging.getLogger(__name__)

router = APIRouter(tags=["telemetry"])

MAX_PAYLOAD_BYTES = 16 * 1024  # 16 KB
MAX_BATCH_ITEMS = 50
RATE_LIMIT_REQUESTS = 60
RATE_LIMIT_WINDOW_SECONDS = 60.0

ALLOWED_ORIGIN_PATTERNS = (
    re.compile(r"^https://([a-zA-Z0-9-]+\.)*greenearth\.social$"),
    re.compile(r"^https://([a-zA-Z0-9-]+\.)*a\.run\.app$"),
    re.compile(r"^http://(localhost|127\.0\.0\.1)(:\d+)?$"),
)


class InMemoryRateLimiter:
    """Sliding-window in-memory rate limiter per client IP."""

    def __init__(
        self,
        max_requests: int = RATE_LIMIT_REQUESTS,
        window_seconds: float = RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()
        self._last_cleanup = time.monotonic()

    def is_allowed(self, client_ip: str) -> bool:
        now = time.monotonic()
        with self._lock:
            if now - self._last_cleanup > self.window_seconds:
                self._cleanup(now)

            timestamps = self._requests[client_ip]
            cutoff = now - self.window_seconds
            while timestamps and timestamps[0] < cutoff:
                timestamps.popleft()

            if len(timestamps) >= self.max_requests:
                return False

            timestamps.append(now)
            return True

    def _cleanup(self, now: float) -> None:
        self._last_cleanup = now
        cutoff = now - self.window_seconds
        stale = [ip for ip, ts in self._requests.items() if not ts or ts[-1] < cutoff]
        for ip in stale:
            del self._requests[ip]

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()


rate_limiter = InMemoryRateLimiter()


def get_client_ip(request: Request) -> str:
    """Extract real client IP, respecting X-Forwarded-For from Cloud Run."""
    x_forwarded = request.headers.get("x-forwarded-for")
    if x_forwarded:
        return x_forwarded.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def is_deployed_environment() -> bool:
    """True when running in a deployed environment (stage/prod)."""
    env = (os.environ.get("ENVIRONMENT") or os.environ.get("GE_ENVIRONMENT") or "").strip().lower()
    return env in ("prod", "production", "stage", "staging")


def get_request_origin(request: Request) -> str | None:
    """Extract origin from Origin or Referer header."""
    origin = request.headers.get("origin")
    if origin:
        return origin.strip()
    referer = request.headers.get("referer")
    if referer:
        parsed = urlparse(referer)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    return None


def is_origin_allowed(origin: str) -> bool:
    """Check if origin matches trusted domains."""
    return any(pattern.match(origin) for pattern in ALLOWED_ORIGIN_PATTERNS)


def verify_telemetry_request(request: Request) -> None:
    """Protect telemetry endpoint with size limits, origin check, and rate limiting."""
    # 1. Payload size check
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_PAYLOAD_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail="Payload exceeds maximum size of 16KB",
                )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Content-Length header",
            ) from None

    # 2. Origin validation
    origin = get_request_origin(request)
    if origin:
        if not is_origin_allowed(origin):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Forbidden origin",
            )
    elif is_deployed_environment():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Missing origin header",
        )

    # 3. Rate limiting
    client_ip = get_client_ip(request)
    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )


class TelemetryItem(BaseModel):
    type: Literal["vitals", "error"]
    name: str = Field(
        ...,
        max_length=64,
        description="Metric or error name (e.g., LCP, INP, FeedRenderError)",
    )
    value: float | None = Field(
        default=None,
        description="Numeric value in milliseconds for vitals",
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Metadata such as app_route, error message, etc.",
    )


def _record_telemetry_item(item: TelemetryItem) -> None:
    collector = get_metric_collector()
    route = str(item.extra.get("app_route") or "unknown")[:64]

    if item.type == "vitals":
        if item.value is not None and item.value >= 0:
            metric_name = f"frontend.web_vitals.{item.name.lower()}_ms"
            if collector is not None:
                collector.record(metric_name, float(item.value), route=route)
    elif item.type == "error":
        error_name = item.name[:64]
        if collector is not None:
            collector.record("frontend.client_error_count", 1, error_name=error_name, route=route)
        logger.error(
            "Frontend client error: %s",
            error_name,
            extra={
                "error_name": error_name,
                "route": route,
                "details": item.extra,
            },
        )


@router.post(
    "/api/telemetry",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_telemetry_request)],
)
@router.post(
    "/telemetry",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_telemetry_request)],
)
async def ingest_telemetry(
    payload: list[TelemetryItem] | TelemetryItem,
    request: Request,
) -> Response:
    """Ingest lightweight frontend telemetry from navigator.sendBeacon or fetch."""
    items = payload if isinstance(payload, list) else [payload]
    if len(items) > MAX_BATCH_ITEMS:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"Batch size exceeds maximum of {MAX_BATCH_ITEMS} items",
        )
    for item in items:
        _record_telemetry_item(item)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

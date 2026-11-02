"""Frontend telemetry ingestion endpoint for Core Web Vitals and client errors.

Companion to greenearth-social/frontend#101 and greenearth-social/api#588.
Ingests metrics and critical errors from the web frontend and exports them to
GCP Cloud Monitoring and Cloud Logging.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel, Field

from ..lib.metrics import get_metric_collector

logger = logging.getLogger(__name__)

router = APIRouter(tags=["telemetry"])


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


@router.post("/api/telemetry", status_code=status.HTTP_204_NO_CONTENT)
@router.post("/telemetry", status_code=status.HTTP_204_NO_CONTENT)
async def ingest_telemetry(
    payload: list[TelemetryItem] | TelemetryItem,
    request: Request,
) -> Response:
    """Ingest lightweight frontend telemetry from navigator.sendBeacon or fetch."""
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        _record_telemetry_item(item)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

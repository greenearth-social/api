"""Client-side Megastream freshness monitoring.

Measures the freshness of ingested posts directly against Elasticsearch
from the API serving path, providing a reliable heartbeat even when the
ingest service itself fails to start or crashes before logging metrics.
See issue ingex#551.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import UTC, datetime
from typing import Any

from .elasticsearch import POSTS_KNN_INDEX, unwrap_es_response
from .metrics import get_metric_collector

logger = logging.getLogger(__name__)

# Minimum interval between live Elasticsearch queries to prevent hammering.
MIN_FRESHNESS_CHECK_INTERVAL_SEC: float = 15.0

# Sampling rate for regular feed requests.
DEFAULT_FRESHNESS_SAMPLE_RATE: float = 0.05

_last_freshness_check_monotonic: float = 0.0


def reset_debounce() -> None:
    """Reset the debounce timestamp (used for testing)."""
    global _last_freshness_check_monotonic
    _last_freshness_check_monotonic = 0.0


def should_sample_freshness(
    is_probe: bool, sample_rate: float = DEFAULT_FRESHNESS_SAMPLE_RATE
) -> bool:
    """Return True if this request should sample freshness."""
    if is_probe:
        return True
    return random.random() < sample_rate


def parse_post_timestamp(ts_str: str) -> datetime | None:
    """Parse an ISO-8601 or RFC3339 timestamp string into a UTC datetime."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None


async def fetch_latest_post_created_at(es: Any) -> datetime | None:
    """Query Elasticsearch for the newest post created_at timestamp in posts_recent."""
    try:
        resp = await es.search(
            index=POSTS_KNN_INDEX,
            op="freshness",
            size=1,
            sort=[{"created_at": "desc"}],
            _source=["created_at"],
            request_timeout=5,
        )
        data = unwrap_es_response(resp)
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return None
        created_at_str = hits[0].get("_source", {}).get("created_at")
        if not created_at_str or not isinstance(created_at_str, str):
            return None
        return parse_post_timestamp(created_at_str)
    except Exception as exc:
        logger.warning("Failed to query latest post for freshness check: %s", exc)
        return None


async def record_megastream_freshness(
    es: Any,
    *,
    force: bool = False,
    now: datetime | None = None,
    collector: Any | None = None,
) -> float | None:
    """Check and record the current megastream freshness lag in seconds.

    Debounced so queries to Elasticsearch happen at most once every
    ``MIN_FRESHNESS_CHECK_INTERVAL_SEC`` seconds unless ``force=True``.
    """
    global _last_freshness_check_monotonic
    now_mono = time.monotonic()
    if (
        not force
        and (now_mono - _last_freshness_check_monotonic) < MIN_FRESHNESS_CHECK_INTERVAL_SEC
    ):
        return None

    _last_freshness_check_monotonic = now_mono

    latest_created_at = await fetch_latest_post_created_at(es)
    if latest_created_at is None:
        return None

    current_time = now if now is not None else datetime.now(UTC)
    lag_sec = max(0.0, (current_time - latest_created_at).total_seconds())

    if collector is None:
        collector = get_metric_collector()

    if collector is not None:
        collector.record("feed.megastream.freshness_sec", lag_sec)
        collector.record("feed.megastream.freshness_sec_gauge", lag_sec)

    return lag_sec

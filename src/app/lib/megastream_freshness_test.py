"""Tests for megastream freshness monitoring."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from .megastream_freshness import (
    fetch_latest_post_created_at,
    parse_post_timestamp,
    record_megastream_freshness,
    reset_debounce,
    should_sample_freshness,
)


def test_parse_post_timestamp():
    # RFC3339 with Z
    dt = parse_post_timestamp("2026-11-02T12:00:00Z")
    assert dt == datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC)

    # With microseconds and explicit offset
    dt2 = parse_post_timestamp("2026-11-02T12:00:00.500000+00:00")
    assert dt2 == datetime(2026, 11, 2, 12, 0, 0, 500000, tzinfo=UTC)

    # Invalid string
    assert parse_post_timestamp("invalid-date") is None
    assert parse_post_timestamp("") is None


def test_should_sample_freshness():
    # Probes are always sampled
    assert should_sample_freshness(is_probe=True, sample_rate=0.0) is True

    # 100% sample rate
    assert should_sample_freshness(is_probe=False, sample_rate=1.0) is True

    # 0% sample rate
    assert should_sample_freshness(is_probe=False, sample_rate=0.0) is False


@pytest.mark.asyncio
async def test_fetch_latest_post_created_at_success():
    es = AsyncMock()
    es.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "created_at": "2026-11-02T13:45:00Z",
                    }
                }
            ]
        }
    }
    result = await fetch_latest_post_created_at(es)
    assert result == datetime(2026, 11, 2, 13, 45, 0, tzinfo=UTC)
    es.search.assert_awaited_once()
    kwargs = es.search.await_args.kwargs
    assert kwargs["index"] == "posts_recent"
    assert kwargs["size"] == 1
    assert kwargs["sort"] == [{"created_at": "desc"}]


@pytest.mark.asyncio
async def test_fetch_latest_post_created_at_empty():
    es = AsyncMock()
    es.search.return_value = {"hits": {"hits": []}}
    result = await fetch_latest_post_created_at(es)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_latest_post_created_at_exception():
    es = AsyncMock()
    es.search.side_effect = RuntimeError("ES connection failed")
    result = await fetch_latest_post_created_at(es)
    assert result is None


@pytest.mark.asyncio
async def test_record_megastream_freshness_success():
    reset_debounce()
    es = AsyncMock()
    es.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "created_at": "2026-11-02T13:50:00Z",
                    }
                }
            ]
        }
    }
    collector = MagicMock()
    fixed_now = datetime(2026, 11, 2, 14, 0, 0, tzinfo=UTC)  # 10 minutes (600s) later

    lag = await record_megastream_freshness(es, now=fixed_now, collector=collector, force=True)
    assert lag == pytest.approx(600.0)

    # Verify metrics emitted
    collector.record.assert_any_call("feed.megastream.freshness_sec", 600.0)
    collector.record.assert_any_call("feed.megastream.freshness_sec_gauge", 600.0)


@pytest.mark.asyncio
async def test_record_megastream_freshness_debounced():
    reset_debounce()
    es = AsyncMock()
    es.search.return_value = {
        "hits": {"hits": [{"_source": {"created_at": "2026-11-02T13:50:00Z"}}]}
    }
    collector = MagicMock()
    fixed_now = datetime(2026, 11, 2, 14, 0, 0, tzinfo=UTC)

    # First call records
    lag1 = await record_megastream_freshness(es, now=fixed_now, collector=collector)
    assert lag1 is not None
    assert es.search.await_count == 1

    # Second call within debounce window is skipped
    lag2 = await record_megastream_freshness(es, now=fixed_now, collector=collector)
    assert lag2 is None
    assert es.search.await_count == 1  # Not called again

    # Forcing bypasses debounce
    lag3 = await record_megastream_freshness(es, now=fixed_now, collector=collector, force=True)
    assert lag3 is not None
    assert es.search.await_count == 2

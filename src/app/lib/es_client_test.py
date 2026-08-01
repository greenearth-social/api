"""Tests for the slow-ES-query logging wrapper."""

import asyncio
import json
import logging

import pytest
from elastic_transport import ConnectionError as EsConnectionError
from elastic_transport import ConnectionTimeout

from . import es_client as es_client_module
from .es_client import SlowQueryLoggingES
from .request_context import set_request_id, reset_request_id


class FakeEs:
    def __init__(self, response=None):
        self.response = response or {"hits": {"hits": []}}
        self.search_calls = []
        self.closed = False

    async def search(self, **kwargs):
        self.search_calls.append(kwargs)
        return self.response

    async def close(self):
        self.closed = True


def test_search_under_threshold_does_not_log(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "1000")
    es = SlowQueryLoggingES(FakeEs())
    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        asyncio.run(es.search(index="posts", query={"match_all": {}}))
    assert "slow_es_query" not in caplog.text


def test_search_over_threshold_logs_body(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")

    fake = FakeEs()
    es = SlowQueryLoggingES(fake)

    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        asyncio.run(
            es.search(
                index="posts_recent",
                knn={"field": "embeddings.x", "query_vector": [0.1, 0.2], "k": 30},
                size=30,
            )
        )

    matching = [r for r in caplog.records if "slow_es_query" in r.message]
    assert len(matching) == 1
    msg = matching[0].message
    assert "index=posts_recent" in msg
    # The body= payload must be valid JSON and must not contain index/
    # request_timeout (they're client-side, not part of the ES request body).
    body_str = msg.split("body=", 1)[1]
    parsed = json.loads(body_str)
    assert "index" not in parsed
    assert "request_timeout" not in parsed
    assert parsed["knn"]["k"] == 30
    assert parsed["size"] == 30


def test_search_log_includes_request_id_when_set(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")
    es = SlowQueryLoggingES(FakeEs())

    token = set_request_id("abc12345")
    try:
        with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
            asyncio.run(es.search(index="posts", query={"match_all": {}}))
    finally:
        reset_request_id(token)

    matching = [r for r in caplog.records if "slow_es_query" in r.message]
    assert matching
    assert "rid=abc12345" in matching[0].message


def test_search_log_uses_dash_when_no_request_id(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")
    es = SlowQueryLoggingES(FakeEs())
    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        asyncio.run(es.search(index="posts", query={"match_all": {}}))
    matching = [r for r in caplog.records if "slow_es_query" in r.message]
    assert matching
    assert "rid=-" in matching[0].message


def test_search_returns_underlying_response():
    fake = FakeEs(response={"hits": {"hits": [{"_id": "x"}]}})
    es = SlowQueryLoggingES(fake)
    resp = asyncio.run(es.search(index="posts", query={"match_all": {}}))
    assert resp == {"hits": {"hits": [{"_id": "x"}]}}


def test_search_propagates_exceptions(caplog, monkeypatch):
    """Slow path still logs even if the underlying call raises."""
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")

    class BadEs:
        async def search(self, **kwargs):
            raise RuntimeError("boom")

    es = SlowQueryLoggingES(BadEs())

    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(es.search(index="posts", query={"match_all": {}}))

    assert any("slow_es_query" in r.message for r in caplog.records)


def test_other_attributes_delegate_to_wrapped():
    fake = FakeEs()
    es = SlowQueryLoggingES(fake)
    asyncio.run(es.close())
    assert fake.closed is True


def test_connection_timeout_logs_timeout_message(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "1000000")

    class TimeoutEs:
        async def search(self, **kwargs):
            raise ConnectionTimeout("timed out")

    es = SlowQueryLoggingES(TimeoutEs())

    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        with pytest.raises(ConnectionTimeout):
            asyncio.run(es.search(index="posts", query={"match_all": {}}))

    matching = [r for r in caplog.records if "es_query_timeout" in r.message]
    assert len(matching) == 1
    assert "index=posts" in matching[0].message


def test_connection_timeout_does_not_log_slow_query(caplog, monkeypatch):
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")

    class TimeoutEs:
        async def search(self, **kwargs):
            raise ConnectionTimeout("timed out")

    es = SlowQueryLoggingES(TimeoutEs())

    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        with pytest.raises(ConnectionTimeout):
            asyncio.run(es.search(index="posts", query={"match_all": {}}))

    assert not any("slow_es_query" in r.message for r in caplog.records)


def test_connection_timeout_is_reraised():
    class TimeoutEs:
        async def search(self, **kwargs):
            raise ConnectionTimeout("timed out")

    es = SlowQueryLoggingES(TimeoutEs())

    with pytest.raises(ConnectionTimeout):
        asyncio.run(es.search(index="posts", query={"match_all": {}}))


def test_connection_timeout_log_includes_request_id(caplog, monkeypatch):
    class TimeoutEs:
        async def search(self, **kwargs):
            raise ConnectionTimeout("timed out")

    es = SlowQueryLoggingES(TimeoutEs())

    token = set_request_id("tid99")
    try:
        with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
            with pytest.raises(ConnectionTimeout):
                asyncio.run(es.search(index="posts", query={"match_all": {}}))
    finally:
        reset_request_id(token)

    matching = [r for r in caplog.records if "es_query_timeout" in r.message]
    assert matching
    assert "rid=tid99" in matching[0].message


def test_threshold_is_re_read_per_call(caplog, monkeypatch):
    """Changing the env var between calls takes effect immediately."""
    fake = FakeEs()
    es = SlowQueryLoggingES(fake)

    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "1000000")
    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        asyncio.run(es.search(index="posts", query={"match_all": {}}))
    assert not any("slow_es_query" in r.message for r in caplog.records)

    caplog.clear()
    monkeypatch.setenv("GE_SLOW_ES_THRESHOLD_MS", "0")
    with caplog.at_level(logging.WARNING, logger=es_client_module.logger.name):
        asyncio.run(es.search(index="posts", query={"match_all": {}}))
    assert any("slow_es_query" in r.message for r in caplog.records)


class _RecordingCollector:
    def __init__(self):
        self.records = []

    def record(self, name, value, **attrs):
        self.records.append((name, value, attrs))


@pytest.mark.asyncio
async def test_search_records_duration_and_took_metrics(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            return {"took": 42, "hits": {"hits": []}}

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="likes", op="likes")

    names = {name: attrs for name, _, attrs in collector.records}
    assert names["es.query.duration_ms"] == {"op": "likes"}
    assert names["es.query.took_ms"] == {"op": "likes"}
    took = [v for n, v, _ in collector.records if n == "es.query.took_ms"]
    assert took == [42]


@pytest.mark.asyncio
async def test_search_does_not_forward_op_to_client(monkeypatch):
    seen_kwargs = {}

    class _FakeES:
        async def search(self, **kwargs):
            seen_kwargs.update(kwargs)
            return {"took": 1}

    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: None)
    wrapped = SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="posts", op="hydrate")
    assert "op" not in seen_kwargs


@pytest.mark.asyncio
async def test_search_defaults_op_to_unlabeled_and_tolerates_missing_took(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            return {"hits": {"hits": []}}  # no "took" key

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    await wrapped.search(index="posts")

    by_name = {n: attrs for n, _, attrs in collector.records}
    assert by_name["es.query.duration_ms"] == {"op": "unlabeled"}
    assert "es.query.took_ms" not in by_name


@pytest.mark.asyncio
async def test_search_records_duration_on_timeout(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            raise ConnectionTimeout("boom")

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    with pytest.raises(ConnectionTimeout):
        await wrapped.search(index="posts", op="knn")

    names = [n for n, _, _ in collector.records]
    assert "es.query.duration_ms" in names
    assert "es.query.took_ms" not in names


@pytest.mark.asyncio
async def test_search_records_error_count_on_timeout(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            raise ConnectionTimeout("boom")

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    with pytest.raises(ConnectionTimeout):
        await wrapped.search(index="posts", op="knn")

    assert ("es.query.error_count", 1, {"op": "knn", "error": "timeout"}) in collector.records


@pytest.mark.asyncio
async def test_search_records_error_count_on_connection_error(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            raise EsConnectionError("connection refused")

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    with pytest.raises(EsConnectionError):
        await wrapped.search(index="posts", op="knn")

    assert ("es.query.error_count", 1, {"op": "knn", "error": "connection"}) in collector.records
    # An errored query still consumed client-side time.
    names = [n for n, _, _ in collector.records]
    assert "es.query.duration_ms" in names


@pytest.mark.asyncio
async def test_search_records_error_count_on_other_exception(monkeypatch):
    class _FakeES:
        async def search(self, **kwargs):
            raise ValueError("boom")

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_FakeES())
    with pytest.raises(ValueError):
        await wrapped.search(index="posts", op="likes")

    assert ("es.query.error_count", 1, {"op": "likes", "error": "other"}) in collector.records


@pytest.mark.asyncio
async def test_search_records_in_flight_count(monkeypatch):
    release = asyncio.Event()

    class _BlockingES:
        async def search(self, **kwargs):
            await release.wait()
            return {"took": 1, "hits": {"hits": []}}

    collector = _RecordingCollector()
    monkeypatch.setattr(es_client_module, "get_metric_collector", lambda: collector)

    wrapped = SlowQueryLoggingES(_BlockingES())
    t1 = asyncio.create_task(wrapped.search(index="posts", op="knn"))
    t2 = asyncio.create_task(wrapped.search(index="posts", op="likes"))
    await asyncio.sleep(0.01)
    release.set()
    await asyncio.gather(t1, t2)

    in_flight = sorted(v for n, v, _ in collector.records if n == "es.client.in_flight")
    assert in_flight == [1, 2]

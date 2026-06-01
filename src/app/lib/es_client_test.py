"""Tests for the slow-ES-query logging wrapper."""

import asyncio
import json
import logging

import pytest

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

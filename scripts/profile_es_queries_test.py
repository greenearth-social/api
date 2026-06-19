"""Tests for profile_es_queries helper functions."""
import json
import importlib.util
import pathlib

import pytest

_SCRIPT = pathlib.Path(__file__).parent / "profile_es_queries.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("profile_es_queries", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


SAMPLE_PAYLOAD = (
    'slow_es_query rid=abc123 elapsed_ms=2654.3 index=posts_recent '
    'body={"knn":{"field":"embeddings.minilm_l12_v1","query_vector":[0.1,0.2],"k":300},"size":300}'
)


def test_parse_log_line_extracts_fields(mod):
    result = mod.parse_log_line(SAMPLE_PAYLOAD)
    assert result is not None
    assert result["rid"] == "abc123"
    assert abs(result["elapsed_ms"] - 2654.3) < 0.01
    assert result["index"] == "posts_recent"
    assert "knn" in result["body"]


def test_parse_log_line_returns_none_for_non_slow_query(mod):
    assert mod.parse_log_line("some other log line") is None


def test_inject_profile_adds_profile_true(mod):
    body = {"knn": {"field": "embeddings.minilm_l12_v1", "k": 100}, "size": 100}
    result = mod.inject_profile(body)
    assert result["profile"] is True
    assert "knn" in result


def test_inject_profile_does_not_mutate_input(mod):
    body = {"query": {"match_all": {}}}
    original = json.dumps(body)
    mod.inject_profile(body)
    assert json.dumps(body) == original


def test_summarise_profile_extracts_max_shard_time(mod):
    profile = {
        "shards": [
            {
                "id": "[shard0]",
                "searches": [{"query": [{"time_in_nanos": 800_000_000}]}],
                "fetch": {"time_in_nanos": 50_000_000},
            },
            {
                "id": "[shard1]",
                "searches": [{"query": [{"time_in_nanos": 1_200_000_000}]}],
                "fetch": {"time_in_nanos": 30_000_000},
            },
        ]
    }
    summary = mod.summarise_profile(profile)
    assert summary["num_shards"] == 2
    assert abs(summary["max_query_ms"] - 1200.0) < 0.1
    assert abs(summary["max_fetch_ms"] - 50.0) < 0.1


def test_summarise_profile_handles_missing_fetch(mod):
    profile = {
        "shards": [
            {"id": "[shard0]", "searches": [{"query": [{"time_in_nanos": 500_000_000}]}]}
        ]
    }
    summary = mod.summarise_profile(profile)
    assert summary["max_fetch_ms"] == 0.0

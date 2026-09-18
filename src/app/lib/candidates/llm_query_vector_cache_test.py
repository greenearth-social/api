"""Tests for LlmQueryVectorCache and its process-level accessor."""

from unittest.mock import MagicMock

import pytest

from .llm_query_vector_cache import (
    get_llm_query_vector_cache,
    set_llm_query_vector_cache,
)


@pytest.fixture(autouse=True)
def reset_cache():
    set_llm_query_vector_cache(None)
    yield
    set_llm_query_vector_cache(None)


class TestCacheSingleton:
    def test_default_is_none(self):
        assert get_llm_query_vector_cache() is None

    def test_set_and_get(self):
        fake_cache = MagicMock()
        set_llm_query_vector_cache(fake_cache)
        assert get_llm_query_vector_cache() is fake_cache

    def test_set_none_clears(self):
        set_llm_query_vector_cache(MagicMock())
        set_llm_query_vector_cache(None)
        assert get_llm_query_vector_cache() is None

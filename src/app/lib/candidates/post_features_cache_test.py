"""Tests for the process-wide post-features cache."""

from unittest.mock import patch

from .post_features_cache import PostFeatures, PostFeaturesCache


def test_returns_only_cached_posts():
    cache = PostFeaturesCache(max_entries=10, ttl_seconds=60)
    cache.put("at://a", PostFeatures("emb", 0.5))

    assert cache.get_many(["at://a", "at://b"]) == {"at://a": PostFeatures("emb", 0.5)}


def test_evicts_the_least_recently_used_entry():
    cache = PostFeaturesCache(max_entries=2, ttl_seconds=60)
    cache.put("at://a", PostFeatures("a", None))
    cache.put("at://b", PostFeatures("b", None))
    cache.get_many(["at://a"])  # a is now more recent than b
    cache.put("at://c", PostFeatures("c", None))

    assert set(cache.get_many(["at://a", "at://b", "at://c"])) == {"at://a", "at://c"}
    assert len(cache) == 2


def test_expired_entries_are_dropped():
    cache = PostFeaturesCache(max_entries=10, ttl_seconds=60)
    with patch("app.lib.candidates.post_features_cache.time.monotonic", return_value=1000.0):
        cache.put("at://a", PostFeatures("a", None))
    with patch("app.lib.candidates.post_features_cache.time.monotonic", return_value=1061.0):
        assert cache.get_many(["at://a"]) == {}
    assert len(cache) == 0

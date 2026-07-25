"""Tests for the pure helpers shared by the load-testing scripts."""

import random

import pytest
from load_test_lib import (
    COHORTS,
    build_interactions,
    feed_uri_from_describe,
    interactions_request_body,
    percentiles,
    sample_page_depth,
    session_start_offsets,
    split_counts,
)


class TestSplitCounts:
    def test_sums_to_total(self):
        counts = split_counts(100, 60, 30, 10)
        assert sum(counts.values()) == 100
        assert counts == {"existing": 60, "active": 30, "low": 10}

    def test_remainder_distributed_in_cohort_order(self):
        # 10 * {34,33,33}% = {3.4, 3.3, 3.3} -> floors {3,3,3}=9, +1 to first.
        counts = split_counts(10, 34, 33, 33)
        assert sum(counts.values()) == 10
        assert counts["existing"] == 4

    def test_rejects_bad_percentages(self):
        with pytest.raises(ValueError):
            split_counts(100, 50, 30, 10)

    def test_zero_total(self):
        assert split_counts(0, 60, 30, 10) == {c: 0 for c in COHORTS}


class TestSamplePageDepth:
    def test_always_at_least_one(self):
        rng = random.Random(0)
        assert all(sample_page_depth(rng, 3) >= 1 for _ in range(200))

    def test_deterministic_under_seed(self):
        a = [sample_page_depth(random.Random(7), 3) for _ in range(5)]
        b = [sample_page_depth(random.Random(7), 3) for _ in range(5)]
        assert a == b

    def test_mean_in_ballpark(self):
        rng = random.Random(1)
        samples = [sample_page_depth(rng, 3) for _ in range(5000)]
        assert 2.5 < sum(samples) / len(samples) < 3.5

    def test_rejects_mean_below_one(self):
        with pytest.raises(ValueError):
            sample_page_depth(random.Random(0), 0.5)


class TestSessionStartOffsets:
    def test_deterministic_and_sorted(self):
        a = session_start_offsets(60, 5, random.Random(3))
        b = session_start_offsets(60, 5, random.Random(3))
        assert a == b
        assert a == sorted(a)

    def test_within_horizon(self):
        offsets = session_start_offsets(120, 2, random.Random(9))
        assert all(0 <= o < 120 for o in offsets)

    def test_empty_for_nonpositive(self):
        assert session_start_offsets(0, 5, random.Random(0)) == []
        assert session_start_offsets(60, 0, random.Random(0)) == []

    def test_rate_scales_count(self):
        slow = len(session_start_offsets(30, 10, random.Random(5)))
        fast = len(session_start_offsets(300, 10, random.Random(5)))
        assert fast > slow


class TestBuildInteractions:
    def _feed(self):
        return [
            {"post": "at://p/1", "feedContext": "TOKEN1"},
            {"post": "at://p/2", "feedContext": "TOKEN2"},
        ]

    def test_echoes_feed_context_verbatim(self):
        specs = build_interactions(
            self._feed(), random.Random(0), seen_share=1.0, like_share=0, click_share=0
        )
        # Every item seen, tokens echoed exactly.
        assert {s.item for s in specs} == {"at://p/1", "at://p/2"}
        by_item = {s.item: s.feed_context for s in specs}
        assert by_item["at://p/1"] == "TOKEN1"
        assert by_item["at://p/2"] == "TOKEN2"
        assert all(s.event.endswith("interactionSeen") for s in specs)

    def test_skips_items_missing_context(self):
        feed = [{"post": "at://p/1"}, {"post": "at://p/2", "feedContext": "T2"}]
        specs = build_interactions(
            feed, random.Random(0), seen_share=1.0, like_share=0, click_share=0
        )
        assert [s.item for s in specs] == ["at://p/2"]

    def test_request_body_shape(self):
        specs = build_interactions(
            self._feed(), random.Random(0), seen_share=1.0, like_share=0, click_share=0
        )
        body = interactions_request_body(specs)
        assert set(body) == {"interactions"}
        assert all(set(i) == {"item", "event", "feedContext"} for i in body["interactions"])


class TestFeedUriFromDescribe:
    def test_matches_by_rkey(self):
        resp = {
            "feeds": [
                {"uri": "at://did:web:svc/app.bsky.feed.generator/other"},
                {"uri": "at://did:web:svc/app.bsky.feed.generator/your-feed"},
            ]
        }
        assert feed_uri_from_describe(resp, "your-feed").endswith("/your-feed")

    def test_returns_none_when_absent(self):
        assert feed_uri_from_describe({"feeds": []}, "your-feed") is None


class TestPercentiles:
    def test_basic(self):
        vals = list(range(1, 101))  # 1..100
        p = percentiles(vals, (50, 95, 99))
        assert p[50] == 50
        assert p[95] == 95
        assert p[99] == 99

    def test_empty_is_zero(self):
        assert percentiles([]) == {50: 0.0, 95: 0.0, 99: 0.0}

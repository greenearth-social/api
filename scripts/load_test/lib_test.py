"""Tests for the pure helpers shared by the load-testing scripts."""

import random
from datetime import datetime, timezone

import pytest

from load_test import lib
from load_test.lib import (
    COHORTS,
    assign_feeds,
    build_interactions,
    feed_bucket_counts,
    feed_uri_from_describe,
    interactions_request_body,
    parse_feed_spec,
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

    def test_mean_pages_one_always_returns_one(self):
        # p == 1 would make log(1 - p) = log(0); must not crash.
        rng = random.Random(3)
        assert all(sample_page_depth(rng, 1) == 1 for _ in range(50))


class TestParseFeedSpec:
    def test_single_bare_feed_is_full_weight(self):
        assert parse_feed_spec("your-feed") == [("your-feed", 100.0)]

    def test_weighted_pairs(self):
        assert parse_feed_spec("your-feed:90,random:10") == [
            ("your-feed", 90.0),
            ("random", 10.0),
        ]

    def test_whitespace_and_bare_tokens_get_equal_weight(self):
        assert parse_feed_spec(" a , b ") == [("a", 100.0), ("b", 100.0)]

    def test_rkey_with_hyphens_and_weight(self):
        assert parse_feed_spec("best-of-friends:25") == [("best-of-friends", 25.0)]

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            parse_feed_spec("  ,  ")

    def test_rejects_nonpositive_weight(self):
        with pytest.raises(ValueError):
            parse_feed_spec("your-feed:0")

    def test_rejects_non_numeric_weight(self):
        with pytest.raises(ValueError):
            parse_feed_spec("your-feed:lots")

    def test_rejects_duplicate_feed(self):
        with pytest.raises(ValueError):
            parse_feed_spec("your-feed:50,your-feed:50")


class TestFeedBucketCounts:
    def test_sums_to_total_and_proportional(self):
        counts = feed_bucket_counts(100, [90, 10])
        assert sum(counts) == 100
        assert counts == [90, 10]

    def test_normalizes_relative_weights(self):
        # Weights need not sum to 100; [3, 1] over 100 -> 75/25.
        assert feed_bucket_counts(100, [3, 1]) == [75, 25]

    def test_remainder_distributed_in_order(self):
        # 10 * [1,1,1]/3 = 3.33 each -> floors 3,3,3 = 9, +1 to first.
        counts = feed_bucket_counts(10, [1, 1, 1])
        assert sum(counts) == 10
        assert counts == [4, 3, 3]

    def test_zero_total(self):
        assert feed_bucket_counts(0, [90, 10]) == [0, 0]

    def test_rejects_nonpositive_weight(self):
        with pytest.raises(ValueError):
            feed_bucket_counts(10, [1, 0])


class TestAssignFeeds:
    def _users(self, n):
        return [{"did": f"did:plc:{i}", "cohort": "active"} for i in range(n)]

    def test_assigns_exact_bucket_sizes(self):
        users = self._users(20)
        assign_feeds(users, [("your-feed", 90), ("random", 10)], random.Random(0))
        counts = {}
        for u in users:
            counts[u["feed"]] = counts.get(u["feed"], 0) + 1
        assert counts == {"your-feed": 18, "random": 2}

    def test_every_user_gets_a_feed(self):
        users = self._users(7)
        assign_feeds(users, [("a", 1), ("b", 1), ("c", 1)], random.Random(3))
        assert all("feed" in u for u in users)

    def test_deterministic_under_seed(self):
        a = self._users(15)
        b = self._users(15)
        assign_feeds(a, [("x", 70), ("y", 30)], random.Random(9))
        assign_feeds(b, [("x", 70), ("y", 30)], random.Random(9))
        assert [u["feed"] for u in a] == [u["feed"] for u in b]


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


class TestDashboardUrl:
    def test_dashboard_url_builds_console_link(self, tmp_path, monkeypatch):
        ids = tmp_path / "ids.env"
        ids.write_text("DASHBOARD_ID_STAGE=projects/12345/dashboards/abcd-ef\n")
        monkeypatch.setattr(lib, "DASHBOARD_IDS_FILE", str(ids))

        start = datetime(2026, 7, 31, 2, 0, tzinfo=timezone.utc)
        end = datetime(2026, 7, 31, 4, 0, tzinfo=timezone.utc)
        url = lib.dashboard_url("stage", start, end)

        assert "console.cloud.google.com/monitoring/dashboards/builder/abcd-ef" in url
        assert "project=greenearth-471522" in url
        # time range encoded as start/end ISO timestamps
        assert "2026-07-31T02:00" in url and "2026-07-31T04:00" in url

    def test_dashboard_url_returns_none_without_ids_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lib, "DASHBOARD_IDS_FILE", str(tmp_path / "missing.env"))
        assert (
            lib.dashboard_url("stage", datetime.now(timezone.utc), datetime.now(timezone.utc))
            is None
        )

import pytest

from app.feeds import FEEDS, SOCIAL_RADIUS_PRESETS

CANDIDATE_ONLY_FEEDS = {
    "post-similarity": "post_similarity",
    "followed-users": "followed_users",
    "network-likes": "network_likes",
    "popularity": "popularity",
    "two-tower": "two_tower",
}


class TestFeedsRegistry:
    def test_social_radius_splits_everyone_weight_evenly(self):
        for generators in SOCIAL_RADIUS_PRESETS.values():
            weights = {generator.name: generator.weight for generator in generators}
            assert weights.get("two_tower", 0.0) == pytest.approx(
                weights.get("popularity", 0.0)
            )
            assert sum(weights.values()) == pytest.approx(1.0)

    def test_friends_social_radius_has_no_everyone_generators(self):
        assert [(generator.name, generator.weight) for generator in SOCIAL_RADIUS_PRESETS[0]] == [
            ("followed_users", 1.0)
        ]

    def test_balanced_social_radius_matches_your_feed_defaults(self):
        assert (
            FEEDS["your-feed"].gen_request_template.generators
            == SOCIAL_RADIUS_PRESETS[3]
        )

    def test_no_collision_between_internal_rkeys_and_primary_rkeys(self):
        primary_rkeys = set(FEEDS.keys())
        internal_rkeys = {
            cfg.internal_rkey
            for cfg in FEEDS.values()
            if cfg.internal_rkey is not None
        }
        overlap = primary_rkeys & internal_rkeys
        assert not overlap, f"internal_rkey collides with a primary rkey: {overlap}"

    def test_candidate_only_feeds_are_direct_unranked_generators(self):
        for feed_name, generator_name in CANDIDATE_ONLY_FEEDS.items():
            cfg = FEEDS[feed_name]
            generators = cfg.gen_request_template.generators
            assert len(generators) == 1
            assert generators[0].name == generator_name
            assert cfg.gen_request_template.infill is None
            assert cfg.rank_request_template is None
            assert cfg.diversify is False

    def test_personalized_feeds_use_heavy_ranker_and_perspective(self):
        for feed_name in ("your-feed", "best-of-friends"):
            cfg = FEEDS[feed_name]
            assert cfg.rank_request_template is not None
            assert [
                spec.name for spec in cfg.rank_request_template.models
            ] == ["heavy_ranker", "perspective"]

    def test_ranked_feeds_have_slate_cutoffs(self):
        for feed_name in ("your-feed", "best-of-friends"):
            cfg = FEEDS[feed_name]
            assert cfg.max_render_share is not None
            assert cfg.min_rank_score is not None
            assert cfg.min_mmr_score is not None

    def test_unranked_feeds_have_no_slate_cutoffs(self):
        for feed_name, cfg in FEEDS.items():
            if cfg.rank_request_template is not None:
                continue
            assert cfg.max_render_share is None, feed_name
            assert cfg.min_rank_score is None, feed_name
            assert cfg.min_mmr_score is None, feed_name

    def test_cutoff_preview_feed_exercises_the_full_ranked_pipeline(self):
        """Private dev feed for tuning slate-cutoff thresholds (issue #248):
        same generator mix as your-feed, with ranking and diversification
        enabled, so it's a faithful preview of production cutoff behavior."""
        cfg = FEEDS["cutoff-preview"]
        assert cfg.public is False
        assert cfg.rank_request_template is not None
        assert [
            spec.name for spec in cfg.rank_request_template.models
        ] == ["heavy_ranker", "perspective"]
        assert cfg.diversify is True
        assert (
            cfg.gen_request_template.generators
            == FEEDS["your-feed"].gen_request_template.generators
        )
        assert cfg.max_render_share == pytest.approx(0.5)
        assert cfg.min_rank_score == pytest.approx(-0.15)
        assert cfg.min_mmr_score == pytest.approx(-0.05)

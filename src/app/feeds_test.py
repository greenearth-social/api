from app.feeds import FEEDS

CANDIDATE_ONLY_FEEDS = {
    "post-similarity": "post_similarity",
    "followed-users": "followed_users",
    "network-likes": "network_likes",
    "popularity": "popularity",
    "two-tower": "two_tower",
}


class TestFeedsRegistry:
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

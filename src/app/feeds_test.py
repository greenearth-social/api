from app.feeds import FEEDS


class TestFeedsRegistry:
    def test_all_feeds_have_internal_rkey_and_internal_display_name(self):
        for rkey, cfg in FEEDS.items():
            assert cfg.internal_rkey is not None, f"{rkey} missing internal_rkey"
            assert cfg.internal_display_name is not None, f"{rkey} missing internal_display_name"

    def test_no_collision_between_internal_rkeys_and_primary_rkeys(self):
        primary_rkeys = set(FEEDS.keys())
        internal_rkeys = {
            cfg.internal_rkey
            for cfg in FEEDS.values()
            if cfg.internal_rkey is not None
        }
        overlap = primary_rkeys & internal_rkeys
        assert not overlap, f"internal_rkey collides with a primary rkey: {overlap}"

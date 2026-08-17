import pytest
from manage_post import parse_content  # pyright: ignore[reportMissingImports]

from app.feeds import (
    DEFAULT_SOCIAL_RADIUS,
    FEEDS,
    SOCIAL_RADIUS_PRESETS_NO_NETWORK_LIKES,
    SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES,
    _pinned_post_uri,
    canonical_feed_name,
)

CANDIDATE_ONLY_FEEDS = {
    "followed-users": "followed_users",
    "network-likes": "network_likes",
    "popularity": "popularity",
    "two-tower": "two_tower",
    "two-tower-empty-history": "two_tower_empty_history",
}


def test_canonical_feed_name_resolves_configured_and_published_rkeys():
    assert canonical_feed_name("your-feed") == "your-feed"
    assert canonical_feed_name("a0-yf") == "your-feed"
    assert canonical_feed_name("fd-bof") == "best-of-friends"
    assert canonical_feed_name("67-r") == "random"
    assert canonical_feed_name("missing") is None


# AT Protocol app.bsky.feed.generator caps displayName at 24 graphemes. Published
# names are composed from this metadata: prod publishes public feeds under the raw
# display_name, while internal ("debug") feeds are published as
# "GE <internal_display_name> <git_sha>" in dev/stage (see publish_feed.py and
# issue #228). Budgeting for the widest composition here means an over-long name in
# feeds.py fails next to its definition, not only in the publish script's tests.
MAX_DISPLAY_NAME_GRAPHEMES = 24
DEV_STAGE_PREFIX = "GE "  # widest env prefix applied to internal feeds
GIT_SHA_SUFFIX_LEN = len(" ") + 7  # " " + 7-char short git sha


class TestFeedsRegistry:
    def test_public_feed_pins_are_repository_managed(self):
        expected_text = {
            "your-feed": (
                "Click SETTINGS to personalize your Green Earth feed.\n\n"
                "A controllable feed designed for constructive conversation."
            ),
            "best-of-friends": (
                "Click SETTINGS to personalize your feed.\n\n"
                "The best posts from your mutuals and people you follow. "
                "Part of the GreenEarth Family."
            ),
            "random": (
                "Click SETTINGS to personalize your feed.\n\n"
                "A random slice of the ATProto universe. Still applies your moderation "
                "settings. Part of the GreenEarth Family."
            ),
        }
        for feed_name, text in expected_text.items():
            content = FEEDS[feed_name].pinned_post_content
            assert content is not None
            segments = parse_content(content)
            assert "".join(segment["text"] for segment in segments) == text
            links = [segment["url"] for segment in segments if segment["type"] == "link"]
            assert links == [f"https://app.greenearth.social/#/settings/{feed_name}"]
            assert len(text) <= 300

    def test_pinned_post_environment_override(self, monkeypatch):
        monkeypatch.setenv("GE_PINNED_POST_YOUR_FEED_URI", "at://managed")
        assert _pinned_post_uri("your-feed", "at://fallback") == "at://managed"

    def test_blank_pinned_post_environment_override_uses_fallback(self, monkeypatch):
        monkeypatch.setenv("GE_PINNED_POST_YOUR_FEED_URI", "")
        assert _pinned_post_uri("your-feed", "at://fallback") == "at://fallback"

    def test_social_radius_splits_everyone_weight_evenly(self):
        for presets in (
            SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES,
            SOCIAL_RADIUS_PRESETS_NO_NETWORK_LIKES,
        ):
            for generators in presets.values():
                weights = {generator.name: generator.weight for generator in generators}
                assert weights.get("two_tower", 0.0) == pytest.approx(
                    weights.get("popularity", 0.0)
                )
                assert sum(weights.values()) == pytest.approx(1.0)

    def test_friends_social_radius_has_no_everyone_generators(self):
        for presets in (
            SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES,
            SOCIAL_RADIUS_PRESETS_NO_NETWORK_LIKES,
        ):
            assert [(generator.name, generator.weight) for generator in presets[0]] == [
                ("followed_users", 1.0)
            ]

    def test_static_feed_defaults_include_network_likes(self):
        assert (
            FEEDS["your-feed"].gen_request_template.generators
            == SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES[DEFAULT_SOCIAL_RADIUS]
        )

    def test_network_likes_enabled_presets_add_network_likes_outside_friends(self):
        for radius in range(1, 5):
            with_network_likes = {
                generator.name for generator in SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES[radius]
            }
            without_network_likes = {
                generator.name for generator in SOCIAL_RADIUS_PRESETS_NO_NETWORK_LIKES[radius]
            }
            assert "network_likes" in with_network_likes
            assert "network_likes" not in without_network_likes

    def test_no_collision_between_internal_rkeys_and_primary_rkeys(self):
        primary_rkeys = set(FEEDS.keys())
        internal_rkeys = {
            cfg.internal_rkey for cfg in FEEDS.values() if cfg.internal_rkey is not None
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
            assert [spec.name for spec in cfg.rank_request_template.models] == [
                "heavy_ranker",
                "perspective",
            ]

    def test_ranked_feeds_have_slate_cutoffs(self):
        for feed_name in ("your-feed", "best-of-friends"):
            cfg = FEEDS[feed_name]
            assert cfg.max_render_share is not None
            assert cfg.min_rank_score == pytest.approx(0.425)
            assert cfg.min_mmr_score is not None

    def test_cold_start_feed_uses_empty_history_models(self):
        cfg = FEEDS["cold-start"]
        assert cfg.public is False
        assert [(spec.name, spec.weight) for spec in cfg.gen_request_template.generators] == [
            ("popularity", 1.0),
        ]
        assert cfg.rank_request_template is not None
        assert [(spec.name, spec.weight) for spec in cfg.rank_request_template.models] == [
            ("heavy_ranker_empty_history", 1.0),
            ("perspective", 1.0),
        ]
        assert cfg.diversify is True
        assert cfg.max_render_share == pytest.approx(0.5)
        assert cfg.min_rank_score == pytest.approx(0.425)
        assert cfg.min_mmr_score == pytest.approx(-0.05)

    def test_public_feeds_show_something_when_logged_out(self):
        """A published feed is visible to logged-out visitors, who would otherwise
        see it as empty and therefore broken (issue #384)."""
        for feed_name, cfg in FEEDS.items():
            if not cfg.public:
                continue
            assert cfg.logged_out in ("explain", "serve"), feed_name

    def test_private_feeds_deny_logged_out_requests(self):
        """Nobody is meant to be looking at a development feed, so it has nothing
        to say to a visitor without a session."""
        for feed_name, cfg in FEEDS.items():
            if cfg.public:
                continue
            assert cfg.logged_out == "deny", feed_name

    def test_random_is_the_only_feed_that_serves_itself_logged_out(self):
        """Its candidates don't depend on the caller."""
        serving = {name for name, cfg in FEEDS.items() if cfg.logged_out == "serve"}
        assert serving == {"random"}

    def test_personalized_feeds_explain_themselves_when_logged_out(self):
        """These need a user to rank for, so there is nothing to serve without one.
        Neither declares it — 'explain' is the FeedConfig default."""
        for feed_name in ("your-feed", "best-of-friends"):
            assert FEEDS[feed_name].logged_out == "explain"

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
        assert [spec.name for spec in cfg.rank_request_template.models] == [
            "heavy_ranker",
            "perspective",
        ]
        assert cfg.diversify is True
        assert (
            cfg.gen_request_template.generators
            == FEEDS["your-feed"].gen_request_template.generators
        )
        assert cfg.max_render_share == pytest.approx(0.5)
        assert cfg.min_rank_score == pytest.approx(0.425)
        assert cfg.min_mmr_score == pytest.approx(-0.05)


class TestFeedNameLengths:
    """Surface over-long feed names at definition time.

    ``publish_feed.py`` also asserts the composed published name fits (via
    ``_resolve_feed_publish_params``); these checks are the same budget applied
    directly to the raw metadata in ``feeds.py`` so the failure points at the
    offending field.
    """

    @pytest.mark.parametrize("feed_name,cfg", list(FEEDS.items()))
    def test_internal_display_name_fits_with_prefix_and_sha(self, feed_name, cfg):
        # Worst case: dev/stage publishes internal feeds as "GE <name> <sha>".
        composed = len(DEV_STAGE_PREFIX) + len(cfg.internal_display_name) + GIT_SHA_SUFFIX_LEN
        assert composed <= MAX_DISPLAY_NAME_GRAPHEMES, (
            f"{feed_name}: internal_display_name {cfg.internal_display_name!r} is too long — "
            f"'GE {cfg.internal_display_name} <sha>' would be {composed} chars "
            f"(limit {MAX_DISPLAY_NAME_GRAPHEMES})"
        )

    @pytest.mark.parametrize("feed_name,cfg", list(FEEDS.items()))
    def test_public_display_name_fits(self, feed_name, cfg):
        # Prod publishes public feeds under the raw display_name (no prefix/sha).
        assert len(cfg.display_name) <= MAX_DISPLAY_NAME_GRAPHEMES, (
            f"{feed_name}: display_name {cfg.display_name!r} exceeds "
            f"{MAX_DISPLAY_NAME_GRAPHEMES} graphemes"
        )

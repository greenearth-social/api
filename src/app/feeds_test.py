"""Tests for the FEEDS catalog."""

from app.feeds import FEEDS

ATTRIBUTION_LINE = "Built by GreenEarth Social (https://www.greenearth.social)"


class TestFeedDescriptions:
    def test_all_feed_descriptions_include_attribution(self):
        for feed_name, feed_cfg in FEEDS.items():
            assert ATTRIBUTION_LINE in feed_cfg.description, (
                f"Feed '{feed_name}' description missing attribution line"
            )

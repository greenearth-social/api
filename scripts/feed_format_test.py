import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("feed_format.py")
spec = importlib.util.spec_from_file_location("feed_format_mod", MODULE_PATH)
assert spec and spec.loader
feed_format = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feed_format)

relative_time = feed_format.relative_time
fmt_score = feed_format.fmt_score
media_badges = feed_format.media_badges


def _ago(**kwargs) -> datetime:
    return datetime.now(timezone.utc) - timedelta(**kwargs)


class TestRelativeTime:
    def test_seconds_reads_as_just_now(self):
        assert relative_time(_ago(seconds=5)) == "just now"

    def test_minutes(self):
        assert relative_time(_ago(minutes=5)) == "5m ago"

    def test_hours(self):
        assert relative_time(_ago(hours=3)) == "3h ago"

    def test_days(self):
        assert relative_time(_ago(days=4)) == "4d ago"

    def test_old_dates_fall_back_to_absolute(self):
        old = datetime(2020, 1, 15, tzinfo=timezone.utc)
        assert relative_time(old) == "Jan 15, 2020"

    def test_bad_input_does_not_raise(self):
        # A naive datetime can't be subtracted from an aware now(); the helper
        # must degrade to a string rather than blow up mid-render.
        assert relative_time(datetime(2020, 1, 1)) == str(datetime(2020, 1, 1))


class TestFmtScore:
    def test_none_is_a_dash(self):
        assert fmt_score(None) == "—"

    def test_formats_four_decimals(self):
        assert fmt_score(0.5) == "0.5000"

    def test_zero_is_not_treated_as_missing(self):
        assert fmt_score(0.0) == "0.0000"


class TestMediaBadges:
    def test_no_media_returns_none(self):
        assert media_badges() is None

    def test_count_wins_over_bool_and_pluralizes(self):
        badge = media_badges(image_count=3, contains_images=True)
        assert badge is not None
        assert badge.plain == "[3 images]"

    def test_singular_image(self):
        badge = media_badges(image_count=1)
        assert badge is not None
        assert badge.plain == "[1 image]"

    def test_bare_bool_without_count(self):
        badge = media_badges(contains_images=True)
        assert badge is not None
        assert badge.plain == "[image]"

    def test_video_and_link_combine(self):
        badge = media_badges(video_count=2, external_uri="https://example.com")
        assert badge is not None
        assert badge.plain == "[2 videos, link]"

    def test_zero_counts_are_not_media(self):
        assert media_badges(image_count=0, video_count=0) is None

import base64
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = Path(__file__).with_name("feed_view.py")
spec = importlib.util.spec_from_file_location("feed_view_cli", MODULE_PATH)
assert spec and spec.loader
feed_view = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feed_view)


def _feed_context(payload: dict) -> str:
    """A feedContext-shaped token: base64url(payload).signature."""
    raw = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"{raw}.made-up-signature"


class TestFeedUri:
    def test_bare_name_expands_with_publisher(self):
        assert (
            feed_view.feed_uri("popularity", "did:web:test")
            == "at://did:web:test/app.bsky.feed.generator/popularity"
        )

    def test_full_uri_passes_through(self):
        uri = "at://did:web:other/app.bsky.feed.generator/custom"
        assert feed_view.feed_uri(uri, "did:web:test") == uri


class TestRequestIdFromFeedContext:
    def test_extracts_rid(self):
        token = _feed_context({"did": "did:plc:x", "feed": "popularity", "rid": "abc123"})
        assert feed_view.request_id_from_feed_context(token) == "abc123"

    def test_none_context_returns_none(self):
        assert feed_view.request_id_from_feed_context(None) is None

    def test_empty_string_returns_none(self):
        assert feed_view.request_id_from_feed_context("") is None

    def test_missing_rid_returns_none(self):
        token = _feed_context({"did": "did:plc:x", "feed": "popularity"})
        assert feed_view.request_id_from_feed_context(token) is None

    def test_garbage_payload_returns_none(self):
        assert feed_view.request_id_from_feed_context("not-a-token.sig") is None

    def test_non_dict_payload_returns_none(self):
        raw = base64.urlsafe_b64encode(json.dumps([1, 2, 3]).encode()).decode().rstrip("=")
        assert feed_view.request_id_from_feed_context(f"{raw}.sig") is None

    def test_non_string_rid_returns_none(self):
        token = _feed_context({"rid": 42})
        assert feed_view.request_id_from_feed_context(token) is None


class TestParseCreatedAt:
    def test_parses_z_suffix(self):
        dt = feed_view._parse_created_at("2026-07-21T16:18:15Z")
        assert dt == datetime(2026, 7, 21, 16, 18, 15, tzinfo=timezone.utc)

    def test_none_returns_none(self):
        assert feed_view._parse_created_at(None) is None

    def test_bad_value_returns_none(self):
        assert feed_view._parse_created_at("not a date") is None


class TestAuthorLabel:
    def test_did_when_present(self):
        assert feed_view._author_label("did:plc:abc") == "did:plc:abc"

    def test_none_falls_back(self):
        assert feed_view._author_label(None) == "unknown author"


def _meta(**kwargs):
    base = dict(generators=[], rank=None, rank_score=None)
    base.update(kwargs)
    return SimpleNamespace(**base)


class TestRenderPost:
    def _source(self, **overrides):
        base = {
            "at_uri": "at://did:plc:abc/app.bsky.feed.post/xyz",
            "author_did": "did:plc:abc",
            "content": "hello world",
            "created_at": "2026-07-21T16:18:15Z",
            "like_count": 12,
        }
        base.update(overrides)
        return base

    def test_missing_source_is_flagged(self):
        text = feed_view.render_post(
            position=1, uri="at://did:plc:abc/app.bsky.feed.post/xyz", source=None, meta=None
        )
        assert "not in Elasticsearch" in text.plain

    def test_renders_author_content_and_likes(self):
        text = feed_view.render_post(
            position=2, uri="at://x", source=self._source(), meta=None
        ).plain
        assert "did:plc:abc" in text
        assert "hello world" in text
        assert "12" in text

    def test_empty_content_shows_placeholder(self):
        text = feed_view.render_post(
            position=1, uri="at://x", source=self._source(content=""), meta=None
        ).plain
        assert "(no text)" in text

    def test_pipeline_meta_adds_generator_and_rank(self):
        meta = _meta(
            generators=[SimpleNamespace(name="popularity", score=1.0)], rank=3, rank_score=0.42
        )
        text = feed_view.render_post(
            position=1, uri="at://x", source=self._source(), meta=meta
        ).plain
        assert "popularity" in text
        assert "#3" in text
        assert "0.4200" in text

    def test_no_meta_omits_pipeline_line(self):
        text = feed_view.render_post(
            position=1, uri="at://x", source=self._source(), meta=None
        ).plain
        assert "via" not in text


class TestBuildParser:
    def test_defaults(self):
        args = feed_view.build_parser().parse_args([])
        assert args.feed == feed_view.DEFAULT_FEED
        assert args.limit == 20
        assert args.pages == 1
        assert args.no_pipeline is False

    def test_feed_and_flags(self):
        args = feed_view.build_parser().parse_args(
            ["random", "--user", "did:plc:z", "--limit", "5", "--no-pipeline"]
        )
        assert args.feed == "random"
        assert args.user == "did:plc:z"
        assert args.limit == 5
        assert args.no_pipeline is True

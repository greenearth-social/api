import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import manage_ux_posts
import managed_posts
import pytest

from app import ux_posts


def _record(uri: str, text: str, links: tuple[str, ...] = (), created: str | None = None) -> dict:
    """Build a listRecords-shaped post record with link facets."""
    facets = [
        {"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"uri": link}]} for link in links
    ]
    return {
        "uri": uri,
        "value": {
            "text": text,
            "facets": facets,
            "createdAt": created or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
    }


def _record_for(name: str, uri: str, created: str | None = None) -> dict:
    """Build a record that exactly matches a managed post's content."""
    text, links = managed_posts.content_signature(ux_posts.read_content(name))
    return _record(uri, text, links, created)


@pytest.fixture
def manifest_path(tmp_path, monkeypatch):
    path = tmp_path / "ux_posts_resolved.json"
    monkeypatch.setattr(ux_posts, "MANIFEST_PATH", path)
    return path


# --- content ----------------------------------------------------------------


class TestContent:
    """The pin text assertions that used to live in feeds_test.py, now against
    the markdown that is actually published."""

    EXPECTED_PINS = {
        "pin-your-feed.md": (
            "your-feed",
            "Click SETTINGS to personalize your MySky feed.\n\n"
            "A feed you control, designed for constructive conversation.",
        ),
        "pin-best-of-friends.md": (
            "best-of-friends",
            "Click SETTINGS to personalize your feed.\n\n"
            "The best posts from your mutuals and people you follow. "
            "Part of the GreenEarth Family.",
        ),
        "pin-random.md": (
            "random",
            "Click SETTINGS to personalize your feed.\n\n"
            "A random slice of the ATProto universe. Still applies your moderation "
            "settings. Part of the GreenEarth Family.",
        ),
    }

    def test_pins_render_the_expected_text_and_settings_link(self):
        for name, (feed_name, expected) in self.EXPECTED_PINS.items():
            text, links = managed_posts.content_signature(ux_posts.read_content(name))
            assert text == expected
            assert links == (f"https://app.greenearth.social/#/settings/{feed_name}",)

    def test_every_post_is_within_the_length_limit(self):
        for name in ux_posts.MANAGED_POSTS:
            text, _ = managed_posts.content_signature(ux_posts.read_content(name))
            assert len(text) <= ux_posts.MAX_POST_GRAPHEMES, name

    def test_check_passes_on_the_real_content(self):
        assert manage_ux_posts.check_content() == []

    def test_check_reports_an_unregistered_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(ux_posts, "CONTENT_DIR", tmp_path)
        for name in ux_posts.MANAGED_POSTS:
            (tmp_path / name).write_text("hello")
        (tmp_path / "stray.md").write_text("hello")
        problems = manage_ux_posts.check_content()
        assert any("stray.md" in p and "MANAGED_POSTS" in p for p in problems)

    def test_check_reports_a_missing_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(ux_posts, "CONTENT_DIR", tmp_path)
        for name in ux_posts.MANAGED_POSTS[1:]:
            (tmp_path / name).write_text("hello")
        problems = manage_ux_posts.check_content()
        assert any(ux_posts.MANAGED_POSTS[0] in p and "missing" in p for p in problems)

    def test_check_reports_an_over_long_post(self, monkeypatch, tmp_path):
        monkeypatch.setattr(ux_posts, "CONTENT_DIR", tmp_path)
        for name in ux_posts.MANAGED_POSTS:
            (tmp_path / name).write_text("hello")
        (tmp_path / ux_posts.PIN_RANDOM).write_text("x" * 301)
        problems = manage_ux_posts.check_content()
        assert any("over the 300" in p for p in problems)

    def test_check_reports_an_empty_post(self, monkeypatch, tmp_path):
        monkeypatch.setattr(ux_posts, "CONTENT_DIR", tmp_path)
        for name in ux_posts.MANAGED_POSTS:
            (tmp_path / name).write_text("hello")
        (tmp_path / ux_posts.PIN_RANDOM).write_text("   \n")
        assert any("is empty" in p for p in manage_ux_posts.check_content())


# --- matching ---------------------------------------------------------------


class TestMatching:
    def test_exact_content_match_is_reused(self):
        """The whole point: unchanged content must not republish."""
        records = [_record_for(ux_posts.PIN_RANDOM, "at://existing")]
        resolved, missing = manage_ux_posts.match_content(records)
        assert resolved[ux_posts.PIN_RANDOM] == "at://existing"
        assert ux_posts.PIN_RANDOM not in missing

    def test_changed_content_does_not_match(self):
        records = [_record("at://stale", "some older wording", ())]
        resolved, missing = manage_ux_posts.match_content(records)
        assert resolved == {}
        assert set(missing) == set(ux_posts.MANAGED_POSTS)

    def test_matching_requires_the_links_to_agree(self):
        """Same visible text with a different link target is a different post."""
        text, _links = managed_posts.content_signature(ux_posts.read_content(ux_posts.PIN_RANDOM))
        records = [_record("at://wrong-link", text, ("https://example.com/elsewhere",))]
        _resolved, missing = manage_ux_posts.match_content(records)
        assert ux_posts.PIN_RANDOM in missing

    def test_later_duplicate_wins(self):
        records = [
            _record_for(ux_posts.PIN_RANDOM, "at://older"),
            _record_for(ux_posts.PIN_RANDOM, "at://newer"),
        ]
        resolved, _missing = manage_ux_posts.match_content(records)
        assert resolved[ux_posts.PIN_RANDOM] == "at://newer"


# --- manifest ---------------------------------------------------------------


class TestManifest:
    def test_manifest_is_sorted_and_records_the_publisher(self, manifest_path):
        manage_ux_posts.write_manifest({"b.md": "at://b", "a.md": "at://a"})
        payload = json.loads(manifest_path.read_text())
        assert list(payload["posts"]) == ["a.md", "b.md"]
        assert payload["publisher"] == ux_posts.PUBLISHER_DID
        assert payload["schema_version"] == ux_posts.MANIFEST_SCHEMA_VERSION

    def test_manifest_round_trips_into_the_registry(self, manifest_path, monkeypatch):
        """Ties the deploy-time producer to the runtime consumer."""
        manage_ux_posts.write_manifest({ux_posts.PIN_RANDOM: "at://written"})
        monkeypatch.delenv(ux_posts.URI_ENV_VAR, raising=False)
        ux_posts._reset_cache_for_tests()
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://written"
        ux_posts._reset_cache_for_tests()


# --- sync -------------------------------------------------------------------


class TestSync:
    def _args(self, **kw):
        defaults = dict(dry_run=False, handle=None, app_password="pw", project_id="p")
        return SimpleNamespace(**{**defaults, **kw})

    def test_sync_publishes_only_missing_posts(self, manifest_path):
        existing = [_record_for(name, f"at://{name}") for name in ux_posts.MANAGED_POSTS[:-1]]
        missing_name = ux_posts.MANAGED_POSTS[-1]
        client = MagicMock()
        client.send_post.return_value = SimpleNamespace(uri="at://freshly-published")

        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=existing),
            patch.object(managed_posts, "login", return_value=client),
        ):
            assert manage_ux_posts.cmd_sync(self._args()) == 0

        assert client.send_post.call_count == 1
        posts = json.loads(manifest_path.read_text())["posts"]
        assert posts[missing_name] == "at://freshly-published"
        assert len(posts) == len(ux_posts.MANAGED_POSTS)

    def test_sync_is_a_noop_when_everything_is_published(self, manifest_path):
        existing = [_record_for(name, f"at://{name}") for name in ux_posts.MANAGED_POSTS]
        client = MagicMock()
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=existing),
            patch.object(managed_posts, "login", return_value=client),
        ):
            assert manage_ux_posts.cmd_sync(self._args()) == 0
        client.send_post.assert_not_called()
        assert len(json.loads(manifest_path.read_text())["posts"]) == len(ux_posts.MANAGED_POSTS)

    def test_dry_run_publishes_nothing(self, manifest_path):
        client = MagicMock()
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=[]),
            patch.object(managed_posts, "login", return_value=client),
        ):
            assert manage_ux_posts.cmd_sync(self._args(dry_run=True)) == 0
        client.send_post.assert_not_called()
        assert not manifest_path.exists()

    def test_sync_refuses_invalid_content(self, monkeypatch, tmp_path, manifest_path):
        monkeypatch.setattr(ux_posts, "CONTENT_DIR", tmp_path)
        for name in ux_posts.MANAGED_POSTS:
            (tmp_path / name).write_text("x" * 301)
        with patch.object(manage_ux_posts, "fetch_repo_posts") as fetch:
            assert manage_ux_posts.cmd_sync(self._args()) == 1
        fetch.assert_not_called()


class TestResolve:
    def test_require_complete_fails_when_a_post_is_unpublished(self, manifest_path):
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=[]),
            patch.object(manage_ux_posts, "ungated_posts", return_value=[]),
        ):
            rc = manage_ux_posts.cmd_resolve(SimpleNamespace(require_complete=True))
        assert rc == 1

    def test_resolve_writes_the_manifest_without_credentials(self, manifest_path):
        existing = [_record_for(name, f"at://{name}") for name in ux_posts.MANAGED_POSTS]
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=existing),
            patch.object(manage_ux_posts, "ungated_posts", return_value=[]),
        ):
            rc = manage_ux_posts.cmd_resolve(SimpleNamespace(require_complete=True))
        assert rc == 0
        assert len(json.loads(manifest_path.read_text())["posts"]) == len(ux_posts.MANAGED_POSTS)


# --- cleanup ----------------------------------------------------------------


class TestCleanup:
    def _args(self, **kw):
        defaults = dict(
            older_than_days=30, yes=False, handle=None, app_password="pw", project_id="p"
        )
        return SimpleNamespace(**{**defaults, **kw})

    def _old(self, days: int) -> str:
        return (datetime.now(UTC) - timedelta(days=days)).isoformat().replace("+00:00", "Z")

    def test_current_posts_are_never_candidates(self, capsys):
        records = [
            _record_for(name, f"at://{name}", self._old(999)) for name in ux_posts.MANAGED_POSTS
        ]
        with patch.object(manage_ux_posts, "fetch_repo_posts", return_value=records):
            assert manage_ux_posts.cmd_cleanup(self._args()) == 0
        assert "Nothing to clean up." in capsys.readouterr().out

    def test_recent_unreferenced_posts_are_protected(self, capsys):
        records = [_record("at://recent", "superseded wording", (), self._old(3))]
        with patch.object(manage_ux_posts, "fetch_repo_posts", return_value=records):
            manage_ux_posts.cmd_cleanup(self._args())
        assert "Nothing to clean up." in capsys.readouterr().out

    def test_old_unreferenced_posts_are_listed_but_not_deleted_without_yes(self, capsys):
        records = [_record("at://old", "superseded wording", (), self._old(99))]
        client = MagicMock()
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=records),
            patch.object(managed_posts, "login", return_value=client),
        ):
            assert manage_ux_posts.cmd_cleanup(self._args()) == 0
        client.com.atproto.repo.delete_record.assert_not_called()
        assert "--yes" in capsys.readouterr().out

    def test_yes_deletes_old_unreferenced_posts(self):
        records = [_record("at://did:plc:x/app.bsky.feed.post/abc", "old", (), self._old(99))]
        client = MagicMock()
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=records),
            patch.object(managed_posts, "login", return_value=client),
        ):
            assert manage_ux_posts.cmd_cleanup(self._args(yes=True)) == 0
        client.com.atproto.repo.delete_record.assert_called_once()


# --- gating -----------------------------------------------------------------


class TestGates:
    """UX posts are one-way notices: replies and quotes are turned off. Likes
    cannot be disabled -- atproto has no like-gating."""

    def test_threadgate_allows_nobody(self):
        record = managed_posts.build_threadgate_record("at://post", "2026-01-01T00:00:00Z")
        # An empty list means "nobody"; omitting allow entirely would mean "everybody",
        # so this assertion is guarding a real footgun.
        assert record.allow == []
        assert record.post == "at://post"

    def test_postgate_disables_quotes(self):
        record = managed_posts.build_postgate_record("at://post", "2026-01-01T00:00:00Z")
        assert record.embedding_rules
        dumped = record.model_dump(exclude_none=True)
        assert dumped["embedding_rules"] == [{"py_type": "app.bsky.feed.postgate#disableRule"}]

    def test_ungated_when_a_gate_is_missing(self):
        resolved = {ux_posts.PIN_RANDOM: "at://did:plc:x/app.bsky.feed.post/abc"}
        with patch.object(manage_ux_posts, "fetch_gate_rkeys", side_effect=[{"abc"}, set()]):
            assert manage_ux_posts.ungated_posts(resolved) == [ux_posts.PIN_RANDOM]

    def test_not_ungated_when_both_gates_exist(self):
        resolved = {ux_posts.PIN_RANDOM: "at://did:plc:x/app.bsky.feed.post/abc"}
        with patch.object(manage_ux_posts, "fetch_gate_rkeys", side_effect=[{"abc"}, {"abc"}]):
            assert manage_ux_posts.ungated_posts(resolved) == []

    def test_apply_gates_writes_both_records_at_the_post_rkey(self):
        client = MagicMock()
        resolved = {ux_posts.PIN_RANDOM: "at://did:plc:x/app.bsky.feed.post/abc"}
        manage_ux_posts.apply_gates(client, resolved, [ux_posts.PIN_RANDOM])

        calls = client.com.atproto.repo.put_record.call_args_list
        assert len(calls) == 2
        collections = {c.args[0].collection for c in calls}
        assert collections == {
            managed_posts.THREADGATE_COLLECTION,
            managed_posts.POSTGATE_COLLECTION,
        }
        # The gate rkey must equal the post's rkey or it governs nothing.
        assert {c.args[0].rkey for c in calls} == {"abc"}

    def test_sync_gates_a_newly_published_post(self, manifest_path):
        client = MagicMock()
        client.send_post.return_value = SimpleNamespace(
            uri="at://did:plc:x/app.bsky.feed.post/new"
        )
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=[]),
            patch.object(manage_ux_posts, "ungated_posts", return_value=[]),
            patch.object(managed_posts, "login", return_value=client),
            patch.object(manage_ux_posts, "apply_gates") as gates,
        ):
            args = SimpleNamespace(dry_run=False, handle=None, app_password="pw", project_id="p")
            assert manage_ux_posts.cmd_sync(args) == 0
        assert set(gates.call_args.args[2]) == set(ux_posts.MANAGED_POSTS)

    def test_sync_gates_an_existing_ungated_post(self, manifest_path):
        existing = [_record_for(name, f"at://{name}") for name in ux_posts.MANAGED_POSTS]
        client = MagicMock()
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=existing),
            patch.object(manage_ux_posts, "ungated_posts", return_value=[ux_posts.PIN_RANDOM]),
            patch.object(managed_posts, "login", return_value=client),
            patch.object(manage_ux_posts, "apply_gates") as gates,
        ):
            args = SimpleNamespace(dry_run=False, handle=None, app_password="pw", project_id="p")
            assert manage_ux_posts.cmd_sync(args) == 0
        client.send_post.assert_not_called()
        assert gates.call_args.args[2] == [ux_posts.PIN_RANDOM]

    def test_resolve_require_complete_fails_on_an_ungated_post(self, manifest_path):
        existing = [_record_for(name, f"at://{name}") for name in ux_posts.MANAGED_POSTS]
        with (
            patch.object(manage_ux_posts, "fetch_repo_posts", return_value=existing),
            patch.object(manage_ux_posts, "ungated_posts", return_value=[ux_posts.PIN_RANDOM]),
        ):
            rc = manage_ux_posts.cmd_resolve(SimpleNamespace(require_complete=True))
        assert rc == 1

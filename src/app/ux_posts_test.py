import json

import pytest

from app import ux_posts


@pytest.fixture
def manifest(tmp_path, monkeypatch):
    """Point the registry at a manifest this test controls."""

    def write(posts: dict[str, str] | None, *, malformed: str | None = None):
        path = tmp_path / "ux_posts_resolved.json"
        if malformed is not None:
            path.write_text(malformed)
        elif posts is not None:
            path.write_text(
                json.dumps(
                    {
                        "schema_version": ux_posts.MANIFEST_SCHEMA_VERSION,
                        "publisher": ux_posts.PUBLISHER_DID,
                        "posts": posts,
                    }
                )
            )
        monkeypatch.setattr(ux_posts, "MANIFEST_PATH", path)
        monkeypatch.delenv(ux_posts.URI_ENV_VAR, raising=False)
        ux_posts._reset_cache_for_tests()

    yield write
    monkeypatch.undo()
    ux_posts._reset_cache_for_tests()


# --- resolution -------------------------------------------------------------


class TestUxPostUri:
    def test_returns_the_manifest_uri(self, manifest):
        manifest({ux_posts.PIN_RANDOM: "at://published"})
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://published"

    def test_unpublished_post_falls_back_to_the_placeholder(self, manifest):
        """A newly added post should be visibly wrong in a dev feed, not missing."""
        manifest({ux_posts.PLACEHOLDER: "at://placeholder"})
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://placeholder"

    def test_placeholder_does_not_fall_back_to_itself(self, manifest):
        manifest({ux_posts.PIN_RANDOM: "at://published"})
        assert ux_posts.ux_post_uri(ux_posts.PLACEHOLDER) is None

    def test_returns_none_when_nothing_is_resolved(self, manifest):
        manifest({})
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) is None

    def test_missing_manifest_is_not_fatal(self, manifest):
        """A fresh checkout has no manifest; importing the app must still work."""
        manifest(None)
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) is None

    def test_malformed_manifest_is_not_fatal(self, manifest):
        manifest(None, malformed="{not json")
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) is None

    def test_blank_uri_is_treated_as_unresolved(self, manifest):
        manifest({ux_posts.PIN_RANDOM: "", ux_posts.PLACEHOLDER: "at://placeholder"})
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://placeholder"


class TestEnvironmentOverride:
    def test_override_wins_over_the_manifest(self, manifest, monkeypatch):
        manifest({ux_posts.PIN_RANDOM: "at://from-manifest"})
        monkeypatch.setenv(ux_posts.URI_ENV_VAR, json.dumps({ux_posts.PIN_RANDOM: "at://pinned"}))
        ux_posts._reset_cache_for_tests()
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://pinned"

    def test_override_supplements_the_manifest(self, manifest, monkeypatch):
        manifest({ux_posts.PIN_RANDOM: "at://from-manifest"})
        monkeypatch.setenv(ux_posts.URI_ENV_VAR, json.dumps({ux_posts.PIN_YOUR_FEED: "at://extra"}))
        ux_posts._reset_cache_for_tests()
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://from-manifest"
        assert ux_posts.ux_post_uri(ux_posts.PIN_YOUR_FEED) == "at://extra"

    def test_malformed_override_falls_back_to_the_manifest(self, manifest, monkeypatch):
        manifest({ux_posts.PIN_RANDOM: "at://from-manifest"})
        monkeypatch.setenv(ux_posts.URI_ENV_VAR, "{not json")
        ux_posts._reset_cache_for_tests()
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://from-manifest"

    def test_non_object_override_is_ignored(self, manifest, monkeypatch):
        manifest({ux_posts.PIN_RANDOM: "at://from-manifest"})
        monkeypatch.setenv(ux_posts.URI_ENV_VAR, '["a"]')
        ux_posts._reset_cache_for_tests()
        assert ux_posts.ux_post_uri(ux_posts.PIN_RANDOM) == "at://from-manifest"


# --- publisher --------------------------------------------------------------


class TestPublisher:
    def test_defaults_to_the_production_notifications_account(self, monkeypatch):
        monkeypatch.delenv(ux_posts.PUBLISHER_DID_ENV_VAR, raising=False)
        monkeypatch.delenv(ux_posts.PUBLISHER_HANDLE_ENV_VAR, raising=False)

        assert ux_posts.publisher_did() == ux_posts.PUBLISHER_DID
        assert ux_posts.publisher_handle() == ux_posts.PUBLISHER_HANDLE

    def test_deployment_can_select_the_stage_account(self, monkeypatch):
        monkeypatch.setenv(ux_posts.PUBLISHER_DID_ENV_VAR, "did:plc:stage")
        monkeypatch.setenv(ux_posts.PUBLISHER_HANDLE_ENV_VAR, "stage.example")

        assert ux_posts.publisher_did() == "did:plc:stage"
        assert ux_posts.publisher_handle() == "stage.example"


# --- catalog ----------------------------------------------------------------


class TestCatalog:
    def test_every_managed_post_has_content(self):
        for name in ux_posts.MANAGED_POSTS:
            assert ux_posts.content_path(name).is_file(), f"{name} is missing from assets/ux_posts"

    def test_every_managed_video_post_has_its_asset(self):
        for name in ux_posts.VIDEO_POSTS:
            path = ux_posts.video_path(name)
            assert path and path.is_file(), f"{name} is missing its video asset"

    def test_video_assets_have_no_unregistered_files(self):
        present = {p.name for p in ux_posts.CONTENT_DIR.glob("*.mp4")}
        expected = {spec.filename for spec in ux_posts.VIDEO_POSTS.values()}
        assert present == expected

    def test_content_directory_has_no_unregistered_files(self):
        present = {p.name for p in ux_posts.CONTENT_DIR.glob("*.md")}
        assert present == set(ux_posts.MANAGED_POSTS)

    def test_catalog_entries_are_unique(self):
        assert len(set(ux_posts.MANAGED_POSTS)) == len(ux_posts.MANAGED_POSTS)

    def test_video_posts_are_managed(self):
        assert set(ux_posts.VIDEO_POSTS) <= set(ux_posts.MANAGED_POSTS)

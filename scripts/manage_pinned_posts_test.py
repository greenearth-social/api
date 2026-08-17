"""Tests for repository-managed Bluesky feed pins."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from manage_pinned_posts import (
    POST_COLLECTION,
    build_post_record,
    deployed_pin_state,
    ensure_pinned_post,
    list_repo_posts,
    pinned_post_config_sha,
    sync_pinned_posts,
)


def test_config_sha_changes_only_with_managed_content():
    config = {"your-feed": "first", "random": "same"}
    assert pinned_post_config_sha(config) == pinned_post_config_sha(dict(reversed(config.items())))
    assert pinned_post_config_sha(config) != pinned_post_config_sha(
        {"your-feed": "second", "random": "same"}
    )


def test_extracts_deployed_pin_state_from_cloud_run_service_json():
    service = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "env": [
                                {"name": "GE_GIT_SHA", "value": "abc123"},
                                {"name": "GE_PINNED_POST_CONFIG_SHA", "value": "pin-sha"},
                                {"name": "GE_PINNED_POST_RANDOM_URI", "value": "at://random"},
                            ]
                        }
                    ]
                }
            }
        }
    }
    assert deployed_pin_state(service) == {
        "GE_PINNED_POST_CONFIG_SHA": "pin-sha",
        "GE_PINNED_POST_RANDOM_URI": "at://random",
    }


def test_build_post_record_creates_settings_link_facet():
    record = build_post_record(
        "Click [SETTINGS](https://app.greenearth.social/#/settings/your-feed) now."
    )
    assert record.text == "Click SETTINGS now."
    assert len(record.facets or []) == 1
    feature = record.facets[0].features[0]
    assert feature.uri == "https://app.greenearth.social/#/settings/your-feed"


def test_existing_exact_post_is_reused():
    client = MagicMock()
    content = "Click [SETTINGS](https://example.com/settings) now."
    existing = SimpleNamespace(
        uri="at://did:plc:account/app.bsky.feed.post/validtid",
        value=build_post_record(content),
    )

    uri = ensure_pinned_post(
        client, "did:plc:account", "random", content, existing_posts=[existing]
    )

    assert uri.endswith("/validtid")
    client.send_post.assert_not_called()


def test_missing_post_is_published():
    client = MagicMock()
    client.send_post.return_value = SimpleNamespace(
        uri="at://did:plc:account/app.bsky.feed.post/new"
    )

    uri = ensure_pinned_post(
        client, "did:plc:account", "your-feed", "new content", existing_posts=[]
    )

    assert uri.endswith("/new")
    builder = client.send_post.call_args.args[0]
    assert builder.build_text() == "new content"


def test_list_repo_posts_follows_cursors():
    client = MagicMock()
    client.com.atproto.repo.list_records.side_effect = [
        SimpleNamespace(records=["new"], cursor="next"),
        SimpleNamespace(records=["old"], cursor=None),
    ]

    assert list_repo_posts(client, "did:plc:account") == ["new", "old"]
    first, second = client.com.atproto.repo.list_records.call_args_list
    assert first.args[0].collection == POST_COLLECTION
    assert first.args[0].cursor is None
    assert second.args[0].cursor == "next"


@patch("manage_pinned_posts.Client")
def test_sync_publishes_all_three_public_feed_pins(MockClient):
    client = MockClient.return_value
    client.login.return_value = SimpleNamespace(did="did:plc:account")
    client.com.atproto.repo.list_records.return_value = SimpleNamespace(records=[], cursor=None)
    client.send_post.side_effect = [
        SimpleNamespace(uri=f"at://did:plc:account/{POST_COLLECTION}/tid-{index}")
        for index in range(3)
    ]

    posts = sync_pinned_posts("greenearth-social.bsky.social", "password")

    assert set(posts) == {"your-feed", "best-of-friends", "random"}
    assert client.com.atproto.repo.list_records.call_count == 1
    assert client.send_post.call_count == 3

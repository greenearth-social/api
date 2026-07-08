import pytest
from unittest.mock import MagicMock, call, patch
from manage_post import parse_content


def test_plain_text():
    result = parse_content("hello world")
    assert result == [{"type": "text", "text": "hello world"}]


def test_single_link():
    result = parse_content("check [this](https://example.com) out")
    assert result == [
        {"type": "text", "text": "check "},
        {"type": "link", "text": "this", "url": "https://example.com"},
        {"type": "text", "text": " out"},
    ]


def test_link_at_end():
    result = parse_content("visit [site](https://example.com)")
    assert result == [
        {"type": "text", "text": "visit "},
        {"type": "link", "text": "site", "url": "https://example.com"},
    ]


def test_multiple_links():
    result = parse_content("[a](https://a.com) and [b](https://b.com)")
    assert result == [
        {"type": "link", "text": "a", "url": "https://a.com"},
        {"type": "text", "text": " and "},
        {"type": "link", "text": "b", "url": "https://b.com"},
    ]


def test_no_trailing_empty_text():
    result = parse_content("[link](https://x.com)")
    assert all(not (s["type"] == "text" and s["text"] == "") for s in result)


def test_build_text_builder_plain():
    from manage_post import build_text_builder
    segments = [{"type": "text", "text": "hello world"}]
    tb = MagicMock()
    with patch("manage_post.client_utils.TextBuilder", return_value=tb):
        build_text_builder(segments)
    tb.text.assert_called_once_with("hello world")
    tb.link.assert_not_called()


def test_build_text_builder_with_link():
    from manage_post import build_text_builder
    segments = [
        {"type": "text", "text": "visit "},
        {"type": "link", "text": "site", "url": "https://example.com"},
    ]
    tb = MagicMock()
    with patch("manage_post.client_utils.TextBuilder", return_value=tb):
        build_text_builder(segments)
    tb.text.assert_called_once_with("visit ")
    tb.link.assert_called_once_with("site", "https://example.com")


def test_cmd_update_merges_record_correctly(tmp_path):
    from manage_post import cmd_update
    import argparse

    post_file = tmp_path / "post.txt"
    post_file.write_text("Hello [link](https://example.com)")

    existing_value = MagicMock()
    existing_value.model_dump.return_value = {
        "$type": "app.bsky.feed.post",
        "text": "old text",
        "createdAt": "2026-01-01T00:00:00.000Z",
    }

    mock_get_resp = MagicMock()
    mock_get_resp.value = existing_value

    captured = {}

    def fake_put_record(params):
        captured["record"] = params["record"]
        return MagicMock()

    mock_client = MagicMock()
    mock_client.me.did = "did:plc:test"
    mock_client.com.atproto.repo.get_record.return_value = mock_get_resp
    mock_client.com.atproto.repo.put_record.side_effect = fake_put_record

    args = argparse.Namespace(
        at_uri="at://did:plc:test/app.bsky.feed.post/3abc",
        handle="test.bsky.social",
        file=str(post_file),
        dry_run=False,
    )

    with patch("manage_post._login", return_value=mock_client):
        cmd_update(args)

    # Verify model_dump was called with by_alias=True
    existing_value.model_dump.assert_called_once_with(by_alias=True)

    record = captured["record"]
    assert record["$type"] == "app.bsky.feed.post"
    assert record["createdAt"] == "2026-01-01T00:00:00.000Z"
    assert record["text"] == "Hello link"
    assert len(record["facets"]) == 1

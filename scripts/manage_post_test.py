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


def test_bracketed_display_text():
    result = parse_content("check [[link]](https://example.com) out")
    assert result == [
        {"type": "text", "text": "check "},
        {"type": "link", "text": "[link]", "url": "https://example.com"},
        {"type": "text", "text": " out"},
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



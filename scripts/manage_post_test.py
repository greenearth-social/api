import pytest
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

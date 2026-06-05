"""Tests for shared candidate construction helpers."""

from __future__ import annotations

from .utils import CANDIDATE_SOURCE_FIELDS, candidate_post_from_hit


def test_candidate_post_from_hit_populates_media_fields():
    hit = {
        "_score": 1.5,
        "_source": {
            "at_uri": "at://post/1",
            "author_did": "did:plc:author",
            "content": "hello world",
            "contains_images": True,
            "contains_video": False,
            "image_count": 2,
            "video_count": 0,
            "external_embed": {"uri": "https://example.com", "title": "x"},
        },
    }

    c = candidate_post_from_hit(hit, generator_name="popularity")

    assert c.at_uri == "at://post/1"
    assert c.author_did == "did:plc:author"
    assert c.contains_images is True
    assert c.contains_video is False
    assert c.image_count == 2
    assert c.video_count == 0
    assert c.external_uri == "https://example.com"
    assert c.generator_name == "popularity"


def test_candidate_post_from_hit_handles_missing_media():
    hit = {"_score": 0.1, "_source": {"at_uri": "at://post/2", "content": "no media"}}

    c = candidate_post_from_hit(hit)

    assert c.contains_images is None
    assert c.image_count is None
    assert c.external_uri is None


def test_media_fields_requested_from_es():
    for field in ("contains_images", "image_count", "video_count", "external_embed"):
        assert field in CANDIDATE_SOURCE_FIELDS

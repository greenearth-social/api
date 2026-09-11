"""Tests for shared candidate construction helpers."""

from __future__ import annotations

from ..embeddings import MINILM_L12_EMBEDDING_KEY
from ..topic_scores import POLITICS_KEY
from .utils import CANDIDATE_SOURCE_FIELDS, candidate_post_from_hit

SAMPLE_EMBEDDING = [0.1, 0.2, 0.3]


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


def test_candidate_post_from_hit_extracts_politics_score():
    candidate = candidate_post_from_hit(
        {
            "_source": {
                "at_uri": "at://post/politics",
                "content": "some news",
                "topic_scores": {POLITICS_KEY: 0.72, "Other": 0.18},
            }
        }
    )

    assert candidate.politics_score == 0.72


def test_candidate_post_from_hit_accepts_politics_score_boundaries():
    candidates = [
        candidate_post_from_hit(
            {
                "_source": {
                    "at_uri": f"at://post/{score}",
                    "content": "some news",
                    "topic_scores": {POLITICS_KEY: score},
                }
            }
        )
        for score in (0.0, 1.0)
    ]

    assert [candidate.politics_score for candidate in candidates] == [0.0, 1.0]


def test_candidate_post_from_hit_ignores_out_of_range_politics_scores():
    candidates = [
        candidate_post_from_hit(
            {
                "_source": {
                    "at_uri": f"at://post/{score}",
                    "content": "some news",
                    "topic_scores": {POLITICS_KEY: score},
                }
            }
        )
        for score in (-0.01, 1.01)
    ]

    assert [candidate.politics_score for candidate in candidates] == [None, None]


def test_candidate_post_from_hit_ignores_missing_or_malformed_politics_score():
    missing = candidate_post_from_hit(
        {"_source": {"at_uri": "at://post/missing", "content": "hello"}}
    )
    non_numeric = candidate_post_from_hit(
        {
            "_source": {
                "at_uri": "at://post/non-numeric",
                "content": "hello",
                "topic_scores": {POLITICS_KEY: "high"},
            }
        }
    )
    malformed_topics = candidate_post_from_hit(
        {
            "_source": {
                "at_uri": "at://post/malformed",
                "content": "hello",
                "topic_scores": [0.9],
            }
        }
    )

    assert missing.politics_score is None
    assert non_numeric.politics_score is None
    assert malformed_topics.politics_score is None


def test_candidate_post_from_hit_keeps_embedding_with_content_source():
    candidate = candidate_post_from_hit({
        "_source": {
            "at_uri": "at://post/1",
            "content": "hello",
            "embeddings": {MINILM_L12_EMBEDDING_KEY: SAMPLE_EMBEDDING},
        }
    })
    assert candidate.minilm_l12_embedding is not None


def test_candidate_post_from_hit_strips_embedding_without_nonblank_source_text():
    candidate = candidate_post_from_hit({
        "_source": {
            "at_uri": "at://post/1",
            "content": "   ",
            "media": [{"alt_text": ""}, {"alt_text": "  "}, "bad"],
            "video_transcript": 123,
            "embeddings": {MINILM_L12_EMBEDDING_KEY: SAMPLE_EMBEDDING},
        }
    })
    assert candidate.minilm_l12_embedding is None


def test_candidate_metadata_fields_requested_from_es():
    for field in (
        "contains_images",
        "image_count",
        "video_count",
        "external_embed",
        "topic_scores",
    ):
        assert field in CANDIDATE_SOURCE_FIELDS

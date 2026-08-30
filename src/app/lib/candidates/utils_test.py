"""Tests for shared candidate construction helpers."""

from __future__ import annotations

from ..embeddings import MINILM_L12_EMBEDDING_KEY
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


def test_media_fields_requested_from_es():
    for field in ("contains_images", "image_count", "video_count", "external_embed"):
        assert field in CANDIDATE_SOURCE_FIELDS


def test_perspective_fields_requested_from_es():
    """Every generator reads CANDIDATE_SOURCE_FIELDS, so this one list is what
    makes ingest-computed scores reach the ranker from all of them."""
    for field in ("combined_perspective_score", "perspective_scored_at"):
        assert field in CANDIDATE_SOURCE_FIELDS


def test_candidate_post_from_hit_reads_perspective_scores():
    candidate = candidate_post_from_hit({
        "_source": {
            "at_uri": "at://post/1",
            "content": "hello",
            "combined_perspective_score": 0.73,
            "perspective_scored_at": "2026-08-28T00:00:00Z",
        }
    })
    assert candidate.combined_perspective_score == 0.73
    assert candidate.perspective_scored_at == "2026-08-28T00:00:00Z"


def test_candidate_post_from_hit_keeps_zero_perspective_score():
    """0.0 is maximally toxic, not missing. A falsy-check here would re-score
    exactly the posts the ranker most needs to demote."""
    candidate = candidate_post_from_hit({
        "_source": {
            "at_uri": "at://post/1",
            "content": "hello",
            "combined_perspective_score": 0.0,
            "perspective_scored_at": "2026-08-28T00:00:00Z",
        }
    })
    assert candidate.combined_perspective_score == 0.0


def test_candidate_post_from_hit_without_perspective_fields():
    """An index that has never seen these fields — i.e. this api deployed
    ahead of the ingest change — must produce candidates that simply fall
    back to live scoring."""
    candidate = candidate_post_from_hit({
        "_source": {"at_uri": "at://post/1", "content": "hello"}
    })
    assert candidate.combined_perspective_score is None
    assert candidate.perspective_scored_at is None


def test_candidate_post_from_hit_unscorable_post():
    """Stamped with no score: ingest asked and the API declined."""
    candidate = candidate_post_from_hit({
        "_source": {
            "at_uri": "at://post/1",
            "content": "日本語",
            "perspective_scored_at": "2026-08-28T00:00:00Z",
        }
    })
    assert candidate.combined_perspective_score is None
    assert candidate.perspective_scored_at == "2026-08-28T00:00:00Z"

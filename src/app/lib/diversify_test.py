"""Tests for MMR-based feed diversification."""

import math

import pytest

from ..models import CandidatePost
from .diversify import AUTHOR_WEIGHT, BETA, DECAY_TAU, _cosine_similarity, mmr_rerank
from .embeddings import encode_float32_b64
from .feed_debug import FeedDebugRecorder, feed_debug_scope


def _post(uri: str, score: float, author_did: str | None = None) -> CandidatePost:
    return CandidatePost(at_uri=uri, score=score, author_did=author_did)


def _post_with_embed(uri: str, score: float, author_did: str, vec: list[float]) -> CandidatePost:
    return CandidatePost(
        at_uri=uri,
        score=score,
        author_did=author_did,
        minilm_l12_embedding=encode_float32_b64(vec),
    )


def test_empty_input_returns_empty():
    assert mmr_rerank([]) == []


def test_single_candidate_unchanged():
    c = _post("at://x/1", score=1.0, author_did="did:plc:alice")
    result = mmr_rerank([c])
    assert len(result) == 1
    assert result[0].at_uri == c.at_uri
    assert result[0].score == c.score
    assert result[0].diversity_score == 1.0


def test_same_author_posts_spread_apart():
    """b1 (lower score, different author) should precede a2 (same author as a1)."""
    a1 = _post("at://alice/1", score=1.0, author_did="did:plc:alice")
    a2 = _post("at://alice/2", score=0.9, author_did="did:plc:alice")
    a3 = _post("at://alice/3", score=0.8, author_did="did:plc:alice")
    b1 = _post("at://bob/1", score=0.5, author_did="did:plc:bob")

    result = mmr_rerank([a1, a2, a3, b1])
    uris = [c.at_uri for c in result]

    assert uris[0] == "at://alice/1"
    assert uris.index("at://bob/1") < uris.index("at://alice/2")


def test_author_penalty_decays_after_intervening_selection():
    """A repeated author should be penalized less after one intervening pick."""
    a1 = _post("at://alice/1", score=1.0, author_did="did:plc:alice")
    a2 = _post("at://alice/2", score=1.0, author_did="did:plc:alice")
    b1 = _post("at://bob/1", score=0.5, author_did="did:plc:bob")

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        result = mmr_rerank([a1, a2, b1])
    uris = [c.at_uri for c in result]

    assert uris == ["at://alice/1", "at://bob/1", "at://alice/2"]
    _, rel, score, author_pen, content_pen = rec.diversification[2]
    expected_author_penalty = BETA * AUTHOR_WEIGHT * math.exp(-1 / DECAY_TAU)
    assert rel == pytest.approx(1.0)
    assert author_pen == pytest.approx(expected_author_penalty)
    assert content_pen == pytest.approx(0.0)
    assert score == pytest.approx((1 - BETA) * 1.0 - expected_author_penalty)


def test_missing_author_dids_do_not_count_as_same_author():
    """Unknown authors should not be treated as matching each other."""
    p1 = _post("at://unknown/1", score=1.0, author_did=None)
    p2 = _post("at://unknown/2", score=0.9, author_did=None)
    b1 = _post("at://bob/1", score=0.5, author_did="did:plc:bob")

    result = mmr_rerank([p1, p2, b1])
    uris = [c.at_uri for c in result]

    assert uris == ["at://unknown/1", "at://unknown/2", "at://bob/1"]


def test_all_different_authors_order_preserved_by_relevance():
    """With no author overlap, MMR reduces to relevance order."""
    posts = [
        _post("at://a/1", score=0.9, author_did="did:plc:a"),
        _post("at://b/1", score=0.7, author_did="did:plc:b"),
        _post("at://c/1", score=0.5, author_did="did:plc:c"),
        _post("at://d/1", score=0.3, author_did="did:plc:d"),
    ]

    result = mmr_rerank(posts)
    uris = [c.at_uri for c in result]
    assert uris == ["at://a/1", "at://b/1", "at://c/1", "at://d/1"]


def test_mixed_positive_and_negative_scores_ranked_by_relevance():
    """Scores crossing zero should still rank highest-to-lowest with distinct authors."""
    posts = [
        _post("at://a/1", score=0.5, author_did="did:plc:a"),
        _post("at://b/1", score=0.0, author_did="did:plc:b"),
        _post("at://c/1", score=-0.5, author_did="did:plc:c"),
    ]
    result = mmr_rerank(posts)
    assert [c.at_uri for c in result] == ["at://a/1", "at://b/1", "at://c/1"]


def test_all_negative_scores_ranked_by_relevance():
    """All-negative scores should still rank highest-to-lowest with distinct authors."""
    posts = [
        _post("at://a/1", score=-0.1, author_did="did:plc:a"),
        _post("at://b/1", score=-0.5, author_did="did:plc:b"),
        _post("at://c/1", score=-1.0, author_did="did:plc:c"),
    ]
    result = mmr_rerank(posts)
    assert [c.at_uri for c in result] == ["at://a/1", "at://b/1", "at://c/1"]


def test_equal_scores_diversity_drives_selection():
    """When all scores are equal, author diversity should determine ordering."""
    a1 = _post("at://alice/1", score=1.0, author_did="did:plc:alice")
    a2 = _post("at://alice/2", score=1.0, author_did="did:plc:alice")
    b1 = _post("at://bob/1", score=1.0, author_did="did:plc:bob")

    result = mmr_rerank([a1, a2, b1])
    uris = [c.at_uri for c in result]
    assert uris.index("at://bob/1") < uris.index("at://alice/2")


# ---------------------------------------------------------------------------
# _cosine_similarity unit tests
# ---------------------------------------------------------------------------

def test_cosine_identical_vectors():
    assert _cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)


def test_cosine_orthogonal_vectors():
    assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_opposite_vectors():
    assert _cosine_similarity([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)


def test_cosine_zero_vector_a_returns_zero():
    assert _cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0


def test_cosine_zero_vector_b_returns_zero():
    assert _cosine_similarity([1.0, 0.0], [0.0, 0.0]) == 0.0


# ---------------------------------------------------------------------------
# mmr_rerank with cosine similarity active
# ---------------------------------------------------------------------------

def test_cosine_penalizes_topically_similar_cross_author_post():
    """A post from a different author but with an identical embedding is penalized
    by the cosine term, causing a topically-distinct post to rank ahead of it."""
    p1 = _post_with_embed("at://alice/1", score=1.0, author_did="did:plc:alice", vec=[1.0, 0.0])
    p2 = _post_with_embed("at://bob/1", score=0.9, author_did="did:plc:bob", vec=[1.0, 0.0])
    p3 = _post_with_embed("at://carol/1", score=0.8, author_did="did:plc:carol", vec=[0.0, 1.0])

    result = mmr_rerank([p1, p2, p3])
    uris = [c.at_uri for c in result]

    assert uris[0] == "at://alice/1"
    # p2 shares p1's topic; p3 is orthogonal — cosine pushes p3 ahead of p2
    assert uris.index("at://carol/1") < uris.index("at://bob/1")


def test_content_penalty_decays_after_intervening_selection():
    """A matching older post should contribute less after one intervening pick."""
    p1 = _post_with_embed("at://topic/1", score=1.0, author_did="did:plc:a", vec=[1.0, 0.0])
    p2 = _post_with_embed("at://topic/2", score=1.0, author_did="did:plc:b", vec=[1.0, 0.0])
    p3 = _post_with_embed("at://other/1", score=0.5, author_did="did:plc:c", vec=[0.0, 1.0])

    rec = FeedDebugRecorder(feed_name="f", regenerated=False)
    with feed_debug_scope(rec):
        result = mmr_rerank([p1, p2, p3])
    uris = [c.at_uri for c in result]

    assert uris == ["at://topic/1", "at://other/1", "at://topic/2"]
    _, rel, score, author_pen, content_pen = rec.diversification[2]
    expected_content_penalty = BETA * (1 - AUTHOR_WEIGHT) * math.exp(-1 / DECAY_TAU)
    assert rel == pytest.approx(1.0)
    assert author_pen == pytest.approx(0.0)
    assert content_pen == pytest.approx(expected_content_penalty)
    assert score == pytest.approx((1 - BETA) * 1.0 - expected_content_penalty)


def test_cosine_similarity_value_matches_manual_calculation():
    """Verify a non-trivial cosine value against manual calculation."""
    vec_a = [3.0, 4.0]
    vec_b = [4.0, 3.0]
    # cosine([3,4],[4,3]) = (12+12)/(5*5) = 24/25
    expected_cosine = 24 / 25
    assert _cosine_similarity(vec_a, vec_b) == pytest.approx(expected_cosine, rel=1e-5)


# ---------------------------------------------------------------------------
# Diversity score stamping tests
# ---------------------------------------------------------------------------

class TestMmrRerankDiversityScore:
    def test_single_candidate_gets_score_1(self):
        posts = [_post("at://a/1", 1.0, "did:plc:a")]
        result = mmr_rerank(posts)
        assert len(result) == 1
        assert result[0].diversity_score == 1.0

    def test_first_pick_always_scores_1(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://b/1", 0.5, "did:plc:b"),
        ]
        result = mmr_rerank(posts)
        assert result[0].diversity_score == 1.0

    def test_same_author_reduces_diversity(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://a/2", 0.9, "did:plc:a"),  # same author — high similarity penalty
            _post("at://b/1", 0.5, "did:plc:b"),
        ]
        result = mmr_rerank(posts)
        # The second pick from the same author should have a lower diversity score
        # than a post from a different author.
        scores_by_uri = {c.at_uri: c.diversity_score for c in result}
        assert scores_by_uri["at://a/1"] == 1.0
        same_author_score = scores_by_uri["at://a/2"]
        diff_author_score = scores_by_uri["at://b/1"]
        assert same_author_score is not None
        assert diff_author_score is not None
        assert same_author_score < diff_author_score

    def test_all_scores_in_unit_range(self):
        posts = [_post(f"at://a/{i}", float(i), "did:plc:a") for i in range(5)]
        result = mmr_rerank(posts)
        for c in result:
            assert c.diversity_score is not None
            assert 0.0 <= c.diversity_score <= 1.0

    def test_scores_parallel_to_output_order(self):
        posts = [
            _post("at://a/1", 1.0, "did:plc:a"),
            _post("at://b/1", 0.8, "did:plc:b"),
            _post("at://c/1", 0.6, "did:plc:c"),
        ]
        result = mmr_rerank(posts)
        assert all(c.diversity_score is not None for c in result)
        assert len(result) == 3

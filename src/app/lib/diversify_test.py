"""Tests for MMR-based feed diversification."""

import pytest

from ..models import CandidatePost
from .diversify import AUTHOR_WEIGHT, _cosine_similarity, _raw_similarity, _similarity, mmr_rerank
from .embeddings import encode_float32_b64


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
    assert result == [c]


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
# _similarity with embeddings (AUTHOR_WEIGHT < 1.0 path)
# ---------------------------------------------------------------------------

def test_similarity_different_authors_identical_embeddings():
    """Cosine=1.0 for identical vecs; no author match → similarity = (1-AUTHOR_WEIGHT)."""
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", [1.0, 0.0])
    b = _post_with_embed("at://b/1", 0.9, "did:plc:bob", [1.0, 0.0])
    assert _similarity(a, b) == pytest.approx((1 - AUTHOR_WEIGHT) * 1.0)


def test_similarity_different_authors_orthogonal_embeddings():
    """Cosine=0 for orthogonal vecs; no author match → similarity = 0."""
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", [1.0, 0.0])
    b = _post_with_embed("at://b/1", 0.9, "did:plc:bob", [0.0, 1.0])
    assert _similarity(a, b) == pytest.approx(0.0)


def test_similarity_same_author_with_embeddings():
    """Same author contributes AUTHOR_WEIGHT; cosine contributes (1-AUTHOR_WEIGHT)."""
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", [1.0, 0.0])
    b = _post_with_embed("at://a/2", 0.9, "did:plc:alice", [1.0, 0.0])
    expected = AUTHOR_WEIGHT * 1.0 + (1 - AUTHOR_WEIGHT) * 1.0
    assert _similarity(a, b) == pytest.approx(expected)


def test_similarity_missing_one_embedding_skips_cosine():
    """If one post has no embedding, cosine is 0 and only author_match counts."""
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", [1.0, 0.0])
    b = CandidatePost(at_uri="at://b/1", score=0.9, author_did="did:plc:bob")
    assert _similarity(a, b) == pytest.approx(0.0)


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


def test_cosine_similarity_value_matches_manual_calculation():
    """Verify the combined similarity formula: AUTHOR_WEIGHT*author + (1-AUTHOR_WEIGHT)*cosine."""
    vec_a = [3.0, 4.0]
    vec_b = [4.0, 3.0]
    # cosine([3,4],[4,3]) = (12+12)/(5*5) = 24/25
    expected_cosine = 24 / 25
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", vec_a)
    b = _post_with_embed("at://b/1", 0.9, "did:plc:bob", vec_b)
    expected = (1 - AUTHOR_WEIGHT) * expected_cosine
    assert _similarity(a, b) == pytest.approx(expected, rel=1e-5)


# ---------------------------------------------------------------------------
# _raw_similarity — same logic as _similarity but accepts pre-decoded vectors
# ---------------------------------------------------------------------------

def test_raw_similarity_matches_similarity_different_authors_identical_embeddings():
    vec = [1.0, 0.0]
    result = _raw_similarity("did:plc:alice", "did:plc:bob", vec, vec)
    assert result == pytest.approx((1 - AUTHOR_WEIGHT) * 1.0)


def test_raw_similarity_matches_similarity_orthogonal_embeddings():
    result = _raw_similarity("did:plc:alice", "did:plc:bob", [1.0, 0.0], [0.0, 1.0])
    assert result == pytest.approx(0.0)


def test_raw_similarity_same_author_full_score():
    vec = [1.0, 0.0]
    result = _raw_similarity("did:plc:alice", "did:plc:alice", vec, vec)
    expected = AUTHOR_WEIGHT * 1.0 + (1 - AUTHOR_WEIGHT) * 1.0
    assert result == pytest.approx(expected)


def test_raw_similarity_none_vec_skips_cosine():
    result = _raw_similarity("did:plc:alice", "did:plc:bob", None, [1.0, 0.0])
    assert result == pytest.approx(0.0)


def test_raw_similarity_none_author_no_match():
    vec = [1.0, 0.0]
    result = _raw_similarity(None, None, vec, vec)
    # author_did=None on both — treated as no match; cosine still applies
    assert result == pytest.approx((1 - AUTHOR_WEIGHT) * 1.0)


def test_raw_similarity_agrees_with_similarity_for_same_inputs():
    """_raw_similarity and _similarity must return the same value for equivalent inputs."""
    vec_a = [3.0, 4.0]
    vec_b = [4.0, 3.0]
    a = _post_with_embed("at://a/1", 1.0, "did:plc:alice", vec_a)
    b = _post_with_embed("at://b/1", 0.9, "did:plc:bob", vec_b)
    a_emb = a.minilm_l12_embedding
    b_emb = b.minilm_l12_embedding
    assert a_emb is not None and b_emb is not None
    from .embeddings import decode_float32_b64
    assert _raw_similarity(
        a.author_did, b.author_did,
        decode_float32_b64(a_emb),
        decode_float32_b64(b_emb),
    ) == pytest.approx(_similarity(a, b), rel=1e-6)

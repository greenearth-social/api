"""MMR-based feed diversification."""

import math

import numpy as np

from ..models import CandidatePost
from .embeddings import decode_float32_b64
from .feed_debug import current_recorder

# User-facing defaults preserve the former fixed 0.35/0.35 penalty split.
DEFAULT_AUTHOR_PENALTY = 0.7
DEFAULT_TOPIC_PENALTY = 0.7
MAX_PENALTY_COEFFICIENT = 0.5

# Backwards-compatible names used by debug tooling and tests for the default
# configuration. Runtime requests derive these values from the two settings.
BETA = 0.7
AUTHOR_WEIGHT = 0.5

# Decay tau for position-based decay of author and content penalties
DECAY_TAU = 15.0


def mmr_weights(author_penalty: float, topic_penalty: float) -> tuple[float, float, float]:
    """Return relevance, author, and topic coefficients for two 0..1 settings."""
    if not 0.0 <= author_penalty <= 1.0 or not 0.0 <= topic_penalty <= 1.0:
        raise ValueError("author_penalty and topic_penalty must be between 0 and 1")
    author_weight = MAX_PENALTY_COEFFICIENT * author_penalty
    topic_weight = MAX_PENALTY_COEFFICIENT * topic_penalty
    return 1.0 - author_weight - topic_weight, author_weight, topic_weight


def mmr_rerank(
    candidates: list[CandidatePost],
    *,
    author_penalty: float = DEFAULT_AUTHOR_PENALTY,
    topic_penalty: float = DEFAULT_TOPIC_PENALTY,
) -> list[tuple[CandidatePost, float]]:
    """Rerank candidates by MMR, returning (candidate, penalized_pick_score) pairs.

    Pairs are in selection order. The pick score is the penalized MMR score the
    candidate was selected on: ``lambda * norm_score - penalties``. Relevance is
    normalized per slate, so pick scores are comparable within one call only.
    Successive pick scores are not guaranteed non-increasing (penalties decay
    with position).
    """
    author_penalty_setting = author_penalty
    topic_penalty_setting = topic_penalty
    relevance_weight, author_penalty_weight, topic_penalty_weight = mmr_weights(
        author_penalty, topic_penalty
    )

    if len(candidates) <= 1:
        # A lone candidate normalizes to relevance 1.0 and carries no penalties.
        return [(c, relevance_weight) for c in candidates]

    n = len(candidates)
    raw_scores = [c.score or 0.0 for c in candidates]
    shift = min(0.0, min(raw_scores))
    shifted_scores = [s - shift for s in raw_scores]
    shifted_max = max(shifted_scores)
    norm_scores = [s / shifted_max for s in shifted_scores] if shifted_max > 0.0 else [1.0] * n

    # Precompute every pairwise content similarity in optimized native code.
    # MMR compares each pair at most once, but doing those 384-d dot products
    # in Python still dominates the entire feed pipeline for a full slate.
    vecs: list[list[float] | None] = []
    for c in candidates:
        if c.minilm_l12_embedding is not None:
            try:
                vecs.append(decode_float32_b64(c.minilm_l12_embedding))
            except Exception:
                vecs.append(None)
        else:
            vecs.append(None)
    content_sims = _pairwise_cosine_similarities(vecs)

    author_dids = [c.author_did for c in candidates]
    remaining = list(range(n))
    selected: list[int] = []
    pick_scores: list[float] = []

    # tracks the highest decayed (content) similarity candidate i has to
    # any selected candidate so far. Updated incrementally — one new comparison per
    # remaining item each round instead of recomputing the full max from scratch.
    decayed_max_content_sims = [-math.inf] * n
    # for each remaining post, keep track of the decayed number of times that post's author
    # has already been selected in the result set
    decayed_same_author_counts = [0] * n

    rec = current_recorder()
    # (at_uri, relevance, score, author_penalty, content_penalty, similarity_score)
    # per pick, for the algorithm-agnostic diversification debug record.
    diag: list[tuple[str, float, float, float, float, float]] | None = (
        [] if rec is not None else None
    )

    def _calculate_author_penalty(i: int) -> float:
        return author_penalty_weight * decayed_same_author_counts[i]

    def _calculate_content_penalty(i: int) -> float:
        return topic_penalty_weight * decayed_max_content_sims[i]

    def _calculate_penalized_score(i: int) -> float:
        total_penalty = _calculate_author_penalty(i) + _calculate_content_penalty(i)
        return relevance_weight * norm_scores[i] - total_penalty

    # We incrementally decay the counts and similarities after each selection
    single_decay_factor = math.exp(-1 / DECAY_TAU)

    while remaining:
        if not selected:
            best = max(remaining, key=lambda i: relevance_weight * norm_scores[i])
            pick_score = relevance_weight * norm_scores[best]
            author_penalty = 0.0
            content_penalty = 0.0
            # Nothing selected yet, so there is nothing to be similar to.
            similarity_score = 0.0
        else:
            best = max(remaining, key=_calculate_penalized_score)
            pick_score = _calculate_penalized_score(best)
            author_penalty = _calculate_author_penalty(best)
            content_penalty = _calculate_content_penalty(best)
            # A setting-independent combined similarity, recorded raw. Higher
            # means more similar to what is already selected — less diverse. Deliberately
            # not clamped to 0..1: repeated authors can push it above 1, and
            # keeping that visible is what distinguishes a really homogenous
            # slate from a merely repetitive one.
            # Keep this observability metric independent of user settings so
            # page-level similarity remains comparable across users and time.
            similarity_score = 0.5 * (
                decayed_same_author_counts[best] + decayed_max_content_sims[best]
            )

        if diag is not None:
            diag.append(
                (
                    candidates[best].at_uri or "",
                    norm_scores[best],
                    pick_score,
                    author_penalty,
                    content_penalty,
                    similarity_score,
                )
            )

        pick_scores.append(pick_score)
        selected.append(best)
        remaining.remove(best)

        # position-decay same author counts and content similarities:
        decayed_same_author_counts = [c * single_decay_factor for c in decayed_same_author_counts]
        decayed_max_content_sims = [s * single_decay_factor for s in decayed_max_content_sims]

        for i in remaining:
            if author_dids[i] is not None and author_dids[best] is not None:
                if author_dids[i] == author_dids[best]:
                    decayed_same_author_counts[i] += 1
            content_sim = float(content_sims[i, best])
            if content_sim > decayed_max_content_sims[i]:
                decayed_max_content_sims[i] = content_sim

    if rec is not None and diag is not None:
        rec.record_diversification(
            diag,
            author_penalty_setting=author_penalty_setting,
            topic_penalty_setting=topic_penalty_setting,
            relevance_weight=relevance_weight,
            author_penalty_weight=author_penalty_weight,
            topic_penalty_weight=topic_penalty_weight,
        )

    return [(candidates[i], score) for i, score in zip(selected, pick_scores, strict=True)]


def _pairwise_cosine_similarities(vecs: list[list[float] | None]) -> np.ndarray:
    """Return a dense cosine-similarity matrix for decoded embeddings.

    Missing, invalid, and zero-length embeddings have zero similarity, matching
    original ``_calculate_content_sim`` fn. Production MiniLM embeddings are uniformly
    384-dimensional.
    """
    n = len(vecs)
    similarities = np.zeros((n, n), dtype=np.float64)
    valid = [(i, vec) for i, vec in enumerate(vecs) if vec is not None]
    if not valid:
        return similarities

    indices = np.fromiter((i for i, _ in valid), dtype=np.intp)
    matrix = np.asarray([vec for _, vec in valid], dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1)
    nonzero = norms != 0.0
    normalized = np.zeros_like(matrix)
    normalized[nonzero] = matrix[nonzero] / norms[nonzero, np.newaxis]
    similarities[np.ix_(indices, indices)] = normalized @ normalized.T
    return similarities


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)

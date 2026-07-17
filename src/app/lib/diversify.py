"""MMR-based feed diversification."""

import math

from .embeddings import decode_float32_b64
from .feed_debug import current_recorder
from ..models import CandidatePost

# Global diversity weight (relevance weight is 1-BETA)
BETA = 0.7

# Author weight (content weight is 1-AUTHOR_WEIGHT)
AUTHOR_WEIGHT = 0.5

# Decay tau for position-based decay of author and content penalties
DECAY_TAU = 15.0


def mmr_rerank(candidates: list[CandidatePost]) -> list[tuple[CandidatePost, float]]:
    """Rerank candidates by MMR, returning (candidate, penalized_pick_score) pairs.

    Pairs are in selection order. The pick score is the penalized MMR score the
    candidate was selected on: ``(1-BETA) * norm_score - penalties``. Relevance is
    normalized per slate, so pick scores are comparable within one call only.
    Successive pick scores are not guaranteed non-increasing (penalties decay
    with position).
    """
    if len(candidates) <= 1:
        # A lone candidate normalizes to relevance 1.0 and carries no penalties.
        return [(c, 1 - BETA) for c in candidates]

    n = len(candidates)
    raw_scores = [c.score or 0.0 for c in candidates]
    shift = min(0.0, min(raw_scores))
    shifted_scores = [s - shift for s in raw_scores]
    shifted_max = max(shifted_scores)
    norm_scores = [s / shifted_max for s in shifted_scores] if shifted_max > 0.0 else [1.0] * n

    # Pre-decode embeddings once so the inner loop never repeats base64 work.
    vecs: list[list[float] | None] = []
    for c in candidates:
        if c.minilm_l12_embedding is not None:
            try:
                vecs.append(decode_float32_b64(c.minilm_l12_embedding))
            except Exception:
                vecs.append(None)
        else:
            vecs.append(None)

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
    # (at_uri, relevance, score, author_penalty, content_penalty) per pick, for
    # the algorithm-agnostic diversification debug record.
    diag: list[tuple[str, float, float, float, float]] | None = [] if rec is not None else None

    def _calculate_author_penalty(i: int) -> float:
        return BETA * AUTHOR_WEIGHT * decayed_same_author_counts[i]

    def _calculate_content_penalty(i: int) -> float:
        return BETA * (1 - AUTHOR_WEIGHT) * decayed_max_content_sims[i]

    def _calculate_penalized_score(i: int) -> float:
        total_penalty = _calculate_author_penalty(i) + _calculate_content_penalty(i)
        return (1 - BETA) * norm_scores[i] - total_penalty

    # We incrementally decay the counts and similarities after each selection
    single_decay_factor = math.exp(-1 / DECAY_TAU)

    while remaining:
        if not selected:
            best = max(remaining, key=lambda i: (1 - BETA) * norm_scores[i])
            pick_score = (1 - BETA) * norm_scores[best]
            author_penalty = 0.0
            content_penalty = 0.0
        else:
            best = max(remaining, key=_calculate_penalized_score)
            pick_score = _calculate_penalized_score(best)
            author_penalty = _calculate_author_penalty(best)
            content_penalty = _calculate_content_penalty(best)

        if diag is not None:
            diag.append(
                (
                    candidates[best].at_uri or "",
                    norm_scores[best],
                    pick_score,
                    author_penalty,
                    content_penalty,
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
            content_sim = _calculate_content_sim(vecs[i], vecs[best])
            if content_sim > decayed_max_content_sims[i]:
                decayed_max_content_sims[i] = content_sim

    if rec is not None and diag is not None:
        rec.record_diversification(diag)

    return [(candidates[i], score) for i, score in zip(selected, pick_scores, strict=True)]


def _calculate_content_sim(
    vec_a: list[float] | None,
    vec_b: list[float] | None,
) -> float:
    if AUTHOR_WEIGHT < 1.0 and vec_a is not None and vec_b is not None:
        return _cosine_similarity(vec_a, vec_b)
    else:
        return 0.0


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)

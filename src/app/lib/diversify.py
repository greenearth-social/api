"""MMR-based feed diversification."""

import math

from .embeddings import decode_float32_b64
from .feed_debug import current_recorder
from ..models import CandidatePost

BETA = 0.5
AUTHOR_WEIGHT = 0.75


def mmr_rerank(candidates: list[CandidatePost]) -> list[CandidatePost]:
    if len(candidates) <= 1:
        return list(candidates)

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
    # max_sim[i] tracks the highest similarity candidate i has to any selected
    # candidate so far. Updated incrementally — one new comparison per remaining
    # item each round instead of recomputing the full max from scratch.
    max_sim = [-math.inf] * n
    # Author/content split of the neighbour that currently drives max_sim[i], so
    # the diversification penalty can be attributed when debugging is enabled.
    max_components: list[tuple[float, float]] = [(0.0, 0.0)] * n

    rec = current_recorder()
    # (at_uri, relevance, score, author_penalty, content_penalty) per pick, for
    # the algorithm-agnostic diversification debug record.
    diag: list[tuple[str, float, float, float, float]] | None = [] if rec is not None else None

    while remaining:
        if not selected:
            best = max(remaining, key=lambda i: (1 - BETA) * norm_scores[i])
            if diag is not None:
                diag.append(
                    (
                        candidates[best].at_uri or "",
                        norm_scores[best],
                        (1 - BETA) * norm_scores[best],
                        0.0,
                        0.0,
                    )
                )
        else:
            best = max(
                remaining,
                key=lambda i: (1 - BETA) * norm_scores[i] - BETA * max_sim[i],
            )
            if diag is not None:
                author_match, cosine = max_components[best]
                author_penalty = BETA * AUTHOR_WEIGHT * author_match
                content_penalty = BETA * (1 - AUTHOR_WEIGHT) * cosine
                score = (1 - BETA) * norm_scores[best] - BETA * max_sim[best]
                diag.append(
                    (
                        candidates[best].at_uri or "",
                        norm_scores[best],
                        score,
                        author_penalty,
                        content_penalty,
                    )
                )

        selected.append(best)
        remaining.remove(best)

        for i in remaining:
            s, author_match, cosine = _similarity_components(
                author_dids[i], author_dids[best], vecs[i], vecs[best]
            )
            if s > max_sim[i]:
                max_sim[i] = s
                max_components[i] = (author_match, cosine)

    if rec is not None and diag is not None:
        rec.record_diversification(diag)

    return [candidates[i] for i in selected]


def _similarity_components(
    author_a: str | None,
    author_b: str | None,
    vec_a: list[float] | None,
    vec_b: list[float] | None,
) -> tuple[float, float, float]:
    """Return (combined_similarity, author_match, cosine) from pre-decoded inputs.

    The combined value is what MMR penalises; the author_match/cosine parts let
    the penalty be attributed to author vs. content diversity when debugging.
    """
    author_match = 1.0 if (author_a is not None and author_a == author_b) else 0.0
    if AUTHOR_WEIGHT < 1.0 and vec_a is not None and vec_b is not None:
        cosine = _cosine_similarity(vec_a, vec_b)
    else:
        cosine = 0.0
    combined = AUTHOR_WEIGHT * author_match + (1 - AUTHOR_WEIGHT) * cosine
    return combined, author_match, cosine


def _raw_similarity(
    author_a: str | None,
    author_b: str | None,
    vec_a: list[float] | None,
    vec_b: list[float] | None,
) -> float:
    """Compute similarity from pre-decoded components; no CandidatePost needed."""
    combined, _, _ = _similarity_components(author_a, author_b, vec_a, vec_b)
    return combined


def _similarity(a: CandidatePost, b: CandidatePost) -> float:
    """Public similarity helper used by tests; decodes embeddings on each call."""
    vec_a = None
    vec_b = None
    if a.minilm_l12_embedding is not None:
        try:
            vec_a = decode_float32_b64(a.minilm_l12_embedding)
        except Exception:
            pass
    if b.minilm_l12_embedding is not None:
        try:
            vec_b = decode_float32_b64(b.minilm_l12_embedding)
        except Exception:
            pass
    return _raw_similarity(a.author_did, b.author_did, vec_a, vec_b)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)

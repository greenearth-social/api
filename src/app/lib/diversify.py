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
    # max_content_sim[i] tracks the highest (content) similarity candidate i has to
    # any selected candidate so far. Updated incrementally — one new comparison per
    # remaining item each round instead of recomputing the full max from scratch.
    max_content_sims = [-math.inf] * n
    # for each remaining post, keep track of the number of times that post's author
    # has already been selected in the result set
    same_author_counts = [0] * n

    rec = current_recorder()
    # (at_uri, relevance, score, author_penalty, content_penalty) per pick, for
    # the algorithm-agnostic diversification debug record.
    diag: list[tuple[str, float, float, float, float]] | None = [] if rec is not None else None

    def _calculate_author_penalty(i: int) -> float:
        return BETA * AUTHOR_WEIGHT * same_author_counts[i]

    def _calculate_content_penalty(i: int) -> float:
        return BETA * (1 - AUTHOR_WEIGHT) * max_content_sims[i]

    def _calculate_penalized_score(i: int) -> float:
        total_penalty = _calculate_author_penalty(i) + _calculate_content_penalty(i)
        return (1 - BETA) * norm_scores[i] - total_penalty

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
            best = max(remaining, key=_calculate_penalized_score)
            if diag is not None:
                author_penalty = _calculate_author_penalty(best)
                content_penalty = _calculate_content_penalty(best)
                norm_score = norm_scores[best]
                penalized_score = _calculate_penalized_score(best)
                diag.append(
                    (
                        candidates[best].at_uri or "",
                        norm_score,
                        penalized_score,
                        author_penalty,
                        content_penalty,
                    )
                )

        selected.append(best)
        remaining.remove(best)

        for i in remaining:
            if author_dids[i] is not None and author_dids[best] is not None:
                if author_dids[i] == author_dids[best]:
                    same_author_counts[i] += 1
            content_sim = _calculate_content_sim(vecs[i], vecs[best])
            if content_sim > max_content_sims[i]:
                max_content_sims[i] = content_sim

    if rec is not None and diag is not None:
        rec.record_diversification(diag)

    return [candidates[i] for i in selected]


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

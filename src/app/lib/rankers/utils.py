"""Utils for rankers"""

from ...models import RankedCandidate, RankPredictResult


def get_rank_predict_results_from_candidates_and_scores(
    candidate_posts,
    scores,
    valid_candidates
) -> RankPredictResult:
    # Rank by the final scores, breaking ties by original order in candidates list
    candidates_with_scores = zip(candidate_posts, scores)
    ranked_candidates = sorted(
        enumerate(candidates_with_scores), # (index, (candidate, score))
        key=lambda item: (
            -(item[1][1] if item[1][1] is not None else float("-inf")),
            item[0],
        ),
    )

    # Get in correct output format
    rankings: list[RankedCandidate] = []
    for rank_idx, (_, (candidate, score)) in enumerate(ranked_candidates, start=1):
        assert candidate.at_uri is not None
        rankings.append(
            RankedCandidate(
                at_uri=candidate.at_uri,
                rank=rank_idx,
                rank_score=score,
            )
        )

    ranked_uris = {ranking.at_uri for ranking in rankings}
    for candidate in valid_candidates:
        if candidate.at_uri is None or candidate.at_uri in ranked_uris:
            continue
        rankings.append(
            RankedCandidate(
                at_uri=candidate.at_uri,
                rank=len(rankings) + 1,
                rank_score=None,
            )
        )
    return RankPredictResult(rankings=rankings)
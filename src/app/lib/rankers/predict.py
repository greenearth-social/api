"""Shared ranking pipeline.

Given a `RankPredictRequest`, runs each configured rank model in parallel,
normalizes each model's raw scores into [0, 1] using its theoretical
`score_bounds`, and combines them into a single ordering via a weighted
average using each model's configured relative weight.
"""

import asyncio
import logging
import statistics

from ...models import RankedCandidate, RankPredictRequest, RankPredictResult
from ..feed_debug import current_recorder
from ..telemetry import timed
from .base import Ranker, RankerError, RankerExecutionError, RankerResult, get_ranker
from ..metrics import get_metric_collector

logger = logging.getLogger(__name__)

class RankModelNotFoundError(Exception):
    """Raised when a requested rank model does not exist."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Rank model not found: {name}")


def _normalize(
    raw: float | None,
    bounds: tuple[float, float],
    fallback_normalized_score: float,
) -> float:
    """Linearly map *raw* from *bounds* into [0, 1], clamping the result.

    Missing scores (``None`` — e.g. a ranker couldn't score a candidate)
    normalize to ``fallback_normalized_score``. Degenerate bounds
    (``hi <= lo``) also normalize to ``fallback_normalized_score`` to avoid
    division by zero.
    """
    if raw is None:
        return fallback_normalized_score
    lo, hi = bounds
    if hi <= lo:
        return fallback_normalized_score
    normalized = (raw - lo) / (hi - lo)
    return max(0.0, min(1.0, normalized))


async def _run_one(
    es,
    user_did: str,
    request: RankPredictRequest,
    name: str,
    ranker: Ranker,
) -> RankerResult:
    try:
        async with timed(
            logger,
            "rank.model.duration_ms",
            record_metric=True,
            metric_attrs={"model_name": name},
            n_candidates=len(request.candidates),
        ):
            return await ranker.predict(es, user_did, request.candidates)
    except RankerError:
        raise
    except RankerExecutionError:
        raise
    except Exception as exc:
        logger.exception("Ranker '%s' failed", name)
        raise RankerExecutionError(name, str(exc) or exc.__class__.__name__) from exc


async def run_predict(
    request: RankPredictRequest,
    es,
) -> RankPredictResult:
    """Rank the supplied candidates by combining the requested rank models.

    Each model runs in parallel; its raw `rank_score`s are normalized into
    [0, 1] using its `score_bounds`, then combined into a single score per
    candidate via a weighted average (weights normalized to sum to 1, so the
    combined score also stays within [0, 1]). The final ordering is by
    combined score, descending, with ties broken by original candidate order.
    If a candidate at_uri has no valid scores from any model, it is dropped.
    If it has a score from at least one model but not the others, the median
    (per-model results) is filled in for those missing scores.
    """
    if not request.candidates:
        raise RankerError("candidates list must not be empty")

    if any(candidate.at_uri is None for candidate in request.candidates):
        raise RankerError("All candidates must include at_uri")

    resolved: list[tuple[str, float, Ranker]] = []
    for spec in request.models:
        ranker = get_ranker(spec.name)
        if ranker is None:
            raise RankModelNotFoundError(spec.name)
        resolved.append((spec.name, spec.weight, ranker))

    results = await asyncio.gather(
        *(
            _run_one(es, request.user_did, request, name, ranker)
            for name, _weight, ranker in resolved
        )
    )

    rec = current_recorder()

    # loop through results once to calculate medians and get scores per candidate
    medians_by_model: dict[str, float] = {}  # {model_name: median_score}
    results_by_candidate: dict[str, dict[str, float]] = {}  # {uri: {model_name: score}}
    models_with_valid_results: list[tuple[str, float, Ranker]] = [] # filtered version of resolved
    for (name, weight, ranker), result in zip(resolved, results):
        # all the valid uris with their scores from this ranker model
        raw_by_uri = {
            r.at_uri: r.rank_score
            for r in result.result.rankings
            if r.at_uri is not None and r.rank_score is not None
        }
        if raw_by_uri:
            bounds = ranker.score_bounds
            raw_median = statistics.median(raw_by_uri.values())
            medians_by_model[name] = _normalize(raw_median, bounds, 0.5)
            models_with_valid_results.append((name, weight, ranker))
        for uri, score in raw_by_uri.items():
            if uri not in results_by_candidate:
                results_by_candidate[uri] = {name: score}
            else:
                results_by_candidate[uri][name] = score

    # loop through again to normalize scores and drop candidates with no valid scores in any model
    candidate_uris = [c.at_uri for c in request.candidates if c.at_uri]
    valid_candidate_uris = []
    normalized_by_model: dict[str, dict[str, float]] = {}  # {model_name: {uri: normalized_score}}
    dropped_candidate_count = 0
    for uri in candidate_uris:
        # if the model has no valid results in any of the rank models, exclude it
        if uri not in results_by_candidate:
            dropped_candidate_count += 1
            continue
        valid_candidate_uris.append(uri)
        for model_name, _, ranker in models_with_valid_results:
            bounds = ranker.score_bounds
            score = None
            if model_name in results_by_candidate[uri]:
                score = results_by_candidate[uri][model_name]
            normalized_score = _normalize(score, bounds, medians_by_model[model_name])
            if model_name not in normalized_by_model:
                normalized_by_model[model_name] = {uri: normalized_score}
            else:
                normalized_by_model[model_name][uri] = normalized_score

    metric_collector = get_metric_collector()
    if metric_collector and dropped_candidate_count > 0:
        metric_collector.record(
            "rank.predict.dropped_candidates_count",
            dropped_candidate_count,
        )

    if rec is not None:
        for model_name, weight, _ in resolved:
            rec.record_model_scores(
                model_name,
                weight,
                normalized_by_model.get(model_name, {}),
            )

    total_weight = sum(weight for _name, weight, _ranker in models_with_valid_results)

    def _combined(at_uri: str) -> float:
        return sum(
            (weight / total_weight) * normalized_by_model[name].get(at_uri, 0.0)
            for name, weight, _ in models_with_valid_results
        )

    candidates_with_scores_initial_order = [
        (initial_idx, uri, _combined(uri))
        for initial_idx, uri in enumerate(valid_candidate_uris)
    ]
    ranked = enumerate(sorted(
        candidates_with_scores_initial_order,
        key=lambda item: (-item[2], item[0]),
    ), start=1)

    rankings: list[RankedCandidate] = []
    for rank_idx, (_, at_uri, score) in ranked:
        rankings.append(
            RankedCandidate(
                at_uri=at_uri,
                rank=rank_idx,
                rank_score=score,
            )
        )

    return RankPredictResult(rankings=rankings)

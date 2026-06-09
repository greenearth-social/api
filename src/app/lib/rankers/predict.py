"""Shared ranking pipeline.

Given a `RankPredictRequest`, runs each configured rank model in parallel,
normalizes each model's raw scores into [-1, 1] using its theoretical
`score_bounds`, and combines them into a single ordering via a weighted
average using each model's configured relative weight.
"""

import asyncio
import logging

from ...models import RankedCandidate, RankPredictRequest, RankPredictResult
from ..feed_debug import current_recorder
from ..telemetry import timed
from .base import Ranker, RankerError, RankerExecutionError, RankerResult, get_ranker

logger = logging.getLogger(__name__)


class RankModelNotFoundError(Exception):
    """Raised when a requested rank model does not exist."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Rank model not found: {name}")


def _normalize(raw: float | None, bounds: tuple[float, float]) -> float:
    """Linearly map *raw* from *bounds* into [-1, 1], clamping the result.

    Missing scores (``None`` — e.g. a ranker couldn't score a candidate)
    normalize to ``0.0`` (neutral), matching how individual rankers already
    treat unscoreable candidates. Degenerate bounds (``hi <= lo``) also
    normalize to ``0.0`` to avoid division by zero.
    """
    if raw is None:
        return 0.0
    lo, hi = bounds
    if hi <= lo:
        return 0.0
    normalized = 2.0 * (raw - lo) / (hi - lo) - 1.0
    return max(-1.0, min(1.0, normalized))


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
    [-1, 1] using its `score_bounds`, then combined into a single score per
    candidate via a weighted average (weights normalized to sum to 1, so the
    combined score also stays within [-1, 1]). The final ordering is by
    combined score, descending, with ties broken by original candidate order.
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

    normalized_by_model: dict[str, dict[str, float]] = {}
    for (name, weight, ranker), result in zip(resolved, results):
        bounds = ranker.score_bounds
        raw_by_uri = {
            ranking.at_uri: ranking.rank_score
            for ranking in result.result.rankings
            if ranking.at_uri
        }
        normalized = {uri: _normalize(score, bounds) for uri, score in raw_by_uri.items()}
        normalized_by_model[name] = normalized
        if rec is not None:
            rec.record_model_scores(name, weight, normalized)

    total_weight = sum(weight for _name, weight, _ranker in resolved)
    valid_candidates = [(c, c.at_uri) for c in request.candidates if c.at_uri is not None]

    def _combined(at_uri: str) -> float:
        return sum(
            (weight / total_weight) * normalized_by_model[name].get(at_uri, 0.0)
            for name, weight, _ranker in resolved
        )

    ranked = sorted(
        enumerate(valid_candidates),
        key=lambda item: (-_combined(item[1][1]), item[0]),
    )

    rankings: list[RankedCandidate] = []
    for rank_idx, (_, (_candidate, at_uri)) in enumerate(ranked, start=1):
        rankings.append(
            RankedCandidate(
                at_uri=at_uri,
                rank=rank_idx,
                rank_score=_combined(at_uri),
            )
        )

    return RankPredictResult(rankings=rankings)

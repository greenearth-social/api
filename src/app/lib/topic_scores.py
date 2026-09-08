"""Extraction and validation helpers for post topic-inference scores."""

import math
from collections.abc import Mapping

POLITICS_KEY = "News & Social Concern"


def politics_score_from_source(source: object) -> float | None:
    """Return a valid politics topic score from an Elasticsearch ``_source``.

    Topic inference scores are probabilities in the inclusive range [0, 1].
    Invalid or malformed values are treated as missing so bad index data cannot
    distort ranking or break response serialization.
    """
    if not isinstance(source, Mapping):
        return None

    topic_scores = source.get("topic_scores")
    if not isinstance(topic_scores, Mapping):
        return None

    raw_score = topic_scores.get(POLITICS_KEY)
    if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
        return None

    score = float(raw_score)
    if not math.isfinite(score) or not 0.0 <= score <= 1.0:
        return None
    return score

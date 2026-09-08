import math

import pytest

from .topic_scores import POLITICS_KEY, politics_score_from_source


@pytest.mark.parametrize("score", [0, 0.0, 0.5, 1, 1.0])
def test_politics_score_from_source_accepts_finite_values_in_range(score):
    assert politics_score_from_source({"topic_scores": {POLITICS_KEY: score}}) == float(score)


@pytest.mark.parametrize(
    "score",
    [
        True,
        False,
        -0.01,
        1.01,
        math.nan,
        math.inf,
        -math.inf,
        "0.5",
        None,
    ],
)
def test_politics_score_from_source_rejects_invalid_values(score):
    assert politics_score_from_source({"topic_scores": {POLITICS_KEY: score}}) is None


@pytest.mark.parametrize(
    "source",
    [None, [], {}, {"topic_scores": None}, {"topic_scores": []}],
)
def test_politics_score_from_source_rejects_malformed_sources(source):
    assert politics_score_from_source(source) is None

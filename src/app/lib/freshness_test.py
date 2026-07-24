import pytest

from .freshness import (
    DEFAULT_MAX_AGE_HOURS,
    FRESHNESS_HOURS_BY_INDEX,
    max_age_hours_for_freshness,
)


@pytest.mark.parametrize(
    ("index", "hours"),
    sorted(FRESHNESS_HOURS_BY_INDEX.items()),
)
def test_maps_freshness_index_to_hours(index, hours):
    assert max_age_hours_for_freshness(index) == hours


def test_unknown_index_falls_back_to_seven_days():
    assert max_age_hours_for_freshness(99) == DEFAULT_MAX_AGE_HOURS == 168

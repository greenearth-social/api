"""Shared freshness presets for feed preferences and candidate generation."""

from ..models import MaxAgeHours

DEFAULT_FRESHNESS_INDEX = 5
DEFAULT_MAX_AGE_HOURS: MaxAgeHours = 168

FRESHNESS_HOURS_BY_INDEX: dict[int, MaxAgeHours] = {
    0: 6,
    1: 12,
    2: 24,
    3: 48,
    4: 72,
    5: 168,
}


def max_age_hours_for_freshness(freshness: int) -> MaxAgeHours:
    """Resolve a stored freshness index, falling back to the seven-day default."""
    return FRESHNESS_HOURS_BY_INDEX.get(freshness, DEFAULT_MAX_AGE_HOURS)

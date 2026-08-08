"""Resolve configured, feed-scoped user controls."""

from __future__ import annotations

from typing import Literal, overload

from ..documents import (
    FeedPreferencesDocument,
    SourceWeightsDocument,
    UserDocument,
)
from ..feeds import FEEDS, SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES
from ..models import FeedControlName

DEFAULT_SOURCE_WEIGHTS = SourceWeightsDocument(
    following=0.3,
    network_likes=0.2,
    authors_topics=0.25,
    popular=0.25,
)

CONTROL_DEFAULTS: dict[FeedControlName, int | float | SourceWeightsDocument] = {
    "source_weights": DEFAULT_SOURCE_WEIGHTS,
    "social_radius": 3,
    "freshness": 5,
    "politics": 1.0,
    "purpose": 0.5,
}


def source_weights_for_social_radius(social_radius: int) -> SourceWeightsDocument:
    """Translate a legacy Social Radius preset into the atomic source shape."""
    generators = SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES.get(
        social_radius,
        SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES[3],
    )
    by_name = {generator.name: generator.weight for generator in generators}
    return SourceWeightsDocument(
        following=by_name.get("followed_users", 0.0),
        network_likes=by_name.get("network_likes", 0.0),
        authors_topics=by_name.get("two_tower", 0.0),
        popular=by_name.get("popularity", 0.0),
    )


def preference_source(feed_name: str) -> str:
    """Return the canonical storage key for a feed's preferences."""
    feed = FEEDS[feed_name]
    return feed.preference_source or feed_name


def configured_controls(feed_name: str) -> tuple[FeedControlName, ...]:
    """Return controls configured for a feed, following any preference alias."""
    return FEEDS[preference_source(feed_name)].controls


def resolve_feed_preferences(
    user: UserDocument | None,
    feed_name: str,
) -> FeedPreferencesDocument:
    """Resolve stored values, legacy fallbacks, and defaults for one feed."""
    source = preference_source(feed_name)
    stored = user.feed_preferences.get(source) if user is not None else None
    values: dict[str, int | float | SourceWeightsDocument] = {}

    for control in configured_controls(feed_name):
        if control == "source_weights":
            if stored is not None and stored.source_weights is not None:
                values[control] = stored.source_weights
            elif stored is not None and stored.social_radius is not None:
                values[control] = source_weights_for_social_radius(stored.social_radius)
            elif user is not None:
                values[control] = source_weights_for_social_radius(user.social_radius)
            else:
                values[control] = DEFAULT_SOURCE_WEIGHTS
            continue
        stored_value = getattr(stored, control, None) if stored is not None else None
        if stored_value is not None:
            values[control] = stored_value
        elif user is not None:
            # Legacy flat preferences initialize every applicable feed until
            # that feed's resolved values are materialized by its first patch.
            values[control] = getattr(user, control)
        else:
            values[control] = CONTROL_DEFAULTS[control]

    return FeedPreferencesDocument.model_validate(values)


@overload
def control_value(
    user: UserDocument | None,
    feed_name: str,
    control: Literal["source_weights"],
) -> SourceWeightsDocument: ...


@overload
def control_value(
    user: UserDocument | None,
    feed_name: str,
    control: Literal["social_radius", "freshness"],
) -> int: ...


@overload
def control_value(
    user: UserDocument | None,
    feed_name: str,
    control: Literal["politics", "purpose"],
) -> float: ...


def control_value(
    user: UserDocument | None,
    feed_name: str,
    control: FeedControlName,
) -> int | float | SourceWeightsDocument:
    """Resolve one value, returning its default when the feed does not expose it."""
    if control not in configured_controls(feed_name):
        return CONTROL_DEFAULTS[control]
    value = getattr(resolve_feed_preferences(user, feed_name), control)
    assert value is not None
    return value

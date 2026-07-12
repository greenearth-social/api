"""PostHog analytics client wrapper.

When ``GE_POSTHOG_API_KEY`` is not set the global client is ``None`` and
all calls are silent no-ops — callers never need to guard against a missing
client.

PostHog events emitted:
  feed_loaded      — one per getFeedSkeleton call (drives DAU/MAU/session counts)
  <interaction>    — behavioural events forwarded from sendInteractions
                     e.g. interactionLike, clickthroughItem, requestMore
"""

from __future__ import annotations

from datetime import datetime

from posthog import Posthog

_posthog_client: Posthog | None = None


def set_posthog_client(client: Posthog | None) -> None:
    global _posthog_client
    _posthog_client = client


def get_posthog_client() -> Posthog | None:
    return _posthog_client


def init_posthog_client(api_key: str, host: str) -> Posthog:
    return Posthog(project_api_key=api_key, host=host)


def track_session(
    client: Posthog | None,
    user_did: str,
    username: str,
    feed_name: str,
    timestamp: datetime,
) -> None:
    """Capture a feed_loaded event and update the user's person properties."""
    if client is None:
        return
    client.capture(
        distinct_id=user_did,
        event="feed_loaded",
        properties={
            "feed_name": feed_name,
            "$set": {"username": username},
        },
        timestamp=timestamp,
    )


def track_interaction(
    client: Posthog | None,
    user_did: str,
    event: str,
    feed_name: str,
    item_uri: str | None,
    timestamp: datetime,
) -> None:
    """Capture a Bluesky interaction event."""
    if client is None:
        return
    properties: dict = {"feed_name": feed_name}
    if item_uri:
        properties["item_uri"] = item_uri
    client.capture(
        distinct_id=user_did,
        event=event,
        properties=properties,
        timestamp=timestamp,
    )

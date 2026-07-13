"""PostHog analytics client wrapper.

When ``GE_POSTHOG_API_KEY`` is not set the global client is ``None`` and
all calls are silent no-ops — callers never need to guard against a missing
client.

Event names follow camelCase, matching the Bluesky interaction event names
(e.g. ``interactionLike``, ``clickthroughItem``) forwarded from sendInteractions.

Every event carries an explicit ``$session_id`` -- PostHog does not infer
sessions from event timestamps, so without one all of a user's events
(including backfilled history) collapse into a single session. Callers pass
the originating feed request's id (the feed-cache key / feedContext ``rid``)
so a feedLoaded event and the interactions it produced group together.

PostHog events emitted:
  feedLoaded       — one per getFeedSkeleton call (drives DAU/MAU/session counts)
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
    session_id: str,
) -> None:
    """Capture a feedLoaded event and update the user's person properties.

    ``session_id`` should be the feed request's id (feed-cache key), so the
    interactions this feed load produces can be grouped into the same
    PostHog session via ``track_interaction``.
    """
    if client is None:
        return
    client.capture(
        distinct_id=user_did,
        event="feedLoaded",
        properties={
            "feed_name": feed_name,
            "$session_id": session_id,
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
    session_id: str,
) -> None:
    """Capture a Bluesky interaction event.

    ``event`` should already be camelCase (e.g. ``interactionLike``) per the
    module-level event naming convention -- callers pass through the event
    name as-is, no case conversion happens here. ``session_id`` should be the
    originating feed request's id (feedContext ``rid``), matching the
    ``session_id`` passed to ``track_session`` for that feed load.
    """
    if client is None:
        return
    properties: dict = {"feed_name": feed_name, "$session_id": session_id}
    if item_uri:
        properties["item_uri"] = item_uri
    client.capture(
        distinct_id=user_did,
        event=event,
        properties=properties,
        timestamp=timestamp,
    )

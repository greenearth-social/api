"""PostHog analytics client wrapper.

When ``GE_POSTHOG_API_KEY`` is not set the global client is ``None`` and
all calls are silent no-ops — callers never need to guard against a missing
client.

Event names follow camelCase, matching the Bluesky interaction event names
(e.g. ``interactionLike``, ``clickthroughItem``) forwarded from sendInteractions.

PostHog events emitted:
  feedLoaded       — one per getFeedSkeleton call (drives DAU/MAU/session counts)
  <interaction>    — behavioural events forwarded from sendInteractions
                     e.g. interactionLike, clickthroughItem, requestMore
"""

from __future__ import annotations

import logging
from datetime import datetime

from posthog import Posthog

logger = logging.getLogger(__name__)

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
    username: str | None,
    feed_name: str,
    timestamp: datetime,
) -> None:
    """Capture a feedLoaded event and update the user's person properties.

    ``username`` may be ``None`` when the handle couldn't be resolved. The
    event is still captured — it's keyed on the DID — but the person property
    is left alone rather than set to null, so a transient resolution failure
    doesn't erase a handle PostHog already has.
    """
    if client is None:
        return
    properties: dict[str, object] = {"feed_name": feed_name}
    if username is not None:
        properties["$set"] = {"username": username}
    client.capture(
        distinct_id=user_did,
        event="feedLoaded",
        properties=properties,
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
    """Capture a Bluesky interaction event.

    ``event`` should already be camelCase (e.g. ``interactionLike``) per the
    module-level event naming convention -- callers pass through the event
    name as-is, no case conversion happens here.
    """
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


def evaluate_fail_fast_flag(client: Posthog | None, user_did: str) -> bool:
    """Evaluate the fail-fast-feed PostHog feature flag for this user.

    Returns True only when the client is present and the flag is enabled for
    user_did. Soft-fails to False on any SDK exception so a PostHog outage
    never breaks feed serving.
    """
    if client is None:
        return False
    try:
        return bool(client.feature_enabled("fail-fast-feed", user_did))
    except Exception:
        logger.warning("PostHog feature flag evaluation failed for %s", user_did)
        return False

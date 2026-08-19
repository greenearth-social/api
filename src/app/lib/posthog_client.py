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
  redirectClicked  — UTM click counting. Keyed on the ``redirect_service``
                     pseudo-user rather than a real DID: the click happens
                     before we know who it was.

Every event carries two annotations, applied by :func:`annotate_event_properties`:

  surface         — which producer emitted the event. The frontend writes to the
                    same PostHog project and stamps ``greenearth_web``; this
                    service stamps ``greenearth_api``. Filter on it whenever an
                    insight should cover one producer rather than both.
  schema_version  — version of *this surface's* event schema. It is scoped to
                    the surface, so it is only meaningful alongside it — the
                    frontend versions its own schema independently. Bump it when
                    an existing event's properties change shape in a way that
                    would break a saved insight.
"""

from __future__ import annotations

import logging
from datetime import datetime

from posthog import Posthog

logger = logging.getLogger(__name__)

_posthog_client: Posthog | None = None

FAIL_FAST_FLAG = "fail-fast-feed"
NETWORK_LIKES_FLAG = "network-likes-in-your-feed"
EXPANDED_CANDIDATE_BATCH_FLAG = "expanded-candidate-batch"

EVENT_SURFACE = "greenearth_api"
EVENT_SCHEMA_VERSION = 1


def annotate_event_properties(properties: dict) -> dict:
    """Return *properties* stamped with the surface and schema version.

    The annotations are applied last so a caller-supplied property can never
    overwrite them — ``surface`` partitions this service's events from the
    frontend's inside a shared PostHog project, and an event that could quietly
    reassign itself to another producer would corrupt every insight built on it.
    """
    return {
        **properties,
        "surface": EVENT_SURFACE,
        "schema_version": EVENT_SCHEMA_VERSION,
    }


def user_identity_properties(username: str | None) -> dict:
    """Return the properties that carry the user's handle on an event.

    ``distinct_id`` stays the DID — it is Bluesky's stable identifier and
    survives a rename, so keying on the handle would fork a person every time
    they change it. The handle is instead what a human reads: ``$set`` populates
    the PostHog person display name (``$set``, not ``$set_once``, so a rename
    propagates), and ``user_handle`` mirrors it onto the event so an insight can
    break down by handle without joining to the person.

    ``username`` may be ``None`` when the handle couldn't be resolved, in which
    case nothing is returned — writing a null would erase a handle PostHog
    already has over what is usually a transient directory failure.
    """
    if username is None:
        return {}
    return {"$set": {"username": username}, "user_handle": username}


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
    properties: dict[str, object] = {
        "feed_name": feed_name,
        **user_identity_properties(username),
    }
    client.capture(
        distinct_id=user_did,
        event="feedLoaded",
        properties=annotate_event_properties(properties),
        timestamp=timestamp,
    )


def track_interaction(
    client: Posthog | None,
    user_did: str,
    event: str,
    feed_name: str,
    item_uri: str | None,
    timestamp: datetime,
    username: str | None = None,
) -> None:
    """Capture a Bluesky interaction event.

    ``event`` should already be camelCase (e.g. ``interactionLike``) per the
    module-level event naming convention -- callers pass through the event
    name as-is, no case conversion happens here.

    ``username`` is the caller's handle, carried so interaction events identify
    the user the same way ``feedLoaded`` does. It is optional and best-effort:
    an unresolved handle costs the identity properties, never the event.
    """
    if client is None:
        return
    properties: dict = {"feed_name": feed_name}
    if item_uri:
        properties["item_uri"] = item_uri
    properties.update(user_identity_properties(username))
    client.capture(
        distinct_id=user_did,
        event=event,
        properties=annotate_event_properties(properties),
        timestamp=timestamp,
    )


def track_redirect(
    client: Posthog | None,
    slug: str,
    to: str,
    utm_params: dict[str, str],
) -> None:
    """Capture a redirectClicked event for UTM click counting."""
    if client is None:
        return
    client.capture(
        distinct_id="redirect_service",
        event="redirectClicked",
        properties=annotate_event_properties({"slug": slug, "to": to, **utm_params}),
    )


def evaluate_feature_flags(
    client: Posthog | None,
    user_did: str,
    flag_keys: list[str],
) -> dict[str, bool]:
    """Evaluate the requested flags in one PostHog request, defaulting to False."""
    values = {key: False for key in flag_keys}
    if client is None:
        return values
    try:
        # The batch API does not emit $feature_flag_called exposure events.
        evaluated = client.get_all_flags(
            user_did,
            flag_keys_to_evaluate=flag_keys,
        ) or {}
        return {key: bool(evaluated.get(key, False)) for key in flag_keys}
    except Exception:
        logger.warning("PostHog feature flag evaluation failed for %s", user_did)
        return values

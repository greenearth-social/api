"""Tests for the PostHog client module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.lib.posthog_client import (
    EVENT_SCHEMA_VERSION,
    EVENT_SURFACE,
    annotate_event_properties,
    get_posthog_client,
    init_posthog_client,
    set_posthog_client,
    track_interaction,
    track_session,
)

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
USER_DID = "did:plc:abc123"
ANNOTATIONS = {"surface": EVENT_SURFACE, "schema_version": EVENT_SCHEMA_VERSION}


@pytest.fixture(autouse=True)
def reset_client():
    """Ensure the global client is None before and after each test."""
    set_posthog_client(None)
    yield
    set_posthog_client(None)


def test_get_and_set_client():
    mock = MagicMock()
    set_posthog_client(mock)
    assert get_posthog_client() is mock


def test_set_client_to_none():
    set_posthog_client(MagicMock())
    set_posthog_client(None)
    assert get_posthog_client() is None


def test_init_posthog_client_creates_posthog():
    with patch("app.lib.posthog_client.Posthog") as MockPosthog:
        MockPosthog.return_value = MagicMock()
        client = init_posthog_client("phc_key", "https://us.i.posthog.com")
        MockPosthog.assert_called_once_with(
            project_api_key="phc_key", host="https://us.i.posthog.com"
        )
        assert client is MockPosthog.return_value


def test_track_session_none_client_is_noop():
    track_session(None, USER_DID, "alice.bsky.app", "your-feed", NOW)


def test_track_session_captures_feed_loaded():
    mock = MagicMock()
    track_session(mock, USER_DID, "alice.bsky.app", "your-feed", NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="feedLoaded",
        properties={
            "feed_name": "your-feed",
            "$set": {"username": "alice.bsky.app"},
            **ANNOTATIONS,
        },
        timestamp=NOW,
    )


def test_track_session_without_a_handle_still_captures_the_event():
    # The event is keyed on the DID, so an unresolved handle shouldn't cost us
    # the analytics signal...
    mock = MagicMock()
    track_session(mock, USER_DID, None, "your-feed", NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="feedLoaded",
        properties={"feed_name": "your-feed", **ANNOTATIONS},
        timestamp=NOW,
    )


def test_track_session_without_a_handle_leaves_the_person_property_alone():
    # ...and must not null out a username PostHog already knows.
    mock = MagicMock()
    track_session(mock, USER_DID, None, "your-feed", NOW)
    properties = mock.capture.call_args.kwargs["properties"]
    assert "$set" not in properties


def test_track_interaction_none_client_is_noop():
    track_interaction(None, USER_DID, "interactionLike", "your-feed", "at://did/post/1", NOW)


def test_track_interaction_captures_event_with_uri():
    mock = MagicMock()
    track_interaction(mock, USER_DID, "interactionLike", "your-feed", "at://did/post/1", NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="interactionLike",
        properties={
            "feed_name": "your-feed",
            "item_uri": "at://did/post/1",
            **ANNOTATIONS,
        },
        timestamp=NOW,
    )


def test_track_interaction_captures_event_without_uri():
    mock = MagicMock()
    track_interaction(mock, USER_DID, "requestMore", "your-feed", None, NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="requestMore",
        properties={"feed_name": "your-feed", **ANNOTATIONS},
        timestamp=NOW,
    )


# ---------------------------------------------------------------------------
# Event annotations (surface / schema_version)
# ---------------------------------------------------------------------------


def test_surface_identifies_this_service():
    # The frontend stamps "greenearth_web" (see frontend PostHogAnalyticsService);
    # both producers share one PostHog project, so the values must not collide.
    assert EVENT_SURFACE == "greenearth_api"


def test_annotate_adds_surface_and_schema_version():
    assert annotate_event_properties({"feed_name": "your-feed"}) == {
        "feed_name": "your-feed",
        "surface": "greenearth_api",
        "schema_version": EVENT_SCHEMA_VERSION,
    }


def test_annotate_does_not_mutate_the_callers_dict():
    properties = {"feed_name": "your-feed"}
    annotate_event_properties(properties)
    assert properties == {"feed_name": "your-feed"}


def test_annotations_win_over_caller_supplied_properties():
    # The partition key must be un-overwritable: an event property named
    # "surface" can't be allowed to silently move events to another producer.
    annotated = annotate_event_properties({"surface": "not_the_api", "schema_version": 99})
    assert annotated["surface"] == "greenearth_api"
    assert annotated["schema_version"] == EVENT_SCHEMA_VERSION


def test_every_captured_event_carries_the_annotations():
    mock = MagicMock()
    track_session(mock, USER_DID, "alice.bsky.app", "your-feed", NOW)
    track_session(mock, USER_DID, None, "your-feed", NOW)
    track_interaction(mock, USER_DID, "interactionLike", "your-feed", "at://did/post/1", NOW)
    track_interaction(mock, USER_DID, "requestMore", "your-feed", None, NOW)

    assert mock.capture.call_count == 4
    for call in mock.capture.call_args_list:
        properties = call.kwargs["properties"]
        assert properties["surface"] == "greenearth_api"
        assert properties["schema_version"] == EVENT_SCHEMA_VERSION


def test_real_posthog_client_is_disabled_in_tests():
    """The global conftest fixture must force every real Posthog client to
    be disabled, so a stray GE_POSTHOG_API_KEY in a developer's environment
    can never cause a test run to send live analytics events."""
    client = init_posthog_client("phc_key", "https://us.i.posthog.com")
    assert client.disabled is True


from app.lib.posthog_client import evaluate_fail_fast_flag


def test_evaluate_fail_fast_flag_none_client_returns_false():
    assert evaluate_fail_fast_flag(None, "did:plc:abc123") is False


def test_evaluate_fail_fast_flag_enabled_returns_true():
    mock = MagicMock()
    mock.feature_enabled.return_value = True
    result = evaluate_fail_fast_flag(mock, "did:plc:abc123")
    assert result is True
    mock.feature_enabled.assert_called_once_with("fail-fast-feed", "did:plc:abc123")


def test_evaluate_fail_fast_flag_disabled_returns_false():
    mock = MagicMock()
    mock.feature_enabled.return_value = False
    assert evaluate_fail_fast_flag(mock, "did:plc:abc123") is False


def test_evaluate_fail_fast_flag_sdk_exception_returns_false():
    mock = MagicMock()
    mock.feature_enabled.side_effect = RuntimeError("network error")
    assert evaluate_fail_fast_flag(mock, "did:plc:abc123") is False


def test_evaluate_fail_fast_flag_sdk_returns_none_returns_false():
    mock = MagicMock()
    mock.feature_enabled.return_value = None
    assert evaluate_fail_fast_flag(mock, "did:plc:abc123") is False

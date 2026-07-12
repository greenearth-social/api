"""Tests for the PostHog client module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.lib.posthog_client import (
    get_posthog_client,
    init_posthog_client,
    set_posthog_client,
    track_interaction,
    track_session,
)

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
USER_DID = "did:plc:abc123"


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
        event="feed_loaded",
        properties={
            "feed_name": "your-feed",
            "$set": {"username": "alice.bsky.app"},
        },
        timestamp=NOW,
    )


def test_track_interaction_none_client_is_noop():
    track_interaction(None, USER_DID, "interactionLike", "your-feed", "at://did/post/1", NOW)


def test_track_interaction_captures_event_with_uri():
    mock = MagicMock()
    track_interaction(mock, USER_DID, "interactionLike", "your-feed", "at://did/post/1", NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="interactionLike",
        properties={"feed_name": "your-feed", "item_uri": "at://did/post/1"},
        timestamp=NOW,
    )


def test_track_interaction_captures_event_without_uri():
    mock = MagicMock()
    track_interaction(mock, USER_DID, "requestMore", "your-feed", None, NOW)
    mock.capture.assert_called_once_with(
        distinct_id=USER_DID,
        event="requestMore",
        properties={"feed_name": "your-feed"},
        timestamp=NOW,
    )


def test_real_posthog_client_is_disabled_in_tests():
    """The global conftest fixture must force every real Posthog client to
    be disabled, so a stray GE_POSTHOG_API_KEY in a developer's environment
    can never cause a test run to send live analytics events."""
    client = init_posthog_client("phc_key", "https://us.i.posthog.com")
    assert client.disabled is True

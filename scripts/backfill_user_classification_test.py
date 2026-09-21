"""Tests for the one-time user-classification backfill."""

from datetime import UTC, datetime

from backfill_user_classification import UserPostSeenHistory, add_interaction


def test_add_interaction_aggregates_distinct_utc_days_and_latest_time():
    histories: dict[str, UserPostSeenHistory] = {}
    first = datetime(2026, 9, 19, 23, 30, tzinfo=UTC)
    latest = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)

    assert add_interaction(
        histories,
        {
            "event": "interactionSeen",
            "user_did": "did:plc:user",
            "item_uri": "at://post/1",
            "created_at": first,
            "load_test": False,
        },
    )
    assert add_interaction(
        histories,
        {
            "event": "interactionSeen",
            "user_did": "did:plc:user",
            "item_uri": "at://post/2",
            "created_at": latest,
            "load_test": False,
        },
    )

    assert histories["did:plc:user"].days == {first.date(), latest.date()}
    assert histories["did:plc:user"].last_seen_at == latest


def test_add_interaction_rejects_load_tests_and_malformed_events():
    histories: dict[str, UserPostSeenHistory] = {}
    base = {
        "event": "interactionSeen",
        "user_did": "did:plc:user",
        "item_uri": "at://post/1",
        "created_at": datetime(2026, 9, 20, tzinfo=UTC),
    }

    assert not add_interaction(histories, {**base, "load_test": True})
    assert not add_interaction(histories, {**base, "event": "interactionLike"})
    assert not add_interaction(histories, {**base, "item_uri": None})
    assert histories == {}

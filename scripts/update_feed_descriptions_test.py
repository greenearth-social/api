"""Tests for the one-time feed-description migration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from update_feed_descriptions import (
    LEGACY_ATTRIBUTIONS,
    NEW_ATTRIBUTION,
    _password_from_secret,
    _target_rkeys,
    replace_legacy_attribution,
    update_feed_descriptions,
)

PDS = "https://pds.example.com"
HANDLE = "greenearth.social"
PASSWORD = "password"
REPO_DID = "did:plc:publisher"
ACCESS_JWT = "jwt"


def _record(rkey: str, description: str, **extra) -> dict:
    return {
        "uri": f"at://{REPO_DID}/app.bsky.feed.generator/{rkey}",
        "value": {
            "$type": "app.bsky.feed.generator",
            "did": "did:web:api.greenearth.social",
            "displayName": "Existing Name",
            "description": description,
            "createdAt": "2026-01-01T00:00:00Z",
            **extra,
        },
    }


def test_replaces_only_legacy_attribution_in_place():
    description = f"Keep this exact text.\n{LEGACY_ATTRIBUTIONS[0]}"

    migrated, outcome = replace_legacy_attribution(description)

    assert outcome == "updated"
    assert migrated == f"Keep this exact text.\n{NEW_ATTRIBUTION}"


def test_recognizes_idempotent_second_run():
    description = f"Keep this exact text.\n{NEW_ATTRIBUTION}"

    migrated, outcome = replace_legacy_attribution(description)

    assert outcome == "current"
    assert migrated == description


def test_does_not_append_when_legacy_text_is_absent():
    description = "A custom description with no attribution."

    migrated, outcome = replace_legacy_attribution(description)

    assert outcome == "not_found"
    assert migrated == description


def test_prod_and_stage_target_only_public_feed_records():
    assert _target_rkeys("prod") == {"your-feed", "best-of-friends", "random"}
    assert _target_rkeys("stage") == {"a0-yf", "fd-bof", "67-r"}


@patch("update_feed_descriptions._put_record")
@patch("update_feed_descriptions._list_records")
@patch("update_feed_descriptions._create_session")
@patch("update_feed_descriptions.httpx.Client")
def test_updates_existing_records_without_rebuilding_them(
    MockClient, mock_session, mock_list, mock_put
):
    client = MagicMock()
    MockClient.return_value.__enter__ = MagicMock(return_value=client)
    MockClient.return_value.__exit__ = MagicMock(return_value=False)
    mock_session.return_value = {"did": REPO_DID, "accessJwt": ACCESS_JWT}
    records = [
        _record(rkey, f"Unique copy for {rkey}.\n{LEGACY_ATTRIBUTIONS[0]}")
        for rkey in sorted(_target_rkeys("prod"))
    ]
    mock_list.return_value = records

    summary = update_feed_descriptions(
        handle=HANDLE,
        password=PASSWORD,
        environment="prod",
        pds=PDS,
    )

    assert summary.updated == 3
    assert summary.needs_attention is False
    assert mock_put.call_count == 3
    for call in mock_put.call_args_list:
        record = call.args[-1]
        assert record["displayName"] == "Existing Name"
        assert record["createdAt"] == "2026-01-01T00:00:00Z"
        assert LEGACY_ATTRIBUTIONS[0] not in record["description"]
        assert record["description"].endswith(NEW_ATTRIBUTION)


@patch("update_feed_descriptions._put_record")
@patch("update_feed_descriptions._list_records")
@patch("update_feed_descriptions._create_session")
@patch("update_feed_descriptions.httpx.Client")
def test_dry_run_and_unmatched_records_do_not_write(MockClient, mock_session, mock_list, mock_put):
    client = MagicMock()
    MockClient.return_value.__enter__ = MagicMock(return_value=client)
    MockClient.return_value.__exit__ = MagicMock(return_value=False)
    mock_session.return_value = {"did": REPO_DID, "accessJwt": ACCESS_JWT}
    prod_rkeys = sorted(_target_rkeys("prod"))
    mock_list.return_value = [
        _record(prod_rkeys[0], f"Copy.\n{LEGACY_ATTRIBUTIONS[0]}"),
        _record(prod_rkeys[1], "Custom copy."),
        _record(prod_rkeys[2], f"Copy.\n{NEW_ATTRIBUTION}"),
    ]

    summary = update_feed_descriptions(
        handle=HANDLE,
        password=PASSWORD,
        environment="prod",
        pds=PDS,
        dry_run=True,
    )

    assert summary.updated == 1
    assert summary.already_current == 1
    assert summary.no_legacy_text == 1
    assert summary.needs_attention is True
    mock_put.assert_not_called()


@patch("update_feed_descriptions.subprocess.run")
def test_reads_environment_password_from_secret_manager(mock_run):
    mock_run.return_value.stdout = "secret-password\n"

    assert _password_from_secret("project", "secret-name") == "secret-password"
    mock_run.assert_called_once_with(
        [
            "gcloud",
            "secrets",
            "versions",
            "access",
            "latest",
            "--secret=secret-name",
            "--project=project",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

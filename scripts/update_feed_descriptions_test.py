"""Tests for synchronizing environment-specific public feed descriptions."""

from __future__ import annotations

from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest
from update_feed_descriptions import (
    ENVIRONMENT_TARGETS,
    FEEDS,
    UpdateSummary,
    _password_from_secret,
    _target_descriptions,
    main,
    update_feed_descriptions,
)

PDS = "https://pds.example.com"
HANDLE = "did:plc:wrmpulygwvuhjn2c3jbalgqj"
PASSWORD = "password"
REPO_DID = "did:plc:publisher"
ACCESS_JWT = "jwt"
GIT_SHA = "abc1234"
STAGE_DESCRIPTION = f"Built by Caterpie · {GIT_SHA}"
FACETS = [{"index": {"byteStart": 0, "byteEnd": 4}, "features": [{"uri": "https://test.org"}]}]


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


@pytest.fixture
def publisher_mocks():
    with (
        patch("update_feed_descriptions.httpx.Client") as mock_client,
        patch("update_feed_descriptions._create_session") as mock_session,
        patch("update_feed_descriptions._list_records") as mock_list,
        patch("update_feed_descriptions._put_record") as mock_put,
    ):
        client = MagicMock()
        mock_client.return_value.__enter__.return_value = client
        mock_session.return_value = {"did": REPO_DID, "accessJwt": ACCESS_JWT}
        yield client, mock_session, mock_list, mock_put


@pytest.mark.parametrize(
    ("environment", "published_keys"),
    [
        ("prod", {"your-feed", "best-of-friends", "random"}),
        ("stage", {"a0-yf", "fd-bof", "67-r"}),
    ],
)
def test_targets_public_records_with_environment_descriptions(environment, published_keys):
    targets = _target_descriptions(environment, git_sha=GIT_SHA)

    assert targets.keys() == published_keys
    assert targets == {
        rkey if environment == "prod" else config.internal_rkey: (
            config.description if environment == "prod" else STAGE_DESCRIPTION
        )
        for rkey, config in FEEDS.items()
        if config.public
    }


@pytest.mark.parametrize(("environment", "rkey"), [("prod", "your-feed"), ("stage", "a0-yf")])
def test_single_feed_uses_canonical_name_and_environment_rkey(environment, rkey):
    expected = FEEDS["your-feed"].description if environment == "prod" else STAGE_DESCRIPTION
    assert _target_descriptions(environment, "your-feed", GIT_SHA) == {rkey: expected}


@pytest.mark.parametrize(
    ("environment", "feed_name"),
    [("stage", "unknown-feed"), ("prod", "unranked-your-feed"), ("dev", "your-feed")],
)
def test_invalid_target_is_rejected_before_authentication(environment, feed_name, publisher_mocks):
    _, mock_session, _, mock_put = publisher_mocks

    with pytest.raises(ValueError):
        update_feed_descriptions(
            handle=HANDLE, password=PASSWORD, environment=environment, feed_name=feed_name
        )

    mock_session.assert_not_called()
    mock_put.assert_not_called()


def test_environment_authentication_uses_stable_publisher_dids():
    assert ENVIRONMENT_TARGETS["prod"].handle == HANDLE
    assert ENVIRONMENT_TARGETS["stage"].handle == "did:plc:s4tl2ajfsnstzuxtegl7r33g"


@pytest.mark.parametrize("environment", ["prod", "stage"])
def test_updates_exact_copy_and_preserves_other_metadata(environment, publisher_mocks):
    client, mock_session, mock_list, mock_put = publisher_mocks
    targets = _target_descriptions(environment, git_sha=GIT_SHA)
    records = [
        _record(
            rkey,
            (
                "Old copy.\nBuilt by GreenEarth (https://www.greenearth.social)."
                if environment == "prod"
                else FEEDS["your-feed"].description
            ),
            avatar={"ref": {"$link": "avatar-blob"}},
            acceptsInteractions=True,
            descriptionFacets=FACETS,
        )
        for rkey in sorted(targets)
    ]
    records.append(_record("e2-s", "Debug copy that must stay unchanged."))
    original_records = deepcopy(records)
    mock_list.return_value = records

    summary = update_feed_descriptions(
        handle=HANDLE, password=PASSWORD, environment=environment, git_sha=GIT_SHA, pds=PDS
    )

    assert summary == UpdateSummary(updated=3, already_current=0, skipped=0, missing=0)
    assert summary.needs_attention is False
    mock_session.assert_called_once_with(client, PDS, HANDLE, PASSWORD)
    assert mock_put.call_count == 3
    for call in mock_put.call_args_list:
        assert call.args[:4] == (client, PDS, ACCESS_JWT, REPO_DID)
        rkey, record = call.args[-2:]
        expected = next(
            item["value"].copy() for item in records if item["uri"].endswith(f"/{rkey}")
        )
        expected["description"] = targets[rkey]
        expected.pop("descriptionFacets")
        assert record == expected
    assert records == original_records


@pytest.mark.parametrize("environment", ["prod", "stage"])
def test_unchanged_descriptions_and_facets_are_idempotent(environment, publisher_mocks):
    _, _, mock_list, mock_put = publisher_mocks
    records = [
        _record(rkey, description, descriptionFacets=FACETS)
        for rkey, description in _target_descriptions(environment, git_sha=GIT_SHA).items()
    ]
    original_records = deepcopy(records)
    mock_list.return_value = records

    summary = update_feed_descriptions(
        handle=HANDLE, password=PASSWORD, environment=environment, git_sha=GIT_SHA
    )

    assert summary == UpdateSummary(updated=0, already_current=3, skipped=0, missing=0)
    mock_put.assert_not_called()
    assert records == original_records


def test_dry_run_reports_before_and_after_for_only_selected_feed(publisher_mocks, capsys):
    _, _, mock_list, mock_put = publisher_mocks
    mock_list.return_value = [
        _record("a0-yf", "Existing stage copy."),
        _record("67-r", "Leave other public feeds alone."),
    ]

    summary = update_feed_descriptions(
        handle=HANDLE,
        password=PASSWORD,
        environment="stage",
        feed_name="your-feed",
        git_sha=GIT_SHA,
        dry_run=True,
    )

    assert summary == UpdateSummary(updated=1, already_current=0, skipped=0, missing=0)
    mock_put.assert_not_called()
    output = capsys.readouterr().out
    assert "Would update: a0-yf" in output
    assert "Before: 'Existing stage copy.'" in output
    assert f"After: {STAGE_DESCRIPTION!r}" in output
    assert "67-r" not in output


def test_record_without_optional_description_receives_configured_copy(publisher_mocks):
    _, _, mock_list, mock_put = publisher_mocks
    record = _record("your-feed", "")
    del record["value"]["description"]
    mock_list.return_value = [record]

    summary = update_feed_descriptions(
        handle=HANDLE, password=PASSWORD, environment="prod", feed_name="your-feed"
    )

    assert summary.updated == 1
    assert summary.needs_attention is False
    assert mock_put.call_args.args[-1] == {
        **record["value"],
        "description": FEEDS["your-feed"].description,
    }


@pytest.mark.parametrize("value", [None, [], "invalid", {"description": None}, {"description": 7}])
def test_invalid_records_are_skipped_and_reported(value, publisher_mocks, capsys):
    _, _, mock_list, mock_put = publisher_mocks
    record = _record("your-feed", "")
    record["value"] = value
    mock_list.return_value = [record]

    summary = update_feed_descriptions(
        handle=HANDLE, password=PASSWORD, environment="prod", feed_name="your-feed"
    )

    assert summary == UpdateSummary(updated=0, already_current=0, skipped=1, missing=0)
    assert summary.needs_attention is True
    mock_put.assert_not_called()
    assert "your-feed" in capsys.readouterr().err


def test_missing_target_is_reported_without_creating_it(publisher_mocks, capsys):
    _, _, mock_list, mock_put = publisher_mocks
    mock_list.return_value = []

    summary = update_feed_descriptions(
        handle=HANDLE, password=PASSWORD, environment="stage", feed_name="your-feed"
    )

    assert summary == UpdateSummary(updated=0, already_current=0, skipped=0, missing=1)
    assert summary.needs_attention is True
    mock_put.assert_not_called()
    assert "Missing: a0-yf" in capsys.readouterr().err


@pytest.mark.parametrize("environment", ["prod", "stage"])
@patch("update_feed_descriptions.load_dotenv")
@patch("update_feed_descriptions._password_from_secret")
@patch("update_feed_descriptions._git_short_sha", return_value=GIT_SHA)
@patch("update_feed_descriptions.update_feed_descriptions")
def test_cli_uses_environment_account_and_secret(
    mock_update, mock_git_sha, mock_secret, mock_dotenv, environment, monkeypatch
):
    monkeypatch.delenv("GE_BSKY_APP_PASSWORD", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "update_feed_descriptions.py",
            "--environment",
            environment,
            "--project-id",
            "test-project",
            "--feed-name",
            "your-feed",
            "--dry-run",
        ],
    )
    mock_secret.return_value = PASSWORD
    mock_update.return_value = UpdateSummary(updated=1, already_current=0, skipped=0, missing=0)

    main()

    target = ENVIRONMENT_TARGETS[environment]
    mock_secret.assert_called_once_with("test-project", target.secret)
    mock_update.assert_called_once_with(
        handle=target.handle,
        password=PASSWORD,
        environment=environment,
        feed_name="your-feed",
        git_sha=GIT_SHA if environment == "stage" else None,
        pds="https://bsky.social",
        dry_run=True,
    )
    if environment == "stage":
        mock_git_sha.assert_called_once_with()
    else:
        mock_git_sha.assert_not_called()


@patch("update_feed_descriptions.load_dotenv")
@patch("update_feed_descriptions._password_from_secret")
@patch("update_feed_descriptions._git_short_sha")
@patch("update_feed_descriptions.update_feed_descriptions")
def test_cli_explicit_stage_sha_overrides_default(
    mock_update, mock_git_sha, mock_secret, mock_dotenv, monkeypatch
):
    monkeypatch.setattr(
        "sys.argv",
        [
            "update_feed_descriptions.py",
            "--environment",
            "stage",
            "--app-password",
            PASSWORD,
            "--git-sha",
            GIT_SHA,
            "--dry-run",
        ],
    )
    mock_update.return_value = UpdateSummary(updated=3, already_current=0, skipped=0, missing=0)

    main()

    assert mock_update.call_args.kwargs["git_sha"] == GIT_SHA
    mock_git_sha.assert_not_called()
    mock_secret.assert_not_called()


@pytest.mark.parametrize(
    "summary",
    [
        UpdateSummary(updated=0, already_current=0, skipped=1, missing=0),
        UpdateSummary(updated=0, already_current=0, skipped=0, missing=1),
    ],
)
@patch("update_feed_descriptions.load_dotenv")
@patch("update_feed_descriptions.update_feed_descriptions")
def test_cli_returns_attention_exit_code_for_skipped_or_missing(
    mock_update, mock_dotenv, summary, monkeypatch
):
    monkeypatch.setattr(
        "sys.argv",
        ["update_feed_descriptions.py", "--environment", "prod", "--app-password", PASSWORD],
    )
    mock_update.return_value = summary

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2


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

"""Tests for the publish_feed script."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from publish_feed import (
    _create_session,
    _delete_record,
    _list_records,
    _put_record,
    delete_all_feeds,
    delete_feed,
    list_feeds,
    publish_feed,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HANDLE = "alice.bsky.social"
PASSWORD = "test-app-password"
PDS = "https://pds.example.com"
REPO_DID = "did:plc:alice123"
ACCESS_JWT = "fake-jwt-token"
GENERATOR_DID = "did:web:feed.example.com"
FEED_NAME = "greenearth-dev"

SESSION_RESPONSE = {"did": REPO_DID, "accessJwt": ACCESS_JWT}


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> httpx.Response:
    """Create a fake httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = "error body"
    return resp


# ---------------------------------------------------------------------------
# _create_session
# ---------------------------------------------------------------------------


class TestCreateSession:
    def test_success(self):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _mock_response(200, SESSION_RESPONSE)

        result = _create_session(client, PDS, HANDLE, PASSWORD)

        assert result == SESSION_RESPONSE
        client.post.assert_called_once_with(
            f"{PDS}/xrpc/com.atproto.server.createSession",
            json={"identifier": HANDLE, "password": PASSWORD},
        )

    def test_failure_exits(self):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _mock_response(401, {})

        with pytest.raises(SystemExit):
            _create_session(client, PDS, HANDLE, PASSWORD)


# ---------------------------------------------------------------------------
# _put_record
# ---------------------------------------------------------------------------


class TestPutRecord:
    def test_success(self):
        client = MagicMock(spec=httpx.Client)
        put_response = {"uri": f"at://{REPO_DID}/app.bsky.feed.generator/{FEED_NAME}", "cid": "bafyabc"}
        client.post.return_value = _mock_response(200, put_response)

        result = _put_record(client, PDS, ACCESS_JWT, REPO_DID, FEED_NAME, {"test": "record"})

        assert result == put_response
        client.post.assert_called_once_with(
            f"{PDS}/xrpc/com.atproto.repo.putRecord",
            headers={"Authorization": f"Bearer {ACCESS_JWT}"},
            json={
                "repo": REPO_DID,
                "collection": "app.bsky.feed.generator",
                "rkey": FEED_NAME,
                "record": {"test": "record"},
            },
        )

    def test_failure_exits(self):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _mock_response(500, {})

        with pytest.raises(SystemExit):
            _put_record(client, PDS, ACCESS_JWT, REPO_DID, FEED_NAME, {})


# ---------------------------------------------------------------------------
# _delete_record
# ---------------------------------------------------------------------------


class TestDeleteRecord:
    def test_success(self):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _mock_response(200, {})

        _delete_record(client, PDS, ACCESS_JWT, REPO_DID, FEED_NAME)

        client.post.assert_called_once_with(
            f"{PDS}/xrpc/com.atproto.repo.deleteRecord",
            headers={"Authorization": f"Bearer {ACCESS_JWT}"},
            json={
                "repo": REPO_DID,
                "collection": "app.bsky.feed.generator",
                "rkey": FEED_NAME,
            },
        )

    def test_failure_exits(self):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _mock_response(404, {})

        with pytest.raises(SystemExit):
            _delete_record(client, PDS, ACCESS_JWT, REPO_DID, FEED_NAME)


# ---------------------------------------------------------------------------
# _list_records
# ---------------------------------------------------------------------------


class TestListRecords:
    def test_success(self):
        client = MagicMock(spec=httpx.Client)
        records = [
            {"uri": f"at://{REPO_DID}/app.bsky.feed.generator/feed-a", "value": {}},
            {"uri": f"at://{REPO_DID}/app.bsky.feed.generator/feed-b", "value": {}},
        ]
        client.get.return_value = _mock_response(200, {"records": records})

        result = _list_records(client, PDS, ACCESS_JWT, REPO_DID)

        assert result == records
        client.get.assert_called_once_with(
            f"{PDS}/xrpc/com.atproto.repo.listRecords",
            headers={"Authorization": f"Bearer {ACCESS_JWT}"},
            params={
                "repo": REPO_DID,
                "collection": "app.bsky.feed.generator",
            },
        )

    def test_empty_records(self):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _mock_response(200, {"records": []})

        result = _list_records(client, PDS, ACCESS_JWT, REPO_DID)

        assert result == []

    def test_failure_exits(self):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _mock_response(500, {})

        with pytest.raises(SystemExit):
            _list_records(client, PDS, ACCESS_JWT, REPO_DID)


# ---------------------------------------------------------------------------
# publish_feed (high-level)
# ---------------------------------------------------------------------------


class TestPublishFeed:
    @patch("scripts.publish_feed.httpx.Client")
    def test_publishes_known_feed(self, MockClient, capsys):
        """Publishing a feed defined in FEEDS uses its display metadata."""
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.side_effect = [
            _mock_response(200, SESSION_RESPONSE),  # createSession
            _mock_response(200, {"cid": "bafyabc"}),  # putRecord
        ]

        result = publish_feed(
            handle=HANDLE,
            password=PASSWORD,
            feed_name=FEED_NAME,
            generator_did=GENERATOR_DID,
            pds=PDS,
        )

        assert result["cid"] == "bafyabc"

        # Verify putRecord was called with the right record shape
        put_call = client.post.call_args_list[1]
        record = put_call.kwargs["json"]["record"] if "json" in put_call.kwargs else put_call[1]["json"]["record"]
        assert record["$type"] == "app.bsky.feed.generator"
        assert record["did"] == GENERATOR_DID
        assert record["displayName"] == "GE Dev"  # from FEEDS config

        captured = capsys.readouterr()
        assert "Published feed record:" in captured.out

    @patch("scripts.publish_feed.httpx.Client")
    def test_publishes_unknown_feed_uses_name_as_display(self, MockClient, capsys):
        """Publishing a feed not in FEEDS falls back to feed_name."""
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.side_effect = [
            _mock_response(200, SESSION_RESPONSE),
            _mock_response(200, {"cid": "bafyxyz"}),
        ]

        result = publish_feed(
            handle=HANDLE,
            password=PASSWORD,
            feed_name="unknown-feed",
            generator_did=GENERATOR_DID,
            pds=PDS,
        )

        put_call = client.post.call_args_list[1]
        record = put_call.kwargs["json"]["record"] if "json" in put_call.kwargs else put_call[1]["json"]["record"]
        assert record["displayName"] == "unknown-feed"
        assert record["description"] == ""

    @patch("scripts.publish_feed.httpx.Client")
    def test_display_name_override(self, MockClient):
        """Explicit display_name / description override FEEDS metadata."""
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.side_effect = [
            _mock_response(200, SESSION_RESPONSE),
            _mock_response(200, {"cid": "bafyoverride"}),
        ]

        publish_feed(
            handle=HANDLE,
            password=PASSWORD,
            feed_name=FEED_NAME,
            generator_did=GENERATOR_DID,
            display_name="Custom Name",
            description="Custom desc",
            pds=PDS,
        )

        put_call = client.post.call_args_list[1]
        record = put_call.kwargs["json"]["record"] if "json" in put_call.kwargs else put_call[1]["json"]["record"]
        assert record["displayName"] == "Custom Name"
        assert record["description"] == "Custom desc"


# ---------------------------------------------------------------------------
# delete_feed (high-level)
# ---------------------------------------------------------------------------


class TestDeleteFeed:
    @patch("scripts.publish_feed.httpx.Client")
    def test_deletes_single_feed(self, MockClient, capsys):
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.side_effect = [
            _mock_response(200, SESSION_RESPONSE),  # createSession
            _mock_response(200, {}),  # deleteRecord
        ]

        delete_feed(handle=HANDLE, password=PASSWORD, feed_name=FEED_NAME, pds=PDS)

        # Verify deleteRecord was called
        delete_call = client.post.call_args_list[1]
        body = delete_call.kwargs["json"] if "json" in delete_call.kwargs else delete_call[1]["json"]
        assert body["rkey"] == FEED_NAME
        assert body["collection"] == "app.bsky.feed.generator"

        captured = capsys.readouterr()
        assert "Deleted feed record:" in captured.out
        assert FEED_NAME in captured.out


# ---------------------------------------------------------------------------
# delete_all_feeds (high-level)
# ---------------------------------------------------------------------------


class TestDeleteAllFeeds:
    @patch("scripts.publish_feed.httpx.Client")
    def test_deletes_all_listed_feeds(self, MockClient, capsys):
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        records = [
            {"uri": f"at://{REPO_DID}/app.bsky.feed.generator/feed-a", "value": {}},
            {"uri": f"at://{REPO_DID}/app.bsky.feed.generator/feed-b", "value": {}},
        ]
        client.post.side_effect = [
            _mock_response(200, SESSION_RESPONSE),  # createSession
            _mock_response(200, {}),  # deleteRecord feed-a
            _mock_response(200, {}),  # deleteRecord feed-b
        ]
        client.get.return_value = _mock_response(200, {"records": records})

        delete_all_feeds(handle=HANDLE, password=PASSWORD, pds=PDS)

        # Two deleteRecord calls
        delete_calls = [c for c in client.post.call_args_list if "deleteRecord" in str(c)]
        assert len(delete_calls) == 2

        captured = capsys.readouterr()
        assert "Deleted: feed-a" in captured.out
        assert "Deleted: feed-b" in captured.out
        assert "Deleted 2 feed record(s)." in captured.out

    @patch("scripts.publish_feed.httpx.Client")
    def test_no_records_prints_message(self, MockClient, capsys):
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.return_value = _mock_response(200, SESSION_RESPONSE)
        client.get.return_value = _mock_response(200, {"records": []})

        delete_all_feeds(handle=HANDLE, password=PASSWORD, pds=PDS)

        captured = capsys.readouterr()
        assert "No feed records found." in captured.out


# ---------------------------------------------------------------------------
# list_feeds (high-level)
# ---------------------------------------------------------------------------


class TestListFeeds:
    @patch("scripts.publish_feed.httpx.Client")
    def test_lists_feeds_with_details(self, MockClient, capsys):
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        records = [
            {
                "uri": f"at://{REPO_DID}/app.bsky.feed.generator/my-feed",
                "value": {
                    "displayName": "My Feed",
                    "description": "A cool feed",
                    "createdAt": "2026-01-01T00:00:00Z",
                },
            },
            {
                "uri": f"at://{REPO_DID}/app.bsky.feed.generator/other-feed",
                "value": {
                    "displayName": "Other Feed",
                    "createdAt": "2026-02-01T00:00:00Z",
                },
            },
        ]
        client.post.return_value = _mock_response(200, SESSION_RESPONSE)
        client.get.return_value = _mock_response(200, {"records": records})

        list_feeds(handle=HANDLE, password=PASSWORD, pds=PDS)

        captured = capsys.readouterr()
        assert "Found 2 feed record(s)" in captured.out
        assert "my-feed" in captured.out
        assert "Name: My Feed" in captured.out
        assert "Desc: A cool feed" in captured.out
        assert "other-feed" in captured.out
        assert "Name: Other Feed" in captured.out
        # "other-feed" has no description, so "Desc:" should only appear once
        assert captured.out.count("Desc:") == 1

    @patch("scripts.publish_feed.httpx.Client")
    def test_no_feeds_prints_message(self, MockClient, capsys):
        client = MagicMock()
        MockClient.return_value.__enter__ = MagicMock(return_value=client)
        MockClient.return_value.__exit__ = MagicMock(return_value=False)

        client.post.return_value = _mock_response(200, SESSION_RESPONSE)
        client.get.return_value = _mock_response(200, {"records": []})

        list_feeds(handle=HANDLE, password=PASSWORD, pds=PDS)

        captured = capsys.readouterr()
        assert "No feed records found." in captured.out


# ---------------------------------------------------------------------------
# main() CLI argument validation
# ---------------------------------------------------------------------------


class TestMainCLI:
    def test_mutually_exclusive_flags(self, monkeypatch):
        monkeypatch.setenv("GE_BSKY_APP_PASSWORD", PASSWORD)
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["publish_feed.py", "--handle", HANDLE, "--all", "--delete"]):
                from scripts.publish_feed import main
                main()

    def test_delete_requires_feed_name(self, monkeypatch):
        monkeypatch.setenv("GE_BSKY_APP_PASSWORD", PASSWORD)
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["publish_feed.py", "--handle", HANDLE, "--delete"]):
                from scripts.publish_feed import main
                main()

    def test_publish_requires_feed_name_or_all(self, monkeypatch):
        monkeypatch.setenv("GE_BSKY_APP_PASSWORD", PASSWORD)
        monkeypatch.setenv("GE_FEED_GENERATOR_DID", GENERATOR_DID)
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["publish_feed.py", "--handle", HANDLE]):
                from scripts.publish_feed import main
                main()

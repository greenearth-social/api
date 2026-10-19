"""Tests for API key CLI scope arguments, persistence, and display."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import apikeys
import pytest


@pytest.fixture
def firestore_mock():
    db = MagicMock()
    doc_ref = AsyncMock()
    db.collection.return_value.document.return_value = doc_ref
    with patch("apikeys.init_firestore_client", return_value=db) as mock_init:
        yield db, doc_ref, mock_init


def _key_data(key_id="a1b2c3d4", **fields):
    now = datetime(2026, 10, 15, 12, 30, tzinfo=UTC)
    return {
        "key_id": key_id,
        "key_hash": "a" * 64,
        "email": "service@example.com",
        "is_active": True,
        "is_admin": False,
        "created_at": now,
        "last_used_at": now,
        "monthly_period": "2026-10",
        "monthly_call_count": 12,
        **fields,
    }


@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize(
    ("scopes", "expected_scopes"),
    [
        ([], ["default"]),
        (["user-config"], ["default", "user-config"]),
        (["user-config", "reports"], ["default", "user-config", "reports"]),
        (["default", "user-config"], ["default", "user-config"]),
        (["Custom-Scope"], ["default", "Custom-Scope"]),
    ],
)
def test_generate_persists_and_displays_scopes(
    admin, scopes, expected_scopes, firestore_mock, monkeypatch, capsys
):
    db, doc_ref, _ = firestore_mock
    argv = ["apikeys.py", "generate", "service@example.com"]
    if admin:
        argv.append("--admin")
    for scope in scopes:
        argv.extend(["--scope", scope])
    monkeypatch.setattr("sys.argv", argv)

    apikeys.main()

    doc_ref.set.assert_awaited_once()
    stored = doc_ref.set.call_args.args[0]
    assert stored["email"] == "service@example.com"
    assert stored["is_admin"] is admin
    assert stored["scopes"] == expected_scopes
    db.collection.assert_called_once_with("api_keys")
    db.collection.return_value.document.assert_called_once_with(stored["key_id"])
    output = capsys.readouterr().out
    assert f"Key ID : {stored['key_id']}" in output
    assert "Email  : service@example.com" in output
    assert f"Admin  : {admin}" in output
    assert f"Scopes : {', '.join(expected_scopes)}" in output
    key_line = next(line for line in output.splitlines() if line.startswith("Key    : "))
    full_key = key_line.removeprefix("Key    : ")
    assert full_key.startswith(f"gea_{stored['key_id']}")
    assert hashlib.sha256(full_key.encode()).hexdigest() == stored["key_hash"]
    assert full_key not in str(stored)
    assert stored["key_hash"] not in output
    assert "This is the only time the plaintext key will be shown." in output


def test_generate_requires_a_value_for_scope(firestore_mock, monkeypatch, capsys):
    _, _, mock_init = firestore_mock
    monkeypatch.setattr("sys.argv", ["apikeys.py", "generate", "service@example.com", "--scope"])

    with pytest.raises(SystemExit) as exc:
        apikeys.main()

    assert exc.value.code == 2
    assert "--scope: expected one argument" in capsys.readouterr().err
    mock_init.assert_not_called()


def test_list_displays_effective_scopes_for_legacy_and_new_keys(
    firestore_mock, monkeypatch, capsys
):
    db, _, _ = firestore_mock
    records = [
        _key_data("legacy01", is_admin=True),
        _key_data("default1", scopes=["default"]),
        _key_data("scoped01", scopes=["user-config", "reports"]),
    ]

    async def stream():
        for record in records:
            snapshot = MagicMock()
            snapshot.to_dict.return_value = record
            yield snapshot

    db.collection.return_value.stream.return_value = stream()
    monkeypatch.setattr("sys.argv", ["apikeys.py", "list"])

    apikeys.main()

    output = capsys.readouterr().out
    lines = output.splitlines()
    assert lines[0].endswith("scopes")
    for key_id, expected_scopes in [
        ("legacy01", "default"),
        ("default1", "default"),
        ("scoped01", "default, user-config, reports"),
    ]:
        line = next(line for line in lines if line.startswith(key_id))
        assert line.endswith(expected_scopes)
        assert "service@example.com" in line
        assert "2026-10-15T12:30:00Z" in line
    assert "gea_" not in output
    assert records[0]["key_hash"] not in output


@pytest.mark.parametrize("is_admin", [False, True])
@pytest.mark.parametrize(
    ("fields", "expected_scopes"),
    [
        ({}, "default"),
        ({"scopes": ["default"]}, "default"),
        ({"scopes": ["user-config", "reports"]}, "default, user-config, reports"),
    ],
)
def test_usage_displays_effective_scopes(
    is_admin, fields, expected_scopes, firestore_mock, monkeypatch, capsys
):
    _, doc_ref, _ = firestore_mock
    record = _key_data(is_admin=is_admin, **fields)
    snapshot = MagicMock()
    snapshot.exists = True
    snapshot.to_dict.return_value = record
    doc_ref.get.return_value = snapshot
    monkeypatch.setattr("sys.argv", ["apikeys.py", "usage", record["key_id"]])

    apikeys.main()

    output = capsys.readouterr().out
    assert f"key_id             : {record['key_id']}" in output
    assert f"scopes             : {expected_scopes}" in output
    assert "monthly_call_count : 12" in output
    assert "gea_" not in output
    assert record["key_hash"] not in output
    doc_ref.set.assert_not_awaited()
    doc_ref.update.assert_not_awaited()

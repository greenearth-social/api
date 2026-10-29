"""Administrative CLI regression tests; never contact a live service."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

CLI = Path(__file__).with_name("apikeys.py")


def run_cli(*args):
    test_env = dict(
        os.environ,
        GOOGLE_APPLICATION_CREDENTIALS="/nonexistent-api-key-test-credentials",
        K_SERVICE="offline-test",
    )
    return subprocess.run(
        [sys.executable, str(CLI), *args], capture_output=True, text=True, timeout=15, env=test_env
    )


def test_environment_is_required_before_any_application_import():
    result = run_cli("generate", "alice@example.com")
    assert result.returncode == 2
    assert "--environment" in result.stderr
    assert "Traceback" not in result.stderr


def test_help_does_not_import_unrelated_application_modules():
    code = """
import importlib.abc, runpy, sys
class BlockApplication(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname == "app" or fullname.startswith(("google.", "jsonschema", "anthropic")):
            raise RuntimeError("unnecessary import: " + fullname)
sys.meta_path.insert(0, BlockApplication())
sys.argv = [sys.argv[1], "--help"]
runpy.run_path(sys.argv[0], run_name="__main__")
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(CLI)], capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
    assert "--environment" in result.stdout


@pytest.mark.parametrize(
    "variable,value",
    [
        ("GE_FIRESTORE_DATABASE", "(default)"),
        ("GE_FIRESTORE_PROJECT", "wrong-project"),
        ("FIRESTORE_EMULATOR_HOST", "localhost:8080"),
        ("GE_FIRESTORE_EMULATOR_HOST", "localhost:8080"),
    ],
)
def test_conflicting_configuration_is_rejected_before_credentials(monkeypatch, variable, value):
    monkeypatch.setenv(variable, value)
    result = run_cli("--environment", "prod", "generate", "alice@example.com")
    assert result.returncode == 2
    assert variable in result.stderr
    assert "Traceback" not in result.stderr


def test_key_crud_imports_without_candidate_registry():
    code = """
import importlib.abc, sys
sys.path.insert(0, "src")
class BlockApplication(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        blocked = ("app.lib.candidates", "app.models", "app.main", "jsonschema", "anthropic")
        if fullname.startswith(blocked):
            raise RuntimeError("unnecessary import: " + fullname)
sys.meta_path.insert(0, BlockApplication())
from app.lib.api_keys import generate_key
assert len(generate_key()[1]) == 60
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=15,
        cwd=CLI.parent.parent,
    )
    assert result.returncode == 0, result.stderr


def test_retention_prefers_used_key_over_new_unused_key():
    from datetime import UTC, datetime, timedelta

    from app.api_key_documents import ApiKeyDocument
    from app.lib import api_keys

    now = datetime.now(UTC)
    used = ApiKeyDocument(
        key_id="used0001",
        key_hash="hash",
        email="a@example.com",
        created_at=now - timedelta(days=3),
        last_used_at=now - timedelta(days=1),
        monthly_call_count=3,
    )
    unused = ApiKeyDocument(
        key_id="new00001", key_hash="hash", email="a@example.com", created_at=now, last_used_at=now
    )
    assert api_keys.choose_retained_key([used, unused]).key_id == used.key_id


def test_unused_duplicates_require_explicit_selection():
    from datetime import UTC, datetime

    from app.api_key_documents import ApiKeyDocument
    from app.lib import api_keys

    now = datetime.now(UTC)
    keys = [
        ApiKeyDocument(
            key_id=k, key_hash="hash", email="a@example.com", created_at=now, last_used_at=now
        )
        for k in ["unused01", "unused02"]
    ]
    with pytest.raises(ValueError, match="--keep"):
        api_keys.choose_retained_key(keys)
    assert api_keys.choose_retained_key(keys, {"unused01"}).key_id == "unused01"


def test_privilege_conflict_requires_explicit_selection():
    from datetime import UTC, datetime

    from app.api_key_documents import ApiKeyDocument
    from app.lib import api_keys

    keys = [
        ApiKeyDocument(
            key_id=k,
            key_hash="hash",
            email="a@example.com",
            is_admin=admin,
            monthly_call_count=1,
            last_used_at=datetime.now(UTC),
        )
        for k, admin in [("normal01", False), ("admin001", True)]
    ]
    with pytest.raises(ValueError, match="--keep"):
        api_keys.choose_retained_key(keys)


def test_normalization_and_invalid_email():
    from app.lib import api_keys

    assert api_keys.normalize_email(" Alice@Example.COM ") == "alice@example.com"
    for email in ["", "no-at", "a@", "a b@example.com", "a@@b.com"]:
        with pytest.raises(ValueError):
            api_keys.normalize_email(email)


@pytest.mark.asyncio
async def test_verification_failure_reports_created_key_without_retry(monkeypatch, capsys):
    import argparse
    from unittest.mock import AsyncMock, Mock

    import apikeys
    from google.cloud import firestore

    from app.lib import api_keys

    doc = Mock(key_id="test0001", email="alice@example.com", is_admin=False)
    create = AsyncMock(return_value=(doc, "gea_test-secret"))
    db = Mock()
    monkeypatch.setattr(firestore, "AsyncClient", Mock(return_value=db))
    monkeypatch.setattr(api_keys, "create_api_key", create)
    monkeypatch.setattr(apikeys, "verify_key", AsyncMock(return_value=False))
    result = await apikeys.execute(
        argparse.Namespace(command="generate", email="alice@example.com", admin=False),
        apikeys.Destination("prod", "greenearth-prod", "https://api.greenearth.social"),
    )
    output = capsys.readouterr()
    assert result == 1
    assert "test0001 WAS CREATED" in output.err
    assert "Do not retry" in output.err
    create.assert_awaited_once()
    db.close.assert_called_once()


def test_destination_mapping_matches_deployment_and_collection_is_not_parameter(capsys):
    import apikeys

    parser = apikeys.build_parser()
    for environment in ["stage", "prod"]:
        parser.parse_args(["--environment", environment, "list"])
        assert apikeys.DESTINATIONS[environment][0] == "greenearth-" + environment
        apikeys.report_destination(
            apikeys.Destination(
                environment, "greenearth-" + environment, apikeys.DESTINATIONS[environment][1]
            )
        )
    assert "api_keys (fixed)" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        parser.parse_args(["--environment", "prod", "--collection", "wrong", "list"])


def test_generate_never_persists_plaintext():
    # Schema compatibility is covered independently of the application's feed imports.
    from app.lib.api_keys import _new_key

    doc, plaintext = _new_key("alice@example.com", False)
    assert plaintext not in str(doc.model_dump())


@pytest.mark.parametrize("missing_field", ["last_used_at", "created_at"])
def test_missing_legacy_history_cannot_displace_a_used_key(missing_field):
    from datetime import UTC, datetime, timedelta

    from app.api_key_documents import ApiKeyDocument
    from app.lib.api_keys import choose_retained_key

    now = datetime.now(UTC)
    data = dict(
        key_id="unknown1",
        key_hash="hash",
        email="alice@example.com",
        created_at=now - timedelta(days=10),
        last_used_at=now,
        monthly_call_count=0,
    )
    del data[missing_field]
    unknown = ApiKeyDocument.model_validate(data)
    used = ApiKeyDocument(
        key_id="used0001",
        key_hash="hash",
        email="alice@example.com",
        created_at=now - timedelta(days=20),
        last_used_at=now - timedelta(days=1),
        monthly_call_count=5,
    )
    with pytest.raises(ValueError, match="--keep"):
        choose_retained_key([used, unknown])
    assert choose_retained_key([used, unknown], {used.key_id}).key_id == used.key_id


def test_missing_dotenv_has_actionable_error_without_traceback():
    code = """
import importlib.abc, runpy, sys
class BlockDotenv(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname == "dotenv":
            raise ModuleNotFoundError("No module named dotenv", name="dotenv")
sys.meta_path.insert(0, BlockDotenv())
sys.argv = [sys.argv[1], "--environment", "prod", "list"]
runpy.run_path(sys.argv[0], run_name="__main__")
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(CLI)], capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 1
    assert "pipenv sync --dev" in result.stderr
    assert "Traceback" not in result.stderr

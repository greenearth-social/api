import logging
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from . import oauth_revoke
from .oauth_revoke import OAUTH_REVOCATIONS_COLLECTION, revoke_oauth_grant

DID = "did:plc:abc"
URL = "https://oauthrevoke-abc-uc.a.run.app"


def make_db(add_side_effect=None):
    db = MagicMock()
    db.collection.return_value.add = AsyncMock(side_effect=add_side_effect)
    return db


def client_returning(status=200, json=None, exc=None, seen=None, content=None):
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        if exc:
            raise exc
        if content is not None:
            return httpx.Response(status, content=content)
        return httpx.Response(status, json=json if json is not None else {})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("GE_OAUTH_REVOKE_URL", URL)
    monkeypatch.setattr(oauth_revoke, "_fetch_id_token", lambda audience: "id-token-secret")


def audit_doc(db):
    db.collection.assert_called_with(OAUTH_REVOCATIONS_COLLECTION)
    return db.collection.return_value.add.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["revoked", "already_revoked", "no_session", "failed"])
async def test_function_outcomes_pass_through_and_are_audited(outcome):
    db = make_db()
    async with client_returning(json={"outcome": outcome}) as client:
        assert await revoke_oauth_grant(db, DID, "admin:k1", client=client) == outcome
    doc = audit_doc(db)
    assert doc["did"] == DID and doc["actor"] == "admin:k1" and doc["outcome"] == outcome
    assert doc["created_at"] is not None


@pytest.mark.asyncio
async def test_sends_a_google_id_token_and_the_did_only():
    db, seen = make_db(), []
    async with client_returning(json={"outcome": "revoked"}, seen=seen) as client:
        await revoke_oauth_grant(db, DID, "user:" + DID, client=client)
    assert seen[0].headers["authorization"] == "Bearer id-token-secret"
    assert seen[0].content == b'{"did":"did:plc:abc"}'


@pytest.mark.asyncio
async def test_plain_http_targets_skip_the_id_token(monkeypatch):
    monkeypatch.setenv("GE_OAUTH_REVOKE_URL", "http://firebase:5001/p/us/oauthRevoke")
    monkeypatch.setattr(oauth_revoke, "_fetch_id_token", lambda audience: pytest.fail("no token"))
    seen = []
    async with client_returning(json={"outcome": "revoked"}, seen=seen) as client:
        await revoke_oauth_grant(make_db(), DID, "cli:max", client=client)
    assert "authorization" not in seen[0].headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, error",
    [
        ({"status": 500}, "status_500"),
        ({"status": 200, "json": {"outcome": "bogus"}}, "invalid_response"),
        ({"status": 200, "content": b"not json"}, "JSONDecodeError"),
        ({"exc": httpx.ReadTimeout("slow")}, "ReadTimeout"),
        ({"exc": httpx.ConnectError("down")}, "ConnectError"),
    ],
)
async def test_any_transport_or_contract_failure_is_failed_and_audited(kwargs, error):
    db = make_db()
    async with client_returning(**kwargs) as client:
        assert await revoke_oauth_grant(db, DID, "admin:k1", client=client) == "failed"
    assert audit_doc(db)["error"] == error


@pytest.mark.asyncio
async def test_missing_url_or_token_failure_is_failed(monkeypatch):
    db = make_db()
    monkeypatch.delenv("GE_OAUTH_REVOKE_URL")
    assert await revoke_oauth_grant(db, DID, "admin:k1") == "failed"
    assert audit_doc(db)["error"] == "not_configured"

    monkeypatch.setenv("GE_OAUTH_REVOKE_URL", URL)
    monkeypatch.setattr(oauth_revoke, "_fetch_id_token", MagicMock(side_effect=RuntimeError("adc")))
    db2 = make_db()
    async with client_returning() as client:
        assert await revoke_oauth_grant(db2, DID, "admin:k1", client=client) == "failed"
    assert audit_doc(db2)["error"] == "RuntimeError"


@pytest.mark.asyncio
async def test_audit_write_failure_still_returns_the_outcome_and_logs_an_error(caplog):
    db = make_db(add_side_effect=RuntimeError("firestore down"))
    async with client_returning(json={"outcome": "revoked"}) as client:
        with caplog.at_level(logging.ERROR):
            assert await revoke_oauth_grant(db, DID, "admin:k1", client=client) == "revoked"
    record = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert DID in record.getMessage() and "admin:k1" in record.getMessage()
    assert "RuntimeError" in record.getMessage()
    assert "id-token-secret" not in caplog.text


@pytest.mark.asyncio
async def test_failed_outcome_logs_a_warning_with_no_tokens(caplog):
    db = make_db()
    async with client_returning(status=500) as client:
        with caplog.at_level(logging.WARNING):
            assert await revoke_oauth_grant(db, DID, "admin:k1", client=client) == "failed"
    record = next(r for r in caplog.records if r.levelno == logging.WARNING)
    assert DID in record.getMessage() and "admin:k1" in record.getMessage()
    assert "status_500" in record.getMessage()
    assert "id-token-secret" not in caplog.text


@pytest.mark.asyncio
async def test_non_failed_outcomes_do_not_log_a_warning(caplog):
    db = make_db()
    async with client_returning(json={"outcome": "revoked"}) as client:
        with caplog.at_level(logging.WARNING):
            await revoke_oauth_grant(db, DID, "admin:k1", client=client)
    assert not any(r.levelno == logging.WARNING for r in caplog.records)


@pytest.mark.asyncio
async def test_audit_contains_no_secrets():
    db = make_db()
    async with client_returning(json={"outcome": "revoked"}) as client:
        await revoke_oauth_grant(db, DID, "admin:k1", client=client)
    assert "id-token-secret" not in repr(audit_doc(db))

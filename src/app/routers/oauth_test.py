from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from ..main import app
from ..security import Caller, verify_admin_or_user

client = TestClient(app)
DID = "did:plc:abc"
PATH = "/api/oauth/revoke"


@pytest.fixture
def as_caller():
    def set_caller(caller):
        app.dependency_overrides[verify_admin_or_user] = lambda: caller

    yield set_caller
    app.dependency_overrides.pop(verify_admin_or_user, None)


@pytest.fixture(autouse=True)
def _firestore():
    app.state.firestore = object()
    yield


def revoke_returning(outcome):
    return patch("app.routers.oauth.revoke_oauth_grant", AsyncMock(return_value=outcome))


@pytest.mark.parametrize("outcome", ["revoked", "already_revoked", "no_session"])
def test_admin_success_outcomes_return_200(as_caller, outcome):
    as_caller(Caller("admin", "k1"))
    with revoke_returning(outcome) as svc:
        r = client.post(PATH, json={"did": DID})
    assert r.status_code == 200 and r.json() == {"did": DID, "outcome": outcome}
    assert svc.await_args is not None
    assert svc.await_args.args[1:] == (DID, "admin:k1")


def test_failed_outcome_returns_502_and_names_the_outcome(as_caller):
    as_caller(Caller("admin", "k1"))
    with revoke_returning("failed"):
        r = client.post(PATH, json={"did": DID})
    assert r.status_code == 502 and r.json() == {"did": DID, "outcome": "failed"}


def test_user_can_revoke_their_own_grant(as_caller):
    as_caller(Caller("user", DID))
    with revoke_returning("revoked") as svc:
        assert client.post(PATH, json={"did": DID}).status_code == 200
    assert svc.await_args is not None
    assert svc.await_args.args[1:] == (DID, f"user:{DID}")


def test_user_cannot_revoke_someone_elses_grant(as_caller):
    as_caller(Caller("user", "did:plc:mallory"))
    with revoke_returning("revoked") as svc:
        r = client.post(PATH, json={"did": DID})
    assert r.status_code == 403
    svc.assert_not_awaited()


def test_non_admin_key_is_rejected_before_anything_runs():
    def deny():
        raise HTTPException(status_code=403, detail="Admin API key required")

    app.dependency_overrides[verify_admin_or_user] = deny
    try:
        with revoke_returning("revoked") as svc:
            assert client.post(PATH, json={"did": DID}).status_code == 403
        svc.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(verify_admin_or_user, None)


@pytest.mark.parametrize("bad", ["", "did:plc:", "nope", "did:plc:a/b", 5, None])
def test_malformed_did_is_422_and_never_reaches_the_service(as_caller, bad):
    as_caller(Caller("admin", "k1"))
    with revoke_returning("revoked") as svc:
        assert client.post(PATH, json={"did": bad}).status_code == 422
    svc.assert_not_awaited()


def test_missing_did_is_422(as_caller):
    as_caller(Caller("admin", "k1"))
    assert client.post(PATH, json={}).status_code == 422


def test_unauthenticated_request_is_401_even_with_a_malformed_did():
    r = TestClient(app).post(PATH, json={"did": "nope"})
    assert r.status_code == 401


def test_firestore_unavailable_is_503(as_caller):
    as_caller(Caller("admin", "k1"))
    app.state.firestore = None
    assert client.post(PATH, json={"did": DID}).status_code == 503

"""Service preference endpoints use explicit user selection and scoped API keys."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from ..documents import ApiKeyDocument, FeedPreferencesDocument, UserDocument
from ..lib.api_keys import generate_key
from ..lib.firebase_auth import verify_firebase_auth
from ..main import app

USER_DID = "did:plc:target-user"
PREFERENCES_URL = f"/api/users/{USER_DID}/feeds/preferences"


@pytest.fixture
def key_credentials():
    return generate_key()


@pytest.fixture
def key_doc(key_credentials):
    key_id, _, key_hash = key_credentials
    return ApiKeyDocument(key_id=key_id, key_hash=key_hash, email="service@example.com")


@pytest.fixture
def key_lookup(key_doc, monkeypatch):
    lookup = AsyncMock(return_value=key_doc)
    monkeypatch.setattr("app.lib.api_keys.get_api_key_doc", lookup)
    return lookup


@pytest.fixture
def client(key_lookup, monkeypatch):
    db = MagicMock()
    db.collection.return_value.document.return_value = AsyncMock()
    monkeypatch.setattr(app.state, "firestore", db, raising=False)
    # Exercise the registered service routes and real scope/authentication checks.
    # Removing the global Firebase override proves an API key alone is sufficient.
    previous_auth = app.dependency_overrides.pop(verify_firebase_auth, None)
    yield TestClient(app)
    if previous_auth is not None:
        app.dependency_overrides[verify_firebase_auth] = previous_auth
    else:
        app.dependency_overrides.pop(verify_firebase_auth, None)


@pytest.fixture
def preferences_io(monkeypatch):
    io = MagicMock()
    io.read = AsyncMock(return_value=None)
    io.save = AsyncMock(return_value=FeedPreferencesDocument(freshness=4, purpose=0.65))
    io.clear_seen = AsyncMock()
    monkeypatch.setattr("app.lib.user_preferences.get_user", io.read)
    monkeypatch.setattr("app.lib.user_preferences.patch_user_feed_preferences", io.save)
    monkeypatch.setattr("app.lib.user_preferences.delete_most_recent_seen_bucket", io.clear_seen)
    return io


@pytest.fixture
def scoped_headers(key_doc, key_credentials):
    key_doc.scopes.append("user-config")
    return {"X-API-Key": key_credentials[1]}


@pytest.fixture(
    params=[
        {"method": "GET", "url": PREFERENCES_URL},
        {
            "method": "PATCH",
            "url": f"{PREFERENCES_URL}/your-feed",
            "json": {"freshness": 4},
        },
    ],
    ids=["read", "write"],
)
def preference_request(request):
    return request.param


@pytest.mark.parametrize("reason", ["missing", "malformed", "unknown", "revoked", "wrong-secret"])
def test_rejects_missing_or_invalid_key(
    client, key_credentials, key_doc, key_lookup, preferences_io, preference_request, reason
):
    headers = {"X-API-Key": key_credentials[1]}
    if reason == "missing":
        headers = {}
    elif reason == "malformed":
        headers["X-API-Key"] = "invalid"
    elif reason == "unknown":
        key_lookup.return_value = None
    elif reason == "revoked":
        key_doc.is_active = False
    else:
        key_doc.key_hash = "wrong-hash"

    response = client.request(**preference_request, headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}
    assert preferences_io.mock_calls == []


@pytest.mark.parametrize("is_admin", [False, True])
@pytest.mark.parametrize("additional_scopes", [[], ["other-scope"]])
def test_requires_explicit_user_config_scope(
    client, key_credentials, key_doc, preferences_io, preference_request, is_admin,
    additional_scopes,
):
    key_doc.is_admin = is_admin
    key_doc.scopes.extend(additional_scopes)

    response = client.request(
        **preference_request, headers={"X-API-Key": key_credentials[1]}
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "API key missing required scope: user-config"}
    assert preferences_io.mock_calls == []


def test_firebase_auth_does_not_replace_service_key(client, preferences_io, preference_request):
    app.dependency_overrides[verify_firebase_auth] = lambda: "target-user"

    response = client.request(**preference_request, headers={"Authorization": "Bearer token"})

    assert response.status_code == 401
    assert preferences_io.mock_calls == []


@pytest.mark.parametrize("stored_user", [False, True])
def test_read_matches_firebase_preferences_for_target_user(
    client, scoped_headers, preferences_io, stored_user
):
    if stored_user:
        preferences_io.read.return_value = UserDocument(
            user_did=USER_DID,
            username="target.bsky.social",
            feed_preferences={"random": FeedPreferencesDocument(freshness=2)},
        )

    response = client.get(PREFERENCES_URL, headers=scoped_headers)

    assert response.status_code == 200
    preferences_io.read.assert_awaited_once_with(app.state.firestore, USER_DID)
    assert response.json()["feeds"]["random"] == {"freshness": 2 if stored_user else 5}
    assert set(response.json()["feeds"]) == {"your-feed", "best-of-friends", "random"}
    preferences_io.save.assert_not_awaited()
    preferences_io.clear_seen.assert_not_awaited()

    app.dependency_overrides[verify_firebase_auth] = lambda: "target-user"
    firebase_response = client.get("/api/feeds/preferences")
    assert firebase_response.status_code == 200
    assert response.json() == firebase_response.json()


@pytest.mark.parametrize("is_admin", [False, True])
def test_patch_saves_target_user_then_clears_seen_history(
    client, scoped_headers, key_doc, key_lookup, preferences_io, is_admin
):
    key_doc.is_admin = is_admin

    async def clear_seen(db, user_did):
        # Cleanup must follow the committed save, for the same explicit user.
        preferences_io.save.assert_awaited_once_with(
            db, user_did, "best-of-friends", FeedPreferencesDocument(freshness=4)
        )

    preferences_io.clear_seen.side_effect = clear_seen
    response = client.patch(
        f"{PREFERENCES_URL}/best-of-friends", headers=scoped_headers, json={"freshness": 4}
    )

    assert response.status_code == 200
    assert response.json() == {"freshness": 4, "purpose": 0.65}
    preferences_io.save.assert_awaited_once_with(
        app.state.firestore, USER_DID, "best-of-friends", FeedPreferencesDocument(freshness=4)
    )
    preferences_io.clear_seen.assert_awaited_once_with(app.state.firestore, USER_DID)
    key_lookup.assert_awaited_once_with(app.state.firestore, key_doc.key_id)
    app.state.firestore.collection.return_value.document.return_value.update.assert_awaited_once()


def test_patch_succeeds_when_seen_cleanup_fails(client, scoped_headers, preferences_io):
    preferences_io.clear_seen.side_effect = RuntimeError("cleanup unavailable")

    response = client.patch(
        f"{PREFERENCES_URL}/best-of-friends", headers=scoped_headers, json={"freshness": 4}
    )

    assert response.status_code == 200
    assert response.json() == {"freshness": 4, "purpose": 0.65}
    preferences_io.save.assert_awaited_once()
    preferences_io.clear_seen.assert_awaited_once_with(app.state.firestore, USER_DID)


def test_failed_save_does_not_clear_seen_history(client, scoped_headers, preferences_io):
    preferences_io.save.side_effect = RuntimeError("save unavailable")

    with pytest.raises(RuntimeError, match="save unavailable"):
        client.patch(
            f"{PREFERENCES_URL}/your-feed", headers=scoped_headers, json={"freshness": 4}
        )

    preferences_io.clear_seen.assert_not_awaited()


@pytest.mark.parametrize(
    ("feed_name", "body", "status_code"),
    [
        ("nope", {"freshness": 2}, 404),
        ("cold-start", {"freshness": 2}, 404),
        ("your-feed", {}, 422),
        ("your-feed", {"freshness": None}, 422),
        ("your-feed", {"freshness": 10}, 422),
        ("your-feed", {"politics": -0.01}, 422),
        ("your-feed", {"purpose": 1.01}, 422),
        ("your-feed", {"author_penalty": -0.01}, 422),
        ("your-feed", {"topic_penalty": 1.01}, 422),
        ("your-feed", {"source_weights": {"following": 0.5}}, 422),
        ("your-feed", {"socialRadius": 3}, 422),
        ("random", {"purpose": 0.8}, 422),
    ],
)
def test_patch_validation_matches_firebase_endpoint(
    client, scoped_headers, preferences_io, feed_name, body, status_code
):
    response = client.patch(
        f"{PREFERENCES_URL}/{feed_name}", headers=scoped_headers, json=body
    )
    app.dependency_overrides[verify_firebase_auth] = lambda: "target-user"
    firebase_response = client.patch(f"/api/feeds/preferences/{feed_name}", json=body)

    assert response.status_code == status_code
    assert firebase_response.status_code == status_code
    assert response.json() == firebase_response.json()
    assert preferences_io.mock_calls == []


@pytest.mark.parametrize(
    "user_did", ["target-user", "did:plc:", "did:PLC:target", "did:plc:bad user"]
)
def test_rejects_invalid_target_did(
    client, scoped_headers, preferences_io, preference_request, user_did
):
    request_kwargs = dict(preference_request)
    request_kwargs["url"] = request_kwargs["url"].replace(USER_DID, user_did)

    response = client.request(**request_kwargs, headers=scoped_headers)

    assert response.status_code == 422
    assert preferences_io.mock_calls == []

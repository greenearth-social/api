from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from ..documents import RedirectDocument
from ..main import app
from ..security import verify_admin_api_key

client = TestClient(app, follow_redirects=False)

_VALID_API_KEY = "test-api-key"


def _make_record(slug: str, url: str) -> RedirectDocument:
    return RedirectDocument(slug=slug, url=url)


def _auth() -> dict:
    return {"X-API-Key": _VALID_API_KEY}


# ---------------------------------------------------------------------------
# GET /r?to=... (existing behaviour — must not regress)
# ---------------------------------------------------------------------------


def test_redirect_basic():
    response = client.get("/r?to=https://bsky.app/profile/greenearth.social")
    assert response.status_code == 302
    assert response.headers["location"] == "https://bsky.app/profile/greenearth.social"


def test_redirect_appends_utm_params():
    response = client.get(
        "/r?to=https://bsky.app/profile/greenearth.social"
        "&utm_source=bluesky&utm_medium=social&utm_campaign=launch"
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert "utm_source=bluesky" in location
    assert "utm_medium=social" in location
    assert "utm_campaign=launch" in location


def test_redirect_preserves_existing_query_params():
    response = client.get("/r?to=https://example.com/page%3Ffoo%3Dbar&utm_source=bluesky")
    assert response.status_code == 302
    location = response.headers["location"]
    assert "utm_source=bluesky" in location


def test_redirect_missing_to_returns_400():
    response = client.get("/r?utm_source=bluesky")
    assert response.status_code == 400


def test_redirect_non_https_returns_400():
    response = client.get("/r?to=http://bsky.app")
    assert response.status_code == 400


def test_redirect_invalid_url_returns_400():
    response = client.get("/r?to=not-a-url")
    assert response.status_code == 400


def test_redirect_omits_empty_utm_params():
    response = client.get("/r?to=https://bsky.app&utm_source=bluesky&utm_medium=")
    assert response.status_code == 302
    location = response.headers["location"]
    assert "utm_source=bluesky" in location
    assert "utm_medium" not in location


# ---------------------------------------------------------------------------
# GET /settings/{feed_name}
# ---------------------------------------------------------------------------


def test_settings_redirect_resolves_current_preview_origin(monkeypatch):
    metadata_url = "https://functions.example/oauthClientMetadataStage"
    monkeypatch.setenv("GE_SETTINGS_APP_METADATA_URL", metadata_url)
    monkeypatch.setenv("GE_SETTINGS_APP_ORIGIN", "https://fallback.example")
    metadata_response = MagicMock()
    metadata_response.json.return_value = {
        "client_uri": "https://greenearth-471522--stage-4tnzb2wq.web.app"
    }
    http_client = MagicMock()
    http_client.get = AsyncMock(return_value=metadata_response)

    with patch("app.routers.redirect.get_http_client", return_value=http_client):
        response = client.get("/settings/your-feed")

    assert response.status_code == 302
    assert response.headers["location"] == (
        "https://greenearth-471522--stage-4tnzb2wq.web.app/#/settings/your-feed"
    )
    http_client.get.assert_awaited_once_with(metadata_url, timeout=5.0)


def test_settings_redirect_falls_back_when_metadata_is_unavailable(monkeypatch):
    monkeypatch.setenv("GE_SETTINGS_APP_METADATA_URL", "https://functions.example/metadata")
    monkeypatch.setenv("GE_SETTINGS_APP_ORIGIN", "https://fallback.example/")
    http_client = MagicMock()
    http_client.get = AsyncMock(
        side_effect=httpx.ConnectError(
            "unavailable",
            request=httpx.Request("GET", "https://functions.example/metadata"),
        )
    )

    with patch("app.routers.redirect.get_http_client", return_value=http_client):
        response = client.get("/settings/random")

    assert response.status_code == 302
    assert response.headers["location"] == "https://fallback.example/#/settings/random"


@pytest.mark.parametrize("feed_name", ["missing", "two-tower"])
def test_settings_redirect_rejects_unknown_and_internal_feeds(feed_name):
    response = client.get(f"/settings/{feed_name}")

    assert response.status_code == 404


def test_settings_redirect_returns_503_without_valid_origin(monkeypatch):
    monkeypatch.delenv("GE_SETTINGS_APP_METADATA_URL", raising=False)
    monkeypatch.delenv("GE_SETTINGS_APP_ORIGIN", raising=False)

    response = client.get("/settings/your-feed")

    assert response.status_code == 503


# ---------------------------------------------------------------------------
# GET /r/{slug}
# ---------------------------------------------------------------------------


def test_slug_redirect_resolves_and_redirects():
    record = _make_record("bsky-profile", "https://bsky.app/profile/greenearth.social")
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.get_redirect", return_value=record):
        response = client.get("/r/bsky-profile")
    assert response.status_code == 302
    assert response.headers["location"] == "https://bsky.app/profile/greenearth.social"


def test_slug_redirect_appends_utm_params():
    record = _make_record("bsky-profile", "https://bsky.app/profile/greenearth.social")
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.get_redirect", return_value=record):
        response = client.get("/r/bsky-profile?utm_source=slack&utm_medium=chat")
    assert response.status_code == 302
    location = response.headers["location"]
    assert "utm_source=slack" in location
    assert "utm_medium=chat" in location


def test_slug_redirect_unknown_slug_returns_404():
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.get_redirect", return_value=None):
        response = client.get("/r/nonexistent")
    assert response.status_code == 404


def test_slug_redirect_no_firestore_returns_503():
    original = getattr(app.state, "firestore", None)
    try:
        app.state.firestore = None
        response = client.get("/r/bsky-profile")
    finally:
        if original is not None:
            app.state.firestore = original
        elif hasattr(app.state, "firestore"):
            delattr(app.state, "firestore")
    assert response.status_code == 503


def test_slug_redirect_preserves_existing_query_params():
    record = _make_record("search", "https://bsky.app/search?q=greenearth")
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.get_redirect", return_value=record):
        response = client.get("/r/search?utm_source=twitter")
    assert response.status_code == 302
    location = response.headers["location"]
    assert "q=greenearth" in location
    assert "utm_source=twitter" in location


# ---------------------------------------------------------------------------
# POST /admin/redirects
# ---------------------------------------------------------------------------


def test_admin_create_redirect():
    app.state.firestore = AsyncMock()
    record = _make_record("new-slug", "https://example.com")
    with patch("app.routers.redirect.create_redirect", return_value=record), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.post(
            "/admin/redirects",
            json={"slug": "new-slug", "url": "https://example.com"},
            headers=_auth(),
        )
    assert response.status_code == 201
    assert response.json()["slug"] == "new-slug"
    assert response.json()["url"] == "https://example.com"


def test_admin_create_redirect_conflict():
    app.state.firestore = AsyncMock()
    err = ValueError("Slug 'x' already exists")
    with patch("app.routers.redirect.create_redirect", side_effect=err), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.post(
            "/admin/redirects",
            json={"slug": "x", "url": "https://example.com"},
            headers=_auth(),
        )
    assert response.status_code == 409


def test_admin_create_redirect_invalid_url():
    app.state.firestore = AsyncMock()
    with patch("app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")):
        response = client.post(
            "/admin/redirects",
            json={"slug": "x", "url": "http://not-https.com"},
            headers=_auth(),
        )
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# PUT /admin/redirects/{slug}
# ---------------------------------------------------------------------------


def test_admin_update_redirect():
    app.state.firestore = AsyncMock()
    record = _make_record("bsky-profile", "https://new.example.com")
    with patch("app.routers.redirect.update_redirect", return_value=record), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.put(
            "/admin/redirects/bsky-profile",
            json={"url": "https://new.example.com"},
            headers=_auth(),
        )
    assert response.status_code == 200
    assert response.json()["url"] == "https://new.example.com"


def test_admin_update_redirect_not_found():
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.update_redirect", return_value=None), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.put(
            "/admin/redirects/nonexistent",
            json={"url": "https://example.com"},
            headers=_auth(),
        )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /admin/redirects/{slug}
# ---------------------------------------------------------------------------


def test_admin_delete_redirect():
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.delete_redirect", return_value=True), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.delete("/admin/redirects/bsky-profile", headers=_auth())
    assert response.status_code == 204


def test_admin_delete_redirect_not_found():
    app.state.firestore = AsyncMock()
    with patch("app.routers.redirect.delete_redirect", return_value=False), patch(
        "app.security.authenticate_api_key", return_value=AsyncMock(key_id="k1")
    ):
        response = client.delete("/admin/redirects/nonexistent", headers=_auth())
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Admin endpoints require an admin API key
# ---------------------------------------------------------------------------


@pytest.fixture
def _non_admin_override():
    def _raise():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin API key required"
        )

    app.dependency_overrides[verify_admin_api_key] = _raise
    yield
    app.dependency_overrides.pop(verify_admin_api_key, None)


def test_admin_create_redirect_forbidden_for_non_admin_key(_non_admin_override):
    app.state.firestore = AsyncMock()
    response = client.post(
        "/admin/redirects",
        json={"slug": "x", "url": "https://example.com"},
        headers=_auth(),
    )
    assert response.status_code == 403


def test_admin_update_redirect_forbidden_for_non_admin_key(_non_admin_override):
    app.state.firestore = AsyncMock()
    response = client.put(
        "/admin/redirects/bsky-profile",
        json={"url": "https://example.com"},
        headers=_auth(),
    )
    assert response.status_code == 403


def test_admin_delete_redirect_forbidden_for_non_admin_key(_non_admin_override):
    app.state.firestore = AsyncMock()
    response = client.delete("/admin/redirects/bsky-profile", headers=_auth())
    assert response.status_code == 403

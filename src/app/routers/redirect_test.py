from fastapi.testclient import TestClient
from ..main import app

client = TestClient(app, follow_redirects=False)


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
    response = client.get(
        "/r?to=https://example.com/page%3Ffoo%3Dbar&utm_source=bluesky"
    )
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
    response = client.get(
        "/r?to=https://bsky.app&utm_source=bluesky&utm_medium="
    )
    assert response.status_code == 302
    location = response.headers["location"]
    assert "utm_source=bluesky" in location
    assert "utm_medium" not in location

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app
from .telemetry import rate_limiter


@pytest.fixture(autouse=True)
def reset_rate_limits():
    """Reset rate limiter before each test."""
    rate_limiter.reset()
    yield
    rate_limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


def test_ingest_single_vitals_metric(client):
    mock_collector = MagicMock()
    with patch("app.routers.telemetry.get_metric_collector", return_value=mock_collector):
        response = client.post(
            "/api/telemetry",
            json={
                "type": "vitals",
                "name": "LCP",
                "value": 1420.5,
                "extra": {"app_route": "/feed"},
            },
        )
    assert response.status_code == 204
    mock_collector.record.assert_called_once_with(
        "frontend.web_vitals.lcp_ms",
        1420.5,
        route="/feed",
    )


def test_ingest_batched_vitals(client):
    mock_collector = MagicMock()
    with patch("app.routers.telemetry.get_metric_collector", return_value=mock_collector):
        response = client.post(
            "/api/telemetry",
            json=[
                {
                    "type": "vitals",
                    "name": "LCP",
                    "value": 1200.0,
                    "extra": {"app_route": "/feed"},
                },
                {
                    "type": "vitals",
                    "name": "INP",
                    "value": 85.2,
                    "extra": {"app_route": "/settings"},
                },
            ],
        )
    assert response.status_code == 204
    assert mock_collector.record.call_count == 2
    mock_collector.record.assert_any_call("frontend.web_vitals.lcp_ms", 1200.0, route="/feed")
    mock_collector.record.assert_any_call("frontend.web_vitals.inp_ms", 85.2, route="/settings")


def test_ingest_client_error(client, caplog):
    mock_collector = MagicMock()
    with patch("app.routers.telemetry.get_metric_collector", return_value=mock_collector):
        response = client.post(
            "/api/telemetry",
            json={
                "type": "error",
                "name": "SettingsSaveError",
                "extra": {"app_route": "/settings", "error_message": "Failed to persist config"},
            },
        )
    assert response.status_code == 204
    mock_collector.record.assert_called_once_with(
        "frontend.client_error_count",
        1,
        error_name="SettingsSaveError",
        route="/settings",
    )
    assert "Frontend client error: SettingsSaveError" in caplog.text


def test_ingest_telemetry_without_collector(client):
    with patch("app.routers.telemetry.get_metric_collector", return_value=None):
        response = client.post(
            "/telemetry",
            json={"type": "vitals", "name": "LCP", "value": 800.0},
        )
    assert response.status_code == 204


def test_ingest_telemetry_allowed_origins(client):
    allowed_origins = [
        "https://greenearth.social",
        "https://api.greenearth.social",
        "https://staging.greenearth.social",
        "https://greenearth-api-stage-oef7fsaama-ue.a.run.app",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
    ]
    for origin in allowed_origins:
        response = client.post(
            "/api/telemetry",
            json={"type": "vitals", "name": "LCP", "value": 900.0},
            headers={"Origin": origin},
        )
        assert response.status_code == 204, f"Origin {origin} failed with {response.status_code}"


def test_ingest_telemetry_forbidden_origin(client):
    forbidden_origins = [
        "https://evil-attacker.com",
        "https://fakegreenearth.social.evil.com",
        "http://not-greenearth.social",
        "https://example.org",
    ]
    for origin in forbidden_origins:
        response = client.post(
            "/api/telemetry",
            json={"type": "vitals", "name": "LCP", "value": 900.0},
            headers={"Origin": origin},
        )
        assert response.status_code == 403
        assert response.json()["detail"] == "Forbidden origin"


def test_ingest_telemetry_referer_fallback(client):
    response = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 950.0},
        headers={"Referer": "https://greenearth.social/feed"},
    )
    assert response.status_code == 204

    forbidden_referer = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 950.0},
        headers={"Referer": "https://malicious-site.com/attack"},
    )
    assert forbidden_referer.status_code == 403


def test_ingest_telemetry_missing_origin_in_deployed_env(client, monkeypatch):
    monkeypatch.setenv("GE_ENVIRONMENT", "production")
    response = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 850.0},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Missing origin header"


def test_ingest_telemetry_payload_too_large_content_length(client):
    response = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 1100.0},
        headers={"Content-Length": str(20 * 1024)},  # 20 KB
    )
    assert response.status_code == 413
    assert "Payload exceeds maximum size of 16KB" in response.json()["detail"]


def test_ingest_telemetry_batch_too_large(client):
    items = [{"type": "vitals", "name": "LCP", "value": 1000.0} for _ in range(51)]
    response = client.post(
        "/api/telemetry",
        json=items,
    )
    assert response.status_code == 413
    assert "Batch size exceeds maximum of 50 items" in response.json()["detail"]


def test_ingest_telemetry_rate_limiting(client):
    headers = {"X-Forwarded-For": "203.0.113.195"}
    for _ in range(60):
        res = client.post(
            "/api/telemetry",
            json={"type": "vitals", "name": "LCP", "value": 800.0},
            headers=headers,
        )
        assert res.status_code == 204

    # 61st request from same IP should be blocked
    blocked = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 800.0},
        headers=headers,
    )
    assert blocked.status_code == 429
    assert blocked.headers.get("Retry-After") == "60"
    assert blocked.json()["detail"] == "Rate limit exceeded"

    # Different IP should still be allowed
    diff_ip = client.post(
        "/api/telemetry",
        json={"type": "vitals", "name": "LCP", "value": 800.0},
        headers={"X-Forwarded-For": "203.0.113.196"},
    )
    assert diff_ip.status_code == 204

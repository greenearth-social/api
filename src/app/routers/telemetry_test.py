from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app


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

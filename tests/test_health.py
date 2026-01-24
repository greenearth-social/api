from fastapi.testclient import TestClient

from src.app.main import app

client = TestClient(app)


def test_healthcheck_returns_200():
    response = client.get("/health")
    assert response.status_code == 200


def test_healthcheck_response_body():
    response = client.get("/health")
    assert response.json() == {"status": "ok"}


def test_healthcheck_response_structure():
    response = client.get("/health")
    data = response.json()
    assert "status" in data
    assert isinstance(data["status"], str)

import pytest


@pytest.fixture(autouse=True)
def _set_perspective_api_key(monkeypatch):
    monkeypatch.setenv("GE_PERSPECTIVE_API_KEY", "test-dummy-key")

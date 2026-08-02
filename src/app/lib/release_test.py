from .release import api_release_sha


def test_api_release_sha_returns_trimmed_environment_value(monkeypatch):
    monkeypatch.setenv("GE_RELEASE_SHA", " abc123 ")

    assert api_release_sha() == "abc123"


def test_api_release_sha_is_none_when_unset(monkeypatch):
    monkeypatch.delenv("GE_RELEASE_SHA", raising=False)

    assert api_release_sha() is None

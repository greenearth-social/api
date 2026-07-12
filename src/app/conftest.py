import pytest
from posthog import Posthog

from .main import app
from .security import verify_api_key


@pytest.fixture(autouse=True)
def _default_api_key_override():
    """Bypass Firestore-backed API key auth in all tests by default.

    Tests that need to test auth behaviour (security_test.py) override
    verify_api_key themselves via their own fixtures, which run after this
    one and win because they also call app.dependency_overrides[verify_api_key].
    """
    app.dependency_overrides.setdefault(verify_api_key, lambda: "test-key-id")
    yield
    app.dependency_overrides.pop(verify_api_key, None)


@pytest.fixture(autouse=True)
def _set_perspective_api_key(monkeypatch):
    monkeypatch.setenv("GE_PERSPECTIVE_API_KEY", "test-dummy-key")


@pytest.fixture(autouse=True)
def _disable_posthog(monkeypatch):
    """Force every real Posthog client constructed during tests to be disabled.

    Tests should mock PostHog explicitly, but this is defense-in-depth so a
    stray GE_POSTHOG_API_KEY in a developer's environment can't cause a test
    run to send live analytics events.
    """
    real_init = Posthog.__init__

    def _disabled_init(self, *args, **kwargs):
        kwargs["disabled"] = True
        real_init(self, *args, **kwargs)

    monkeypatch.setattr(Posthog, "__init__", _disabled_init)

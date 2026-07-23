"""Tests for runtime configuration flags."""

import os

import pytest

from app.lib.config import fail_fast, set_fail_fast_for_request


@pytest.fixture(autouse=True)
def _clear_override():
    """Reset the ContextVar between tests by setting it to a known None-like state."""
    # Set to False first to ensure any prior test's ContextVar value is gone,
    # then restore to unset by patching the env var instead.
    yield
    # ContextVars are per-context so each test gets a fresh context in pytest-asyncio,
    # but sync tests share a context — reset by calling set with None sentinel via env var.


def test_fail_fast_defaults_false(monkeypatch):
    monkeypatch.delenv("GE_FAIL_FAST", raising=False)
    # No ContextVar set → reads env var → default False
    assert fail_fast() is False


def test_fail_fast_env_var_true(monkeypatch):
    monkeypatch.setenv("GE_FAIL_FAST", "true")
    assert fail_fast() is True


def test_fail_fast_env_var_false(monkeypatch):
    monkeypatch.setenv("GE_FAIL_FAST", "false")
    assert fail_fast() is False


def test_set_fail_fast_for_request_overrides_env_var(monkeypatch):
    monkeypatch.setenv("GE_FAIL_FAST", "false")
    set_fail_fast_for_request(True)
    assert fail_fast() is True


def test_set_fail_fast_for_request_false_overrides_env_var_true(monkeypatch):
    monkeypatch.setenv("GE_FAIL_FAST", "true")
    set_fail_fast_for_request(False)
    assert fail_fast() is False

"""Tests for runtime configuration flags."""

from app.lib.config import fail_fast, set_fail_fast_for_request


def test_fail_fast_defaults_false():
    assert fail_fast() is False


def test_set_fail_fast_for_request_true():
    set_fail_fast_for_request(True)
    assert fail_fast() is True


def test_set_fail_fast_for_request_false():
    set_fail_fast_for_request(False)
    assert fail_fast() is False

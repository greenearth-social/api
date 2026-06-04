"""Tests for signed feedContext tokens."""

import pytest

from .feed_context import (
    FeedContextPayload,
    decode_feed_context,
    encode_feed_context,
)

SECRET = "test-secret-value"


@pytest.fixture(autouse=True)
def _set_secret(monkeypatch):
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", SECRET)


def _payload(**overrides) -> FeedContextPayload:
    payload = FeedContextPayload(did="did:plc:abc123", feed="your-feed", rid="req-1", iat=1730000000)
    return payload.model_copy(update=overrides) if overrides else payload


class TestRoundTrip:
    def test_encode_decode_round_trip(self):
        payload = _payload()
        token = encode_feed_context(payload)
        decoded = decode_feed_context(token)
        assert decoded == payload

    def test_token_has_two_segments(self):
        token = encode_feed_context(_payload())
        assert token.count(".") == 1

    def test_token_well_under_2000_chars(self):
        token = encode_feed_context(_payload())
        assert len(token) < 2000


class TestVerificationFailures:
    def test_tampered_payload_rejected(self):
        token = encode_feed_context(_payload())
        payload_b64, sig = token.split(".", 1)
        # Flip a character in the payload; signature no longer matches.
        mangled = ("A" if payload_b64[0] != "A" else "B") + payload_b64[1:]
        assert decode_feed_context(f"{mangled}.{sig}") is None

    def test_tampered_signature_rejected(self):
        token = encode_feed_context(_payload())
        payload_b64, sig = token.split(".", 1)
        mangled_sig = ("A" if sig[0] != "A" else "B") + sig[1:]
        assert decode_feed_context(f"{payload_b64}.{mangled_sig}") is None

    def test_wrong_secret_rejected(self, monkeypatch):
        token = encode_feed_context(_payload())
        monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "a-different-secret")
        assert decode_feed_context(token) is None

    def test_malformed_token_rejected(self):
        assert decode_feed_context("not-a-valid-token") is None

    def test_empty_token_rejected(self):
        assert decode_feed_context("") is None


class TestMissingSecret:
    def test_encode_raises_without_secret(self, monkeypatch):
        monkeypatch.delenv("GE_FEED_CONTEXT_SECRET", raising=False)
        with pytest.raises(RuntimeError):
            encode_feed_context(_payload())

    def test_decode_raises_without_secret(self, monkeypatch):
        token = encode_feed_context(_payload())
        monkeypatch.delenv("GE_FEED_CONTEXT_SECRET", raising=False)
        with pytest.raises(RuntimeError):
            decode_feed_context(token)

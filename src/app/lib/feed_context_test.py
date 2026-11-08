"""Tests for signed feedContext tokens."""

import pytest

from .feed_context import (
    FeedContextPayload,
    _b64decode,
    _b64encode,
    _secret,
    _sign,
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


class TestLoadTestClaim:
    def test_lt_defaults_to_false(self):
        assert _payload().lt is False

    def test_lt_round_trips(self):
        token = encode_feed_context(_payload(lt=True))
        decoded = decode_feed_context(token)
        assert decoded is not None
        assert decoded.lt is True

    def test_lt_token_still_well_under_2000_chars(self):
        assert len(encode_feed_context(_payload(lt=True))) < 2000

    def test_legacy_token_without_lt_decodes_to_false(self):
        """Tokens minted before ``lt`` existed still decode, defaulting to False."""
        import json

        legacy = {"did": "did:plc:abc123", "feed": "your-feed", "rid": "req-1", "iat": 1730000000}
        payload_b64 = _b64encode(json.dumps(legacy).encode())
        token = f"{payload_b64}.{_sign(payload_b64, _secret())}"
        decoded = decode_feed_context(token)
        assert decoded is not None
        assert decoded.lt is False


class TestPostMetadataClaims:
    @pytest.mark.parametrize("score", [0.75, 0.0])
    def test_post_metadata_round_trips(self, score):
        post_uri = "at://did:plc:author/app.bsky.feed.post/1"
        token = encode_feed_context(
            _payload(hrs=score, adid="did:plc:author", uri=post_uri)
        )
        decoded = decode_feed_context(token)
        assert decoded is not None
        assert decoded.hrs == score
        assert decoded.adid == "did:plc:author"
        assert decoded.uri == post_uri
        assert len(token) < 2000

    @pytest.mark.parametrize("metadata", [{}, {"hrs": 0.75, "adid": "did:plc:author"}])
    def test_legacy_token_without_post_uri_decodes_to_none(self, metadata):
        import json

        legacy = {
            "did": "did:plc:abc123",
            "feed": "your-feed",
            "rid": "req-1",
            "iat": 1730000000,
            "lt": False,
            **metadata,
        }
        payload_b64 = _b64encode(json.dumps(legacy).encode())
        token = f"{payload_b64}.{_sign(payload_b64, _secret())}"
        decoded = decode_feed_context(token)
        assert decoded is not None
        assert decoded.hrs == metadata.get("hrs")
        assert decoded.adid == metadata.get("adid")
        assert decoded.uri is None

    @pytest.mark.parametrize("author_did", ["did:plc:author", None])
    def test_missing_metadata_is_nullable_in_signed_payload(self, author_did):
        import json

        token = encode_feed_context(_payload(adid=author_did))
        payload_b64, _signature = token.split(".", 1)
        assert json.loads(_b64decode(payload_b64)) == {
            "did": "did:plc:abc123",
            "feed": "your-feed",
            "rid": "req-1",
            "iat": 1730000000,
            "lt": False,
            "hrs": None,
            "adid": author_did,
            "uri": None,
        }


class TestVerificationFailures:
    def test_tampered_post_uri_rejected(self):
        import json

        token = encode_feed_context(
            _payload(
                hrs=0.75,
                adid="did:plc:author",
                post_uri="at://did:plc:author/app.bsky.feed.post/1",
            )
        )
        payload_b64, sig = token.split(".", 1)
        claims = json.loads(_b64decode(payload_b64))
        claims["uri"] = "at://did:plc:other/app.bsky.feed.post/2"
        tampered_payload = _b64encode(json.dumps(claims).encode())
        assert decode_feed_context(f"{tampered_payload}.{sig}") is None

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

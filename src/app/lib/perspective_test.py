"""Tests for PRC scoring and Perspective API candidate scoring."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ..models import CandidatePost
from . import perspective as perspective_module
from .perspective import PerspectiveLanguageNotSupportedError, _prc_score, score_candidates


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reset module-level rate limiter state between tests."""
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0
    yield
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0


# ---------------------------------------------------------------------------
# _prc_score arithmetic
#
# Mirrors the `perspective_baseline_minus_outrage_toxic` weight groups from
# the PRC reference implementation: 6 positively-weighted "bridging"
# attributes at +1/6 each, and 9 negatively-weighted attributes split into
# three groups — 2 "outrage" attrs at -1/6, 3 "outrage" attrs at -1/18, and
# 4 "toxic" attrs at -1/8 — summing to weights of (-1.0, +1.0).
# ---------------------------------------------------------------------------

_BRIDGING_ATTRS = [
    "REASONING_EXPERIMENTAL",
    "PERSONAL_STORY_EXPERIMENTAL",
    "AFFINITY_EXPERIMENTAL",
    "COMPASSION_EXPERIMENTAL",
    "RESPECT_EXPERIMENTAL",
    "CURIOSITY_EXPERIMENTAL",
]
_OUTRAGE_SIXTH_ATTRS = ["FEARMONGERING_EXPERIMENTAL", "GENERALIZATION_EXPERIMENTAL"]
_OUTRAGE_EIGHTEENTH_ATTRS = [
    "SCAPEGOATING_EXPERIMENTAL",
    "MORAL_OUTRAGE_EXPERIMENTAL",
    "ALIENATION_EXPERIMENTAL",
]
_TOXIC_EIGHTH_ATTRS = ["TOXICITY", "IDENTITY_ATTACK", "INSULT", "THREAT"]
_ALL_PRC_ATTRS = _BRIDGING_ATTRS + _OUTRAGE_SIXTH_ATTRS + _OUTRAGE_EIGHTEENTH_ATTRS + _TOXIC_EIGHTH_ATTRS


def _zero_attr() -> dict[str, float]:
    return dict.fromkeys(_ALL_PRC_ATTRS, 0.0)


class TestPrcScore:
    def test_all_zeros_returns_zero(self):
        assert _prc_score(_zero_attr()) == pytest.approx(0.0)

    def test_pure_bridging_returns_positive(self):
        attr = {**_zero_attr(), **dict.fromkeys(_BRIDGING_ATTRS, 1.0)}
        # 6 attrs at weight 1/6 each, all at 1.0 -> score = 1.0
        assert _prc_score(attr) == pytest.approx(1.0)

    def test_pure_negative_returns_negative(self):
        attr = {
            **_zero_attr(),
            **dict.fromkeys(_OUTRAGE_SIXTH_ATTRS, 1.0),
            **dict.fromkeys(_OUTRAGE_EIGHTEENTH_ATTRS, 1.0),
            **dict.fromkeys(_TOXIC_EIGHTH_ATTRS, 1.0),
        }
        # negative weights sum to -1.0 (2*(-1/6) + 3*(-1/18) + 4*(-1/8)),
        # all at 1.0 -> score = -1.0
        assert _prc_score(attr) == pytest.approx(-1.0)

    def test_known_mixed_inputs(self):
        attr = {
            **dict.fromkeys(_BRIDGING_ATTRS, 0.6),
            **dict.fromkeys(_OUTRAGE_SIXTH_ATTRS, 0.3),
            **dict.fromkeys(_OUTRAGE_EIGHTEENTH_ATTRS, 0.9),
            **dict.fromkeys(_TOXIC_EIGHTH_ATTRS, 0.4),
        }
        expected = (
            len(_BRIDGING_ATTRS) * (1 / 6) * 0.6
            + len(_OUTRAGE_SIXTH_ATTRS) * (-1 / 6) * 0.3
            + len(_OUTRAGE_EIGHTEENTH_ATTRS) * (-1 / 18) * 0.9
            + len(_TOXIC_EIGHTH_ATTRS) * (-1 / 8) * 0.4
        )
        assert _prc_score(attr) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# score_candidates
# ---------------------------------------------------------------------------

def _make_candidate(uri: str, content: str | None = "text", score: float = 1.0) -> CandidatePost:
    return CandidatePost(
        at_uri=uri,
        content=content,
        score=score,
        minilm_l12_embedding=None,
        generator_name="test",
    )


def _fake_client(scores: list[float]) -> MagicMock:
    """Build a mock PerspectiveClient whose score() yields values in order."""
    client = MagicMock()
    client.score = AsyncMock(side_effect=scores)
    return client


class TestPerspectiveClientScore:
    def test_language_not_supported_raises_specific_error(self):
        """A 400 LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE response should raise
        PerspectiveLanguageNotSupportedError, not a generic HTTPStatusError,
        so callers can handle it gracefully without treating it as an API bug."""
        import asyncio
        import json

        from .perspective import PerspectiveClient

        body = json.dumps({"error": {"code": 400, "details": [{"errorType": "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE"}]}})
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 400
        mock_response.text = body
        mock_response.json.return_value = json.loads(body)
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError("400", request=MagicMock(), response=mock_response)

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        with (
            patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}),
            patch("app.lib.perspective.get_http_client", return_value=mock_client),
        ):
            with pytest.raises(PerspectiveLanguageNotSupportedError):
                asyncio.run(PerspectiveClient().score("にじほ"))

    def test_other_400_still_raises_http_error(self):
        """Non-language 400s should still propagate as HTTPStatusError."""
        import asyncio
        import json

        from .perspective import PerspectiveClient

        body = json.dumps({"error": {"code": 400, "details": [{"errorType": "SOME_OTHER_ERROR"}]}})
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 400
        mock_response.text = body
        mock_response.json.return_value = json.loads(body)
        exc = httpx.HTTPStatusError("400", request=MagicMock(), response=mock_response)
        mock_response.raise_for_status.side_effect = exc

        mock_client = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        with (
            patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}),
            patch("app.lib.perspective.get_http_client", return_value=mock_client),
        ):
            with pytest.raises(httpx.HTTPStatusError):
                asyncio.run(PerspectiveClient().score("bad request"))


class TestScoreCandidates:
    def test_empty_list_returns_empty(self):
        with patch("app.lib.perspective._get_client") as mock_get:
            import asyncio
            result = asyncio.run(score_candidates([]))
        mock_get.assert_not_called()
        assert result == {}

    def test_returns_raw_scores_keyed_by_at_uri(self):
        candidates = [
            _make_candidate("at://a/1", content="low quality"),
            _make_candidate("at://a/2", content="medium quality"),
            _make_candidate("at://a/3", content="high quality"),
        ]
        fake = _fake_client([0.1, 0.5, 0.9])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": 0.1, "at://a/2": 0.5, "at://a/3": 0.9}

    def test_zero_score_remains_valid_score(self):
        candidates = [
            _make_candidate("at://a/1", content="neutral post"),
            _make_candidate("at://a/2", content="good post"),
        ]
        fake = _fake_client([0.0, 0.8])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": 0.0, "at://a/2": 0.8}

    def test_none_content_gets_missing_score(self):
        candidates = [
            _make_candidate("at://a/1", content=None),
            _make_candidate("at://a/2", content="good post"),
        ]
        fake = _fake_client([0.8])  # only called once for the non-None post

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": None, "at://a/2": 0.8}

    def test_api_failure_gets_missing_score(self):
        candidates = [
            _make_candidate("at://a/1", content="some content"),
            _make_candidate("at://a/2", content="other content"),
        ]
        fake = MagicMock()
        fake.score = AsyncMock(side_effect=[RuntimeError("API down"), 0.7])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": None, "at://a/2": 0.7}

    def test_language_not_supported_gets_missing_score(self):
        """A LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE 400 should return None without
        logging at ERROR level — it's expected for non-English content."""
        import asyncio

        candidates = [
            _make_candidate("at://a/1", content="にじほ"),
            _make_candidate("at://a/2", content="english content"),
        ]
        fake = MagicMock()
        fake.score = AsyncMock(side_effect=[PerspectiveLanguageNotSupportedError("ja"), 0.7])

        with patch("app.lib.perspective._get_client", return_value=fake):
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": None, "at://a/2": 0.7}

    def test_rate_limit_gets_missing_score(self):
        candidates = [
            _make_candidate("at://a/1", content="some content"),
            _make_candidate("at://a/2", content="other content"),
        ]
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_exc = httpx.HTTPStatusError("429", request=MagicMock(), response=rate_limit_response)
        fake = MagicMock()
        fake.score = AsyncMock(side_effect=[rate_limit_exc, 0.7])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert result == {"at://a/1": None, "at://a/2": 0.7}

    def test_minute_quota_exhausted_returns_missing_without_api_call(self):
        candidates = [_make_candidate("at://a/1", content="text")]
        fake = _fake_client([0.9])

        perspective_module._rate_bucket_minute = int(__import__("time").time()) // 60
        perspective_module._rate_count = perspective_module._QUOTA_RPM  # bucket full

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        fake.score.assert_not_called()
        assert result == {"at://a/1": None}

    def test_all_candidates_scored_none_dropped(self):
        candidates = [_make_candidate(f"at://a/{i}", content="text") for i in range(5)]
        fake = _fake_client([0.5] * 5)

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(score_candidates(candidates))

        assert len(result) == 5

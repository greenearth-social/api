"""Tests for PRC scoring and perspective_rerank."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from ..models import CandidatePost
from . import perspective as perspective_module
from .perspective import _prc_score, perspective_rerank


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reset module-level rate limiter state between tests."""
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0
    yield
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0


def _make_candidate(uri: str, content: str | None = "text", score: float = 1.0) -> CandidatePost:
    return CandidatePost(
        at_uri=uri,
        content=content,
        score=score,
        minilm_l12_embedding=None,
        generator_name="test",
    )


# ---------------------------------------------------------------------------
# _prc_score arithmetic
# ---------------------------------------------------------------------------

def _zero_attr() -> dict[str, float]:
    return {
        "COMPASSION_EXPERIMENTAL": 0.0,
        "CURIOSITY_EXPERIMENTAL": 0.0,
        "NUANCE_EXPERIMENTAL": 0.0,
        "REASONING_EXPERIMENTAL": 0.0,
        "TOXICITY": 0.0,
        "SEVERE_TOXICITY": 0.0,
        "IDENTITY_ATTACK": 0.0,
        "INSULT": 0.0,
    }


class TestPrcScore:
    def test_all_zeros_returns_zero(self):
        assert _prc_score(_zero_attr()) == pytest.approx(0.0)

    def test_pure_bridging_returns_positive(self):
        attr = {**_zero_attr(),
                "COMPASSION_EXPERIMENTAL": 1.0, "CURIOSITY_EXPERIMENTAL": 1.0,
                "NUANCE_EXPERIMENTAL": 1.0, "REASONING_EXPERIMENTAL": 1.0}
        # bridging=1.0, toxicity=0.0 → score=1.0
        assert _prc_score(attr) == pytest.approx(1.0)

    def test_pure_toxicity_returns_negative(self):
        attr = {**_zero_attr(),
                "TOXICITY": 1.0, "SEVERE_TOXICITY": 1.0,
                "IDENTITY_ATTACK": 1.0, "INSULT": 1.0}
        # bridging=0.0, toxicity=1.0 → score = -0.5
        assert _prc_score(attr) == pytest.approx(-0.5)

    def test_known_mixed_inputs(self):
        attr = {
            "COMPASSION_EXPERIMENTAL": 0.6,
            "CURIOSITY_EXPERIMENTAL": 0.4,
            "NUANCE_EXPERIMENTAL": 0.8,
            "REASONING_EXPERIMENTAL": 0.6,
            "TOXICITY": 0.3,
            "SEVERE_TOXICITY": 0.1,
            "IDENTITY_ATTACK": 0.2,
            "INSULT": 0.4,
        }
        bridging = (0.6 + 0.4 + 0.8 + 0.6) / 4.0
        toxicity = (0.3 + 0.1 + 0.2 + 0.4) / 4.0
        assert _prc_score(attr) == pytest.approx(bridging - 0.5 * toxicity)


# ---------------------------------------------------------------------------
# perspective_rerank
# ---------------------------------------------------------------------------

def _fake_client(scores: list[float]) -> MagicMock:
    """Build a mock PerspectiveClient whose score() yields values in order."""
    client = MagicMock()
    client.score = AsyncMock(side_effect=scores)
    return client


class TestPerspectiveRerank:
    def test_empty_list_returns_empty(self):
        with patch("app.lib.perspective._get_client") as mock_get:
            import asyncio
            result = asyncio.run(perspective_rerank([]))
        mock_get.assert_not_called()
        assert result == []

    def test_sorts_by_prc_score_descending(self):
        candidates = [
            _make_candidate("at://a/1", content="low quality"),
            _make_candidate("at://a/2", content="medium quality"),
            _make_candidate("at://a/3", content="high quality"),
        ]
        fake = _fake_client([0.1, 0.5, 0.9])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(perspective_rerank(candidates))

        uris = [c.at_uri for c in result]
        assert uris == ["at://a/3", "at://a/2", "at://a/1"]

    def test_none_content_gets_neutral_score(self):
        candidates = [
            _make_candidate("at://a/1", content=None),
            _make_candidate("at://a/2", content="good post"),
        ]
        fake = _fake_client([0.8])  # only called once for the non-None post

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(perspective_rerank(candidates))

        # at://a/2 scores 0.8, at://a/1 scores 0.0 (neutral) → a/2 first
        assert result[0].at_uri == "at://a/2"
        assert result[1].at_uri == "at://a/1"

    def test_api_failure_gets_neutral_score(self):
        candidates = [
            _make_candidate("at://a/1", content="some content"),
            _make_candidate("at://a/2", content="other content"),
        ]
        fake = MagicMock()
        fake.score = AsyncMock(side_effect=[RuntimeError("API down"), 0.7])

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(perspective_rerank(candidates))

        # at://a/1 fails → 0.0, at://a/2 → 0.7 → a/2 first
        assert result[0].at_uri == "at://a/2"
        assert result[1].at_uri == "at://a/1"

    def test_rate_limit_gets_neutral_score(self):
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
            result = asyncio.run(perspective_rerank(candidates))

        # at://a/1 rate limited → 0.0, at://a/2 → 0.7 → a/2 first
        assert result[0].at_uri == "at://a/2"
        assert result[1].at_uri == "at://a/1"

    def test_minute_quota_exhausted_returns_neutral_without_api_call(self):
        candidates = [_make_candidate("at://a/1", content="text")]
        fake = _fake_client([0.9])

        perspective_module._rate_bucket_minute = int(__import__("time").time()) // 60
        perspective_module._rate_count = perspective_module._QUOTA_RPM  # bucket full

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(perspective_rerank(candidates))

        fake.score.assert_not_called()
        assert result[0].at_uri == "at://a/1"

    def test_all_candidates_returned_none_dropped(self):
        candidates = [_make_candidate(f"at://a/{i}", content="text") for i in range(5)]
        fake = _fake_client([0.5] * 5)

        with patch("app.lib.perspective._get_client", return_value=fake):
            import asyncio
            result = asyncio.run(perspective_rerank(candidates))

        assert len(result) == 5

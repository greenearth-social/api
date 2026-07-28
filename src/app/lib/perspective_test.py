"""Tests for PRC scoring and Perspective API candidate scoring."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from ..models import CandidatePost
from . import perspective as perspective_module
from .perspective import PerspectiveLanguageNotSupportedError, _prc_score, score_candidates
from .pipeline_context import DegradationStage, PipelineContext, pipeline_context_scope


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reset module-level rate limiter state between tests."""
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0
    yield
    perspective_module._rate_bucket_minute = -1
    perspective_module._rate_count = 0


# ---------------------------------------------------------------------------
# _perspective_url
# ---------------------------------------------------------------------------


class TestPerspectiveUrl:
    def test_default_host(self):
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("GE_PERSPECTIVE_HOST", None)
            assert (
                perspective_module._perspective_url()
                == "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"
            )

    def test_custom_host_override(self):
        with patch.dict("os.environ", {"GE_PERSPECTIVE_HOST": "http://127.0.0.1:8099"}):
            assert (
                perspective_module._perspective_url()
                == "http://127.0.0.1:8099/v1alpha1/comments:analyze"
            )


# ---------------------------------------------------------------------------
# _prc_score arithmetic
#
# Mirrors the `perspective_baseline_minus_outrage_toxic` weight groups from
# the PRC reference implementation: 6 positively-weighted "bridging"
# attributes at +1/6 each, and 9 negatively-weighted attributes split into
# three groups — 2 "outrage" attrs at -1/6, 3 "outrage" attrs at -1/18, and
# 4 "toxic" attrs at -1/8 — summing to raw weights of (-1.0, +1.0), then
# rescaled to final scores in [0, 1].
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
    def test_all_zeros_returns_midpoint(self):
        assert _prc_score(_zero_attr()) == pytest.approx(0.5)

    def test_pure_bridging_returns_positive(self):
        attr = {**_zero_attr(), **dict.fromkeys(_BRIDGING_ATTRS, 1.0)}
        # 6 attrs at weight 1/6 each, all at 1.0 -> raw score = 1.0 -> final score = 1.0
        assert _prc_score(attr) == pytest.approx(1.0)

    def test_pure_negative_returns_bottom_score(self):
        attr = {
            **_zero_attr(),
            **dict.fromkeys(_OUTRAGE_SIXTH_ATTRS, 1.0),
            **dict.fromkeys(_OUTRAGE_EIGHTEENTH_ATTRS, 1.0),
            **dict.fromkeys(_TOXIC_EIGHTH_ATTRS, 1.0),
        }
        # negative weights sum to -1.0 (2*(-1/6) + 3*(-1/18) + 4*(-1/8)),
        # all at 1.0 -> raw score = -1.0 -> final score = 0.0
        assert _prc_score(attr) == pytest.approx(0.0)

    def test_known_mixed_inputs(self):
        attr = {
            **dict.fromkeys(_BRIDGING_ATTRS, 0.6),
            **dict.fromkeys(_OUTRAGE_SIXTH_ATTRS, 0.3),
            **dict.fromkeys(_OUTRAGE_EIGHTEENTH_ATTRS, 0.9),
            **dict.fromkeys(_TOXIC_EIGHTH_ATTRS, 0.4),
        }
        raw_expected = (
            len(_BRIDGING_ATTRS) * (1 / 6) * 0.6
            + len(_OUTRAGE_SIXTH_ATTRS) * (-1 / 6) * 0.3
            + len(_OUTRAGE_EIGHTEENTH_ATTRS) * (-1 / 18) * 0.9
            + len(_TOXIC_EIGHTH_ATTRS) * (-1 / 8) * 0.4
        )
        expected = (raw_expected + 1.0) / 2.0
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


class _FakeResponseCM:
    """Async context manager mimicking aiohttp's ClientSession.post() return value."""

    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc_info):
        return False


class _TimeoutCM:
    """Async context manager that raises TimeoutError on entry, simulating a
    request that blew past aiohttp.ClientTimeout."""

    async def __aenter__(self):
        raise TimeoutError()

    async def __aexit__(self, *exc_info):
        return False


def _fake_response(
    status: int = 200, json_body: dict | None = None, text_body: str = ""
) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=json_body or {})
    response.text = AsyncMock(return_value=text_body)
    if status >= 400:
        response.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=status
            )
        )
    else:
        response.raise_for_status = MagicMock()
    return response


_SUCCESS_ATTR_SCORES = {name: {"summaryScore": {"value": 0.5}} for name in _ALL_PRC_ATTRS}
_SUCCESS_BODY = {"attributeScores": _SUCCESS_ATTR_SCORES}


class TestPerspectiveClientScore:
    def test_language_not_supported_raises_specific_error(self):
        """A 400 LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE response should raise
        PerspectiveLanguageNotSupportedError, not a generic ClientResponseError,
        so callers can handle it gracefully without treating it as an API bug."""
        import asyncio

        from .perspective import PerspectiveClient

        body = {
            "error": {
                "code": 400,
                "details": [
                    {
                        "errorType": "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE",
                        "languageNotSupportedByAttributeError": {"detectedLanguages": ["ja"]},
                    }
                ],
            }
        }
        response = _fake_response(status=400, json_body=body)
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponseCM(response))

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        with patch.object(PerspectiveClient, "_get_session", return_value=session):
            with pytest.raises(PerspectiveLanguageNotSupportedError):
                asyncio.run(client.score("にじほ"))

    def test_other_400_still_raises_client_response_error(self):
        """Non-language 400s should still propagate as aiohttp.ClientResponseError."""
        import asyncio

        from .perspective import PerspectiveClient

        body = {"error": {"code": 400, "details": [{"errorType": "SOME_OTHER_ERROR"}]}}
        response = _fake_response(status=400, json_body=body)
        session = MagicMock()
        session.post = MagicMock(return_value=_FakeResponseCM(response))

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        with patch.object(PerspectiveClient, "_get_session", return_value=session):
            with pytest.raises(aiohttp.ClientResponseError):
                asyncio.run(client.score("bad request"))

    def test_timeout_retries_once_then_succeeds(self):
        """A single timeout should be retried (2 total attempts), matching the
        PRC reference implementation's retry-once behavior."""
        import asyncio

        from .perspective import PerspectiveClient

        response = _fake_response(status=200, json_body=_SUCCESS_BODY)
        session = MagicMock()
        session.post = MagicMock(side_effect=[_TimeoutCM(), _FakeResponseCM(response)])

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        with patch.object(PerspectiveClient, "_get_session", return_value=session):
            score = asyncio.run(client.score("hello"))

        assert score == pytest.approx(_prc_score({name: 0.5 for name in _ALL_PRC_ATTRS}))
        assert session.post.call_count == 2

    def test_timeout_exhausts_retries_raises(self):
        """Two consecutive timeouts should give up and raise, not retry forever."""
        import asyncio

        from .perspective import PerspectiveClient

        session = MagicMock()
        session.post = MagicMock(side_effect=[_TimeoutCM(), _TimeoutCM()])

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        with patch.object(PerspectiveClient, "_get_session", return_value=session):
            with pytest.raises(TimeoutError):
                asyncio.run(client.score("hello"))

        assert session.post.call_count == 2

    def test_session_uses_unlimited_connection_pool(self):
        """The Perspective client's connector must set limit=0/limit_per_host=0
        so a burst of concurrent scoring calls can't be starved by a default
        pool size -- the root cause investigated in issue #250."""
        import asyncio

        from .perspective import PerspectiveClient

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        async def _build():
            with (
                patch("app.lib.perspective.aiohttp.TCPConnector") as mock_connector,
                patch("app.lib.perspective.aiohttp.ClientSession") as mock_session_cls,
            ):
                client._get_session()
                mock_connector.assert_called_once_with(
                    limit=0,
                    limit_per_host=0,
                    enable_cleanup_closed=True,
                    keepalive_timeout=45,
                )
                mock_session_cls.assert_called_once_with(connector=mock_connector.return_value)

        asyncio.run(_build())

    def test_close_closes_session_and_resets(self):
        import asyncio

        from .perspective import PerspectiveClient

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        fake_session = MagicMock()
        fake_session.close = AsyncMock()
        client._session = fake_session

        asyncio.run(client.close())

        fake_session.close.assert_awaited_once()
        assert client._session is None

    def test_close_is_noop_when_never_opened(self):
        import asyncio

        from .perspective import PerspectiveClient

        with patch.dict("os.environ", {"GE_PERSPECTIVE_API_KEY": "test-key"}):
            client = PerspectiveClient()

        asyncio.run(client.close())  # must not raise


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
        rate_limit_exc = aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=429
        )
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


class TestClosePerspectiveClient:
    @pytest.fixture(autouse=True)
    def _reset_client(self):
        perspective_module._client = None
        yield
        perspective_module._client = None

    def test_closes_singleton_and_resets_module_state(self):
        import asyncio

        fake_client = MagicMock()
        fake_client.close = AsyncMock()
        perspective_module._client = fake_client

        asyncio.run(perspective_module.close_perspective_client())

        fake_client.close.assert_awaited_once()
        assert perspective_module._client is None

    def test_noop_when_no_singleton_created(self):
        import asyncio

        perspective_module._client = None
        asyncio.run(perspective_module.close_perspective_client())  # must not raise


# ---------------------------------------------------------------------------
# Degradation tracking
# ---------------------------------------------------------------------------


class TestScoreCandidatesDegradation:
    """Unexpected Perspective errors record DegradationEvent; expected errors don't."""

    @pytest.fixture(autouse=True)
    def _reset_perspective_client(self, monkeypatch):
        monkeypatch.setattr(perspective_module, "_client", None)

    def _candidate(self, uri: str) -> CandidatePost:
        return _make_candidate(uri, content="some text")

    @pytest.mark.asyncio
    async def test_unexpected_http_error_records_degradation(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.score = AsyncMock(
            side_effect=aiohttp.ClientResponseError(MagicMock(), (), status=500)
        )
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            scores = await score_candidates([self._candidate("at://a/1")])

        assert scores.get("at://a/1") is None  # still soft-fails
        assert len(ctx.degradations) == 1
        assert ctx.degradations[0].stage == DegradationStage.RANK
        assert ctx.degradations[0].component == "perspective"

    @pytest.mark.asyncio
    async def test_generic_exception_records_degradation(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.score = AsyncMock(side_effect=ConnectionError("timeout"))
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            scores = await score_candidates([self._candidate("at://a/1")])

        assert scores.get("at://a/1") is None
        assert len(ctx.degradations) == 1
        assert ctx.degradations[0].stage == DegradationStage.RANK
        assert ctx.degradations[0].component == "perspective"

    @pytest.mark.asyncio
    async def test_language_not_supported_does_not_record_degradation(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.score = AsyncMock(
            side_effect=PerspectiveLanguageNotSupportedError("ja")
        )
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            scores = await score_candidates([self._candidate("at://a/1")])

        assert scores.get("at://a/1") is None
        assert ctx.degradations == []

    @pytest.mark.asyncio
    async def test_rate_limit_429_does_not_record_degradation(self, monkeypatch):
        mock_client = MagicMock()
        mock_client.score = AsyncMock(
            side_effect=aiohttp.ClientResponseError(MagicMock(), (), status=429)
        )
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            scores = await score_candidates([self._candidate("at://a/1")])

        assert scores.get("at://a/1") is None
        assert ctx.degradations == []

    @pytest.mark.asyncio
    async def test_no_context_unexpected_error_still_returns_none(self, monkeypatch):
        """Without PipelineContext, unexpected errors still return None (no crash)."""
        mock_client = MagicMock()
        mock_client.score = AsyncMock(side_effect=ConnectionError("timeout"))
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        scores = await score_candidates([self._candidate("at://a/1")])
        assert scores.get("at://a/1") is None
        # no assertion about degradations — there is no context

    @pytest.mark.asyncio
    async def test_multiple_posts_unexpected_error_records_one_event_per_post(self, monkeypatch):
        """Each failing post records its own event (may be noisy — by design for now)."""
        mock_client = MagicMock()
        mock_client.score = AsyncMock(
            side_effect=aiohttp.ClientResponseError(MagicMock(), (), status=503)
        )
        monkeypatch.setattr(perspective_module, "_client", mock_client)

        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            scores = await score_candidates(
                [self._candidate("at://a/1"), self._candidate("at://a/2")]
            )

        assert all(v is None for v in scores.values())
        assert len(ctx.degradations) == 2  # one per post — conservative tracking

"""Perspective API integration for post-ranking by conversational quality."""

from __future__ import annotations

import asyncio
import logging
import os
import time

import aiohttp

from ..models import CandidatePost
from .http_client import get_http_client
from .pipeline_context import DegradationEvent, DegradationStage, current_pipeline_context
from .telemetry import timed

logger = logging.getLogger(__name__)

_PERSPECTIVE_HOST_DEFAULT = "https://commentanalyzer.googleapis.com"
_SCORE_TIMEOUT_SECONDS = 1.0
_SCORE_ATTEMPTS = 2


def _perspective_url() -> str:
    """Perspective API endpoint URL, overridable via GE_PERSPECTIVE_HOST for local profiling."""
    host = os.environ.get("GE_PERSPECTIVE_HOST", _PERSPECTIVE_HOST_DEFAULT)
    return f"{host}/v1alpha1/comments:analyze"


class PerspectiveLanguageNotSupportedError(Exception):
    """Raised when the Perspective API rejects a request because the detected
    language is not supported by one or more requested attributes.

    This is expected for non-English content and should be handled gracefully
    by callers (e.g. mark the score missing rather than logging an error).
    """

    def __init__(self, language: str | None = None) -> None:
        self.language = language
        msg = f"language not supported: {language}" if language else "language not supported"
        super().__init__(msg)

# `perspective_baseline_minus_outrage_toxic` from the PRC reference
# implementation (PRC paper's "Uprank Bridging, Downrank Toxic" condition —
# the only one to reach statistical significance, p<0.05):
# https://github.com/HumanCompatibleAI/ranking-challenge-perspective/blob/main/perspective_ranker.py#L163-L179
#
# Verified via direct calls to the live Perspective API that every attribute
# referenced below is available (none 400). Note `SEVERE_TOXICITY` is *not*
# part of this reference formula and is intentionally omitted.
#
# Each attribute score from the Perspective API is in [0, 1], so for any
# weighted sum of attributes the theoretical score bounds are
# (sum of negative weights, sum of positive weights) — see
# `_weighted_score_bounds`. This formula's positive weights sum to 1.0
# (6 * 1/6) and negative weights sum to -1.0 (2*(-1/6) + 3*(-1/18) +
# 4*(-1/8)), giving bounds of exactly (-1.0, 1.0) — no rescaling needed
# beyond the float-precision clamp `_weighted_score_bounds` already performs
# implicitly via `_normalize`'s clamping in the rank-model pipeline.
_PRC_WEIGHTS: dict[str, float] = {
    "REASONING_EXPERIMENTAL": 1 / 6,
    "PERSONAL_STORY_EXPERIMENTAL": 1 / 6,
    "AFFINITY_EXPERIMENTAL": 1 / 6,
    "COMPASSION_EXPERIMENTAL": 1 / 6,
    "RESPECT_EXPERIMENTAL": 1 / 6,
    "CURIOSITY_EXPERIMENTAL": 1 / 6,
    "FEARMONGERING_EXPERIMENTAL": -1 / 6,
    "GENERALIZATION_EXPERIMENTAL": -1 / 6,
    "SCAPEGOATING_EXPERIMENTAL": -1 / 18,
    "MORAL_OUTRAGE_EXPERIMENTAL": -1 / 18,
    "ALIENATION_EXPERIMENTAL": -1 / 18,
    "TOXICITY": -1 / 8,
    "IDENTITY_ATTACK": -1 / 8,
    "INSULT": -1 / 8,
    "THREAT": -1 / 8,
}

_REQUESTED_ATTRIBUTES = {name: {} for name in _PRC_WEIGHTS}


def _weighted_score_bounds(weights: dict[str, float]) -> tuple[float, float]:
    """Theoretical (min, max) bounds for a weighted sum of Perspective attributes.

    Each Perspective API attribute score is in [0, 1]. For a weighted sum
    `score = sum(weight[attr] * value[attr])`, the minimum is achieved when
    every negatively-weighted attribute is at its max (1.0) and every
    positively-weighted attribute is at its min (0.0) — i.e. the sum of the
    negative weights — and the maximum is the mirror image — the sum of the
    positive weights.
    """
    lo = sum(w for w in weights.values() if w < 0)
    hi = sum(w for w in weights.values() if w > 0)
    return (lo, hi)


def _prc_score(attr: dict[str, float], weights: dict[str, float] = _PRC_WEIGHTS) -> float:
    """Score a post as a weighted sum of its Perspective attribute scores."""
    return sum(weight * attr[name] for name, weight in weights.items())


class PerspectiveClient:
    """Thin async client for the Perspective API.

    Reads GE_PERSPECTIVE_API_KEY from the environment at instantiation.
    Raises RuntimeError if the key is missing.
    """

    def __init__(self) -> None:
        key = os.environ.get("GE_PERSPECTIVE_API_KEY")
        if not key:
            raise RuntimeError("GE_PERSPECTIVE_API_KEY environment variable is not set")
        self._api_key = key
        self._session: aiohttp.ClientSession | None = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Lazily build the dedicated aiohttp session.

        Constructed lazily (not in __init__) because aiohttp.ClientSession()
        requires a running event loop, which isn't available at
        PerspectiveClient() construction time (e.g. module import, pytest
        collection). limit=0/limit_per_host=0 removes the connection-pool cap
        that caused head-of-line blocking under concurrent asyncio.gather
        scoring bursts (issue #250); keepalive_timeout=45 matches the PRC
        reference implementation.
        """
        if self._session is None:
            connector = aiohttp.TCPConnector(
                limit=0,
                limit_per_host=0,
                enable_cleanup_closed=True,
                keepalive_timeout=45,
            )
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self) -> None:
        """Close the session, if one was ever opened."""
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _handle_error_response(self, response: aiohttp.ClientResponse, content: str) -> None:
        if response.status == 400:
            try:
                body = await response.json()
                details = body.get("error", {}).get("details", [])
                if details and details[0].get("errorType") == "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE":
                    lang_error = details[0].get("languageNotSupportedByAttributeError", {})
                    detected = (lang_error.get("detectedLanguages") or [None])[0]
                    raise PerspectiveLanguageNotSupportedError(detected)
            except PerspectiveLanguageNotSupportedError:
                raise
            except Exception:
                pass
        text = await response.text()
        logger.warning(
            "Perspective API %s for content %.80r: %s",
            response.status,
            content,
            text,
        )
        response.raise_for_status()

    @staticmethod
    def _extract_score(data: dict) -> float:
        attr_scores = {
            name: data["attributeScores"][name]["summaryScore"]["value"]
            for name in _REQUESTED_ATTRIBUTES
        }
        return _prc_score(attr_scores)

    async def score(self, content: str) -> float:
        """Return the PRC score for the given text content.

        Retries once on a per-request timeout (2 total attempts), matching
        the PRC reference implementation
        (https://github.com/HumanCompatibleAI/ranking-challenge-perspective/blob/main/perspective_ranker.py#L329).
        """
        payload = {
            "comment": {"text": content},
            "requestedAttributes": _REQUESTED_ATTRIBUTES,
        }
        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=_SCORE_TIMEOUT_SECONDS)

        for attempt in range(_SCORE_ATTEMPTS):
            try:
                async with timed(logger, "perspective.score.duration_ms", record_metric=True):
                    async with session.post(
                        _perspective_url(),
                        params={"key": self._api_key},
                        json=payload,
                        timeout=timeout,
                    ) as response:
                        if response.status != 200:
                            await self._handle_error_response(response, content)
                        data = await response.json()
                return self._extract_score(data)
            except TimeoutError:
                if attempt == _SCORE_ATTEMPTS - 1:
                    raise
                logger.warning(
                    "Perspective API timeout (%ss) scoring content %.80r; retrying",
                    _SCORE_TIMEOUT_SECONDS,
                    content,
                )

        raise AssertionError("unreachable: loop above always returns or raises")


# Client-side rate limiter tracking usage within the current calendar-minute
# bucket, matching how the Perspective API measures its 600 QPS quota.
# Set to 500 QPS (30 000 RPM) to keep a safety margin.
_QUOTA_QPS = 500
_QUOTA_RPM = _QUOTA_QPS * 60
_rate_lock = asyncio.Lock()
_rate_bucket_minute: int = -1
_rate_count: int = 0


async def _rate_limit_acquire() -> bool:
    """Return True if a request is allowed, False if the minute quota is exhausted."""
    global _rate_bucket_minute, _rate_count
    async with _rate_lock:
        current_minute = int(time.time()) // 60
        if current_minute != _rate_bucket_minute:
            _rate_bucket_minute = current_minute
            _rate_count = 0
        if _rate_count >= _QUOTA_RPM:
            return False
        _rate_count += 1
        return True

_client: PerspectiveClient | None = None


def _get_client() -> PerspectiveClient:
    global _client
    if _client is None:
        _client = PerspectiveClient()
    return _client


async def close_perspective_client() -> None:
    """Close the singleton PerspectiveClient's aiohttp session, if one was created."""
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def score_candidates(candidates: list[CandidatePost]) -> dict[str, float | None]:
    """Return PRC scores for *candidates*, keyed by ``at_uri``.

    Posts with content=None, where the minute quota is exhausted, or where the
    API call fails receive a missing score of None. Every candidate with an
    ``at_uri`` is returned — none are dropped.
    """
    if not candidates:
        return {}

    client = _get_client()

    async def _score_one(c: CandidatePost) -> float | None:
        if not c.content or not c.content.strip():
            return None
        if not await _rate_limit_acquire():
            logger.warning("Perspective API minute quota exhausted; using missing score for post %s", c.at_uri)
            return None
        try:
            return await client.score(c.content)
        except PerspectiveLanguageNotSupportedError as exc:
            logger.debug(
                "Perspective API: language not supported (%s) for post %s; using missing score",
                exc.language,
                c.at_uri,
            )
            return None  # expected — not a degradation
        except aiohttp.ClientResponseError as exc:
            if exc.status == 429:
                logger.warning(
                    "Perspective API rate limited for post %s; using missing score", c.at_uri
                )
                return None  # expected rate-limit — not a degradation
            logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            ctx = current_pipeline_context()
            if ctx is not None:
                ctx.record(DegradationEvent(
                    stage=DegradationStage.RANK,
                    component="perspective",
                    cause=exc,
                ))
            return None
        except Exception as exc:
            logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            ctx = current_pipeline_context()
            if ctx is not None:
                ctx.record(DegradationEvent(
                    stage=DegradationStage.RANK,
                    component="perspective",
                    cause=exc,
                ))
            return None

    scorable = [c for c in candidates if c.at_uri]
    scores = await asyncio.gather(*(_score_one(c) for c in scorable))
    return {
        c.at_uri: score
        for c, score in zip(scorable, scores, strict=True)
        if c.at_uri is not None
    }

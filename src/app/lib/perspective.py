"""Perspective API integration for post-ranking by conversational quality."""

from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx

from ..models import CandidatePost
from .http_client import get_http_client
from .telemetry import timed

logger = logging.getLogger(__name__)

_PERSPECTIVE_URL = "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"


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

    async def score(self, content: str) -> float:
        """Return the PRC score for the given text content."""
        payload = {
            "comment": {"text": content},
            "requestedAttributes": _REQUESTED_ATTRIBUTES,
        }
        client = get_http_client()
        async with timed(logger, "perspective.score.duration_ms", record_metric=True):
            response = await client.post(
                _PERSPECTIVE_URL,
                params={"key": self._api_key},
                json=payload,
            )
        if not response.is_success:
            if response.status_code == 400:
                try:
                    details = response.json().get("error", {}).get("details", [])
                    if details and details[0].get("errorType") == "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE":
                        lang_error = details[0].get("languageNotSupportedByAttributeError", {})
                        detected = (lang_error.get("detectedLanguages") or [None])[0]
                        raise PerspectiveLanguageNotSupportedError(detected)
                except PerspectiveLanguageNotSupportedError:
                    raise
                except Exception:
                    pass
            logger.warning(
                "Perspective API %s for content %.80r: %s",
                response.status_code,
                content,
                response.text,
            )
            response.raise_for_status()
        data = response.json()
        attr_scores = {
            name: data["attributeScores"][name]["summaryScore"]["value"]
            for name in _REQUESTED_ATTRIBUTES
        }
        return _prc_score(attr_scores)


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
            return None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                logger.warning("Perspective API rate limited for post %s; using missing score", c.at_uri)
            else:
                logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            return None
        except Exception:
            logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            return None

    scorable = [c for c in candidates if c.at_uri]
    scores = await asyncio.gather(*(_score_one(c) for c in scorable))
    return {
        c.at_uri: score
        for c, score in zip(scorable, scores, strict=True)
        if c.at_uri is not None
    }

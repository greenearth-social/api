"""Perspective API integration for post-ranking by conversational quality."""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

from ..models import CandidatePost
from .http_client import get_http_client

logger = logging.getLogger(__name__)

_PERSPECTIVE_URL = "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"

_REQUESTED_ATTRIBUTES = {
    "COMPASSION_EXPERIMENTAL": {},
    "CURIOSITY_EXPERIMENTAL": {},
    "NUANCE_EXPERIMENTAL": {},
    "REASONING_EXPERIMENTAL": {},
    "TOXICITY": {},
    "SEVERE_TOXICITY": {},
    "IDENTITY_ATTACK": {},
    "INSULT": {},
}


def _prc_score(attr: dict[str, float]) -> float:
    """Score a post using a public-API approximation of the PRC paper formula.

    The paper's "Uprank Bridging, Downrank Toxic" condition was the only one
    to reach statistical significance (p<0.05). The paper's negative attributes
    (MORAL_OUTRAGE, SCAPEGOATING, ALIENATION, PERSUASION) are not in the public
    API; they were grouped together due to strong mutual correlation, so
    TOXICITY/SEVERE_TOXICITY serve as equivalent proxies.

    Formula: bridging - 0.5 * toxicity
        bridging = avg(COMPASSION_EXPERIMENTAL, CURIOSITY_EXPERIMENTAL,
                       NUANCE_EXPERIMENTAL, REASONING_EXPERIMENTAL)
        toxicity = avg(TOXICITY, SEVERE_TOXICITY, IDENTITY_ATTACK, INSULT)
    """
    bridging = (
        attr["COMPASSION_EXPERIMENTAL"] + attr["CURIOSITY_EXPERIMENTAL"]
        + attr["NUANCE_EXPERIMENTAL"] + attr["REASONING_EXPERIMENTAL"]
    ) / 4.0
    toxicity = (
        attr["TOXICITY"] + attr["SEVERE_TOXICITY"]
        + attr["IDENTITY_ATTACK"] + attr["INSULT"]
    ) / 4.0
    return bridging - 0.5 * toxicity


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
        response = await client.post(
            _PERSPECTIVE_URL,
            params={"key": self._api_key},
            json=payload,
        )
        if not response.is_success:
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


# Limit concurrent Perspective API calls to stay well under the 600 QPS quota.
_SEMAPHORE = asyncio.Semaphore(50)

_client: PerspectiveClient | None = None


def _get_client() -> PerspectiveClient:
    global _client
    if _client is None:
        _client = PerspectiveClient()
    return _client


async def perspective_rerank(candidates: list[CandidatePost]) -> list[CandidatePost]:
    """Re-rank candidates by PRC score (descending).

    Posts with content=None or where the API call fails receive a neutral score
    of 0.0.  All candidates are returned — none are dropped.
    """
    if not candidates:
        return []

    client = _get_client()

    async def _score_one(c: CandidatePost) -> float:
        if not c.content or not c.content.strip():
            return 0.0
        try:
            async with _SEMAPHORE:
                return await client.score(c.content)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                logger.warning("Perspective API rate limited for post %s; using neutral score", c.at_uri)
            else:
                logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            return 0.0
        except Exception:
            logger.exception("Perspective API scoring failed for post %s", c.at_uri)
            return 0.0

    scores = await asyncio.gather(*(_score_one(c) for c in candidates))
    paired = sorted(zip(scores, candidates, strict=True), key=lambda x: x[0], reverse=True)
    return [c for _, c in paired]

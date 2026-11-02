"""LLM filter (#566): score every candidate against the user's filter prompt.

The filter vector is a ridge fit from lib/llm_query_vector_fit.py: for a post
with unit MiniLM embedding e, ``e @ q + intercept`` predicts the 1-10 verdict
the model would give it. Mapped to [0, 1] it plays the part the topic score
plays for the politics multiplier.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import numpy as np

from ...documents import LlmQueryVectorDocument
from ...models import CandidatePost
from ..embeddings import decode_float32_b64
from ..llm_query_vector_fit import MAX_SCORE, MIN_SCORE

logger = logging.getLogger(__name__)


def llm_filter_scores(
    candidates: Sequence[CandidatePost], query_vector: Sequence[float], intercept: float
) -> dict[str, float]:
    """Filter score in [0, 1] per AT URI; candidates without a usable embedding are left out."""
    q = np.asarray(query_vector, dtype=np.float64)
    uris: list[str] = []
    rows: list[list[float]] = []
    for c in candidates:
        if not c.at_uri or not c.minilm_l12_embedding:
            continue
        try:
            embedding = decode_float32_b64(c.minilm_l12_embedding)
        except Exception:
            continue
        if len(embedding) == q.shape[0]:
            uris.append(c.at_uri)
            rows.append(embedding)
    if not rows:
        return {}
    E = np.asarray(rows, dtype=np.float64)
    norms = np.linalg.norm(E, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    verdicts = (E / norms) @ q + intercept
    unit = np.clip((verdicts - MIN_SCORE) / (MAX_SCORE - MIN_SCORE), 0.0, 1.0)
    return dict(zip(uris, unit.tolist(), strict=True))


def llm_filter_multiplier(setting: float, filter_score: float) -> float:
    """Same shape as the politics multiplier: 1.0 at score 0, ``setting`` at score 1."""
    return (setting - 1.0) * filter_score + 1.0


async def fetch_active_llm_filter(user_did: str) -> LlmQueryVectorDocument | None:
    """The user's active filter vector, or None. Fails open: the filter never breaks ranking."""
    # Imported here to avoid the documents -> candidates -> llm_prompt_state -> documents cycle.
    from ..candidates.llm_query_vector import get_llm_query_vector_db
    from ..llm_prompt_state import get_current_llm_query_vector

    db = get_llm_query_vector_db()
    if db is None:
        return None
    try:
        return await get_current_llm_query_vector(db, user_did, role="filter")
    except Exception:
        logger.exception("llm_filter: could not read the active filter vector; ranking unfiltered")
        return None

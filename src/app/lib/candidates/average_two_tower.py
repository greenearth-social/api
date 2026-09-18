"""Experimental two-tower retrieval using a fixed, offline average user vector."""

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from uuid import UUID

from ...models import MaxAgeHours
from ..elasticsearch import POSTS_QUALITY_KNN_INDEX, two_tower_knn_index
from ..embeddings import GE_POST_EMBEDDING_FIELD
from .base import CandidateGenerator, CandidateResult
from .es_candidates import knn_search_posts
from .two_tower import MIN_LIKE_COUNT

logger = logging.getLogger(__name__)

AVERAGE_EMBEDDING_PATH = Path(__file__).with_name("data") / "average_user_embedding.json"


@dataclass(frozen=True)
class AverageUserEmbedding:
    embedding: tuple[float, ...]
    dimension: int
    user_model_uuid: str
    post_model_uuid: str
    source_completed_at: str
    contributing_users: int


@lru_cache(maxsize=1)
def load_average_embedding() -> AverageUserEmbedding:
    """Validate once per process; restart the API when replacing the bundled asset.

    The model pair is pinned together. The average's user-model UUID cannot be
    used as the Elasticsearch post-model filter, and a newer live post model
    cannot safely replace its paired model.
    """
    with AVERAGE_EMBEDDING_PATH.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("average_two_tower asset must be a JSON object")

    dimension = payload.get("dimension")
    if type(dimension) is not int or dimension != 128:
        raise ValueError("average_two_tower dimension must be 128 for the pinned model pair")
    vector = payload.get("embedding")
    if not isinstance(vector, list) or len(vector) != dimension:
        raise ValueError("average_two_tower embedding must contain exactly 128 coordinates")
    try:
        finite = all(type(value) in (int, float) and math.isfinite(value) for value in vector)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError("average_two_tower coordinates must be finite numbers, not booleans")
    if not any(value != 0 for value in vector):
        raise ValueError("average_two_tower embedding must have nonzero magnitude")

    model_uuids = {}
    for key in ("user_model_uuid", "post_model_uuid"):
        value = payload.get(key)
        try:
            if not isinstance(value, str):
                raise ValueError
            UUID(value)
        except ValueError:
            raise ValueError(f"average_two_tower {key} must be a valid model UUID") from None
        model_uuids[key] = value

    contributing_users = payload.get("contributing_users")
    if type(contributing_users) is not int or contributing_users <= 0:
        raise ValueError("average_two_tower contributing_users must be a positive integer")
    completed_at = payload.get("source_completed_at")
    try:
        if not isinstance(completed_at, str):
            raise ValueError
        timestamp = datetime.fromisoformat(completed_at)
        if timestamp.utcoffset() != timedelta(0):
            raise ValueError
    except ValueError:
        raise ValueError("average_two_tower source_completed_at must be a UTC timestamp") from None

    result = AverageUserEmbedding(
        embedding=tuple(vector),
        dimension=dimension,
        user_model_uuid=model_uuids["user_model_uuid"],
        post_model_uuid=model_uuids["post_model_uuid"],
        source_completed_at=completed_at,
        contributing_users=contributing_users,
    )
    logger.info(
        "Loaded average_two_tower asset: user_model_uuid=%s post_model_uuid=%s "
        "dimension=%d contributing_users=%d source_completed_at=%s",
        result.user_model_uuid,
        result.post_model_uuid,
        result.dimension,
        result.contributing_users,
        result.source_completed_at,
    )
    return result


class AverageTwoTowerCandidateGenerator(CandidateGenerator):
    """Search the paired post tower's vectors without user history or inference."""

    @property
    def name(self) -> str:
        return "average_two_tower"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
        max_age_hours: MaxAgeHours = 168,
    ) -> CandidateResult:
        average = load_average_embedding()
        resolved_index = two_tower_knn_index()
        min_like_count = None if resolved_index == POSTS_QUALITY_KNN_INDEX else MIN_LIKE_COUNT
        candidates = await knn_search_posts(
            es,
            list(average.embedding),
            num_candidates,
            search_field=GE_POST_EMBEDDING_FIELD,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
            ge_post_embedding_model_uuid=average.post_model_uuid,
            min_like_count=min_like_count,
            max_age_hours=max_age_hours,
            index=resolved_index,
        )
        logger.info(
            "average_two_tower retrieval: user_did=%s index=%s post_model_uuid=%s "
            "requested=%d returned=%d",
            user_did,
            resolved_index,
            average.post_model_uuid,
            num_candidates,
            len(candidates),
        )
        return CandidateResult(
            generator_name=self.name,
            candidates=candidates,
            reason=None if candidates else "no_recent_average_embedding_posts",
        )

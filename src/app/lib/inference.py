"""Shared Inference Service utilities.

Used for calling the engagement prediction models: the user and post
towers of the two tower model, etc.
"""

import asyncio
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

import httpx

from .http_client import get_http_client
from .request_context import get_request_id
from .telemetry import timed
from .user_history_cache import UserHistory

logger = logging.getLogger(__name__)


def get_metric_collector():
    """Indirection point so tests can monkeypatch at module level."""
    from .metrics import get_metric_collector as _get
    return _get()


def _status_code_label(exc: BaseException) -> str:
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    if isinstance(exc, (httpx.ConnectError, httpx.NetworkError, httpx.RemoteProtocolError)):
        return "connection"
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return str(status) if status else "other"


def _count_failure(metric: str, exc: BaseException) -> None:
    collector = get_metric_collector()
    if collector is not None:
        collector.record(metric, 1, status_code=_status_code_label(exc))

# Keep the post-tower UUID fresh, but allow transient /ready failures to use
# the last known UUID briefly instead of disabling two-tower candidates.
_POST_TOWER_UUID_TTL_SEC = 300
_POST_TOWER_UUID_STALE_GRACE_SEC = 3600
_post_tower_uuid_cache: dict[tuple[str, str], tuple[str, float]] = {}
_post_tower_uuid_locks: dict[tuple[str, str], asyncio.Lock] = {}

HistoryMode = Literal["actual", "empty"]

class InferenceResponseFormatError(RuntimeError):
    """Raised when inference-service returns a successful but malformed response."""


class InferenceModelMetadataError(InferenceResponseFormatError):
    """The deployed inference service does not provide an authoritative model pair."""


@dataclass(frozen=True)
class UserEmbeddingResult:
    history_like_count: int
    history_embedding_count: int
    embedding: list[float] | None = None
    user_model_uuid: str | None = None
    post_model_uuid: str | None = None
    reason: Literal["no_likes", "no_embedded_history"] | None = None


def build_inference_headers(api_key: str) -> dict[str, str]:
    """Outbound headers for inference HTTP calls.

    Includes the current request ID (when set) so the inference service
    can log it alongside our own logs for cross-service correlation.
    """
    headers = {"X-API-Key": api_key}
    rid = get_request_id()
    if rid is not None:
        headers["x-request-id"] = rid
    return headers


def get_inference_settings() -> tuple[str, str]:
    """Load inference configuration"""
    base_url = os.environ.get("GE_INFERENCE_BASE_URL", "").rstrip("/")
    if not base_url:
        raise RuntimeError("GE_INFERENCE_BASE_URL environment variable is required")

    api_key = os.environ.get("GE_INFERENCE_API_KEY")
    if not api_key:
        raise RuntimeError("GE_INFERENCE_API_KEY environment variable is required")

    return base_url, api_key


def raise_inference_response_error(
    source_name: str,
    status_code: int,
    body: str
) -> None:
    body = body.strip()
    if len(body) > 2000:
        body = f"{body[:2000]}..."
    raise RuntimeError(
        f"{source_name} inference failed status={status_code} body={body}",
    )


def _decode_inference_json(source_name: str, resp) -> object:
    try:
        return resp.json()
    except ValueError as exc:
        raise InferenceResponseFormatError(
            f"{source_name} inference response was not valid JSON",
        ) from exc


def _extract_inference_outputs(
    source_name: str,
    payload: object,
) -> list:
    if not isinstance(payload, dict):
        raise InferenceResponseFormatError(
            f"{source_name} inference response was not an object",
        )
    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        raise InferenceResponseFormatError(
            f"{source_name} inference response missing outputs list",
        )
    return outputs


def _extract_post_tower_uuid_from_ready(payload: object) -> str | None:
    if not isinstance(payload, dict):
        raise InferenceResponseFormatError("ready response was not an object")

    models = payload.get("models")
    if not isinstance(models, list):
        raise InferenceResponseFormatError("ready response missing models list")

    for idx, model_dict in enumerate(models):
        if not isinstance(model_dict, dict):
            raise InferenceResponseFormatError(
                f"ready response model entry {idx} was not an object",
            )

        model_type = model_dict.get("type")
        if not isinstance(model_type, str):
            raise InferenceResponseFormatError(
                f"ready response model entry {idx} missing string type",
            )
        if model_type != "post-tower":
            continue

        # A missing post-tower entry means "not configured"; a post-tower entry
        # without a UUID means the /ready contract is broken.
        post_tower_uuid = model_dict.get("model_uuid")
        if not isinstance(post_tower_uuid, str) or not post_tower_uuid:
            raise InferenceResponseFormatError(
                "ready response post-tower model missing model_uuid",
            )
        return post_tower_uuid

    return None


async def predict_user_tower_single(
    history_embeddings: list[list[float]],
    history_author_dids: list[str],
    *,
    base_url: str,
    api_key: str,
) -> object:
    """Request one user prediction, retaining outputs and model metadata."""
    url = f"{base_url}/models/user-tower/predict"
    headers = build_inference_headers(api_key)
    payload = {
        "history_embeddings": history_embeddings,
        "history_author_dids": history_author_dids,
    }

    client = get_http_client()
    async with timed(logger, "user_tower_http", n_history=len(history_embeddings)):
        try:
            resp = await client.post(url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            _count_failure("rank.model.failure_count", exc)
            raise
    if resp.is_error:
        logger.error(
            "user-tower predict failed status=%s body=%s",
            resp.status_code,
            resp.text,
        )
        collector = get_metric_collector()
        if collector is not None:
            collector.record("rank.model.failure_count", 1, status_code=str(resp.status_code))
        raise_inference_response_error("user-tower", resp.status_code, resp.text)
    return _decode_inference_json("user-tower", resp)


def _model_uuid(payload: dict, field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not re.fullmatch(
        r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})", value
    ):
        raise InferenceModelMetadataError(f"User-tower response missing valid {field}")
    model_uuid = UUID(value)
    if model_uuid.int == 0:
        raise InferenceModelMetadataError(f"User-tower response missing valid {field}")
    return model_uuid.hex


async def predict_user_embedding(
    history: UserHistory,
    *,
    base_url: str,
    api_key: str,
) -> UserEmbeddingResult:
    """Export an actual-history prediction and its authoritative model pair.

    The caller supplies history from the shared production history loader. This path
    never synthesizes empty history and never reads the readiness UUID cache.
    """
    embedded_history = history.items_with_embeddings
    like_count = len(history.items)
    embedding_count = len(embedded_history)
    if not history.items:
        return UserEmbeddingResult(like_count, embedding_count, reason="no_likes")
    if not embedded_history:
        return UserEmbeddingResult(like_count, embedding_count, reason="no_embedded_history")
    payload = await predict_user_tower_single(
        [item.embedding for item in embedded_history if item.embedding is not None],
        [item.author_did for item in embedded_history],
        base_url=base_url,
        api_key=api_key,
    )
    outputs = _extract_inference_outputs("user-tower", payload)
    if not isinstance(payload, dict) or payload.get("model_type") != "user-tower":
        raise InferenceResponseFormatError("Expected user-tower prediction metadata")
    user_model_uuid = _model_uuid(payload, "model_uuid")
    post_model_uuid = _model_uuid(payload, "paired_post_model_uuid")
    if len(outputs) != 1 or not isinstance(outputs[0], list) or not outputs[0]:
        raise InferenceResponseFormatError("Expected exactly one nonempty embedding")
    vector = outputs[0]
    try:
        valid = all(type(value) in (int, float) and math.isfinite(value) for value in vector)
    except OverflowError:
        valid = False
    if not valid:
        raise InferenceResponseFormatError("Expected finite numeric embedding coordinates")
    return UserEmbeddingResult(
        history_like_count=like_count,
        history_embedding_count=embedding_count,
        embedding=vector,
        user_model_uuid=user_model_uuid,
        post_model_uuid=post_model_uuid,
    )


async def predict_heavy_ranker_single_user(
    history_embeddings: list[list[float]],
    history_author_dids: list[str],
    history_liked_at_times: list[str],
    history_like_counts: list[int],
    candidate_post_embeddings: list[list[float]],
    candidate_author_dids: list[str],
    candidate_like_counts: list[int],
    *,
    base_url: str,
    api_key: str,
) -> list[float]:
    url = f"{base_url}/models/ranker/predict"
    headers = build_inference_headers(api_key)
    payload = {
        "history_embeddings": history_embeddings,
        "history_author_dids": history_author_dids,
        "history_liked_at_times": history_liked_at_times,
        "history_prior_cumulative_likes": history_like_counts,
        "candidate_post_embeddings": candidate_post_embeddings,
        "candidate_author_dids": candidate_author_dids,
        "candidate_prior_cumulative_likes": candidate_like_counts,
    }

    client = get_http_client()
    async with timed(
        logger,
        "ranker_predict_http",
        n_history=len(history_embeddings),
        n_candidates=len(candidate_post_embeddings)
    ):
        try:
            resp = await client.post(url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            _count_failure("rank.model.failure_count", exc)
            raise
    if resp.is_error:
        logger.error(
            "ranker predict failed status=%s body=%s",
            resp.status_code,
            resp.text,
        )
        collector = get_metric_collector()
        if collector is not None:
            collector.record("rank.model.failure_count", 1, status_code=str(resp.status_code))
        raise_inference_response_error("ranker", resp.status_code, resp.text)
    payload = _decode_inference_json("ranker", resp)
    return _extract_inference_outputs("ranker", payload)


async def get_post_tower_uuid(
    base_url: str,
    api_key: str,
) -> str | None:
    url = f"{base_url}/ready"
    headers = build_inference_headers(api_key)

    client = get_http_client()
    resp = await client.get(url, headers=headers)
    if resp.is_error:
        logger.error(
            "get post tower uuid from inference-service failed; status=%s body=%s",
            resp.status_code,
            resp.text,
        )
        raise_inference_response_error("ready", resp.status_code, resp.text)

    payload = _decode_inference_json("ready", resp)
    return _extract_post_tower_uuid_from_ready(payload)


async def get_cached_post_tower_uuid(
    base_url: str,
    api_key: str,
) -> str | None:
    key = (base_url, api_key)
    now = time.monotonic()
    cached = _post_tower_uuid_cache.get(key)
    if cached is not None:
        post_tower_uuid, expires_at = cached
        if now < expires_at:
            return post_tower_uuid

    lock = _post_tower_uuid_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = _post_tower_uuid_cache.get(key)
        stale_post_tower_uuid = None
        if cached is not None:
            post_tower_uuid, expires_at = cached
            if now < expires_at:
                return post_tower_uuid
            if now < expires_at + _POST_TOWER_UUID_STALE_GRACE_SEC:
                stale_post_tower_uuid = post_tower_uuid

        # Only refresh errors use the stale UUID. A successful /ready response
        # with no post-tower should return None so callers stop using old UUIDs.
        try:
            post_tower_uuid = await get_post_tower_uuid(base_url, api_key)
        except Exception:
            if stale_post_tower_uuid is not None:
                logger.warning(
                    "Using stale post tower UUID after ready refresh failed",
                    exc_info=True,
                )
                return stale_post_tower_uuid
            raise
        if post_tower_uuid:
            _post_tower_uuid_cache[key] = (
                post_tower_uuid,
                time.monotonic() + _POST_TOWER_UUID_TTL_SEC,
            )
            return post_tower_uuid
        return post_tower_uuid

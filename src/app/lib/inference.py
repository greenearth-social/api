"""Shared Inference Service utilities.

Used for calling the engagement prediction models: the user and post
towers of the two tower model, etc.
"""

import asyncio
import logging
import os
import time

from .elasticsearch import fetch_recent_liked_post_uris, fetch_post_embeddings_and_authors
from .feed_debug import current_recorder
from .telemetry import timed
from .request_context import get_request_id
from .http_client import get_http_client

logger = logging.getLogger(__name__)

# Keep the post-tower UUID fresh, but allow transient /ready failures to use
# the last known UUID briefly instead of disabling two-tower candidates.
_POST_TOWER_UUID_TTL_SEC = 300
_POST_TOWER_UUID_STALE_GRACE_SEC = 3600
_post_tower_uuid_cache: dict[tuple[str, str], tuple[str, float]] = {}
_post_tower_uuid_locks: dict[tuple[str, str], asyncio.Lock] = {}


class InferenceResponseFormatError(RuntimeError):
    """Raised when inference-service returns a successful but malformed response."""


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
) -> list[list[float]]:
    url = f"{base_url}/models/user-tower/predict"
    headers = build_inference_headers(api_key)
    payload = {
        "history_embeddings": history_embeddings,
        "history_author_dids": history_author_dids,
    }

    client = get_http_client()
    async with timed(logger, "user_tower_http", n_history=len(history_embeddings)):
        resp = await client.post(url, json=payload, headers=headers)
    if resp.is_error:
        logger.error(
            "user-tower predict failed status=%s body=%s",
            resp.status_code,
            resp.text,
        )
        raise_inference_response_error("user-tower", resp.status_code, resp.text)
    payload = _decode_inference_json("user-tower", resp)
    return _extract_inference_outputs("user-tower", payload)


async def predict_heavy_ranker_single_user(
    history_embeddings: list[list[float]],
    history_author_dids: list[str],
    history_liked_at_times: list[str],
    candidate_post_embeddings: list[list[float]],
    candidate_author_dids: list[str],
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
        "candidate_post_embeddings": candidate_post_embeddings,
        "candidate_author_dids": candidate_author_dids,
    }

    client = get_http_client()
    async with timed(
        logger,
        "ranker_predict_http",
        n_history=len(history_embeddings),
        n_candidates=len(candidate_post_embeddings)
    ):
        resp = await client.post(url, json=payload, headers=headers)
    if resp.is_error:
        logger.error(
            "ranker predict failed status=%s body=%s",
            resp.status_code,
            resp.text,
        )
        raise_inference_response_error("ranker", resp.status_code, resp.text)
    payload = _decode_inference_json("ranker", resp)
    return _extract_inference_outputs("ranker", payload)


async def compute_user_embedding(
    user_did: str,
    es,
    inference_base_url: str,
    inference_api_key: str,
    source: str,
) -> list[float]:
    async with timed(logger, "two_tower_user_side", user_did=user_did):
        user_history_vectors: list[list[float]] = []
        history_author_dids: list[str] = []
        user_history_liked_uris = await fetch_recent_liked_post_uris(es, user_did)

        rec = current_recorder()

        if not user_history_liked_uris:
            logger.info("No likes found for user %s", user_did)
            if rec is not None:
                rec.record_user_features(source, [], 0)
        else:
            user_history_embedding_pairs: list[tuple[str, list[float], str]] = await fetch_post_embeddings_and_authors(
                es, user_history_liked_uris,
            )
            if rec is not None:
                rec.record_user_features(
                    source, user_history_liked_uris, len(user_history_embedding_pairs)
                )
            if not user_history_embedding_pairs:
                logger.info(
                    "No embeddings found for %d liked posts of user %s",
                    len(user_history_liked_uris),
                    user_did,
                )
            else:
                user_history_vectors = [embedding for _, embedding, _ in user_history_embedding_pairs]
                history_author_dids = [author_did for _, _, author_did in user_history_embedding_pairs]

        output_user_embedding_list = await predict_user_tower_single(
            user_history_vectors,
            history_author_dids,
            base_url=inference_base_url,
            api_key=inference_api_key,
        )
        if len(output_user_embedding_list) != 1:
            raise RuntimeError(
                f"user inference returned {len(output_user_embedding_list)} embeddings; expected 1",
            )
        return output_user_embedding_list[0]


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

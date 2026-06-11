"""Shared Inference Service utilities.

Used for calling the engagement prediction models: the user and post
towers of the two tower model, etc.
"""

import logging
import os

from .elasticsearch import fetch_recent_liked_post_uris, fetch_post_embeddings_and_authors
from .feed_debug import current_recorder
from .telemetry import timed
from .request_context import get_request_id
from .http_client import get_http_client

logger = logging.getLogger(__name__)


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
    model_type: str,
    status_code: int,
    body: str
) -> None:
    body = body.strip()
    if len(body) > 2000:
        body = f"{body[:2000]}..."
    raise RuntimeError(
        f"{model_type} inference failed status={status_code} body={body}",
    )


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
    return resp.json()["outputs"]


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

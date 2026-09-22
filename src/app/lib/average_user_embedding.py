"""Load the optional average-user prior once, outside the feed request path."""

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from src.average_user_embedding_artifact import (
    ArtifactValidationError,
    load_artifact,
    parse_artifact,
)

from .embeddings import MINILM_L12_EMBEDDING_KEY
from .user_history_cache import USER_HISTORY_LIMIT

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AverageUserEmbedding:
    embedding: tuple[float, ...]
    dimension: int
    user_model_uuid: str
    post_model_uuid: str
    run_id: str
    contributing_users: int


_average_user_embedding: AverageUserEmbedding | None = None
_average_user_embedding_error: str | None = "not_configured"


def get_average_user_embedding() -> AverageUserEmbedding | None:
    return _average_user_embedding


def get_average_user_embedding_error() -> str | None:
    return _average_user_embedding_error


def set_average_user_embedding(
    value: AverageUserEmbedding | None, error: str | None = None
) -> None:
    global _average_user_embedding, _average_user_embedding_error
    _average_user_embedding = value
    _average_user_embedding_error = error


def _load_average_user_embedding(uri: str) -> AverageUserEmbedding:
    parts = urlsplit(uri)
    if parts.scheme == "gs":
        if (
            not parts.netloc
            or not parts.path.strip("/")
            or parts.username is not None
            or ":" in parts.netloc
            or parts.query
            or parts.fragment
        ):
            raise ArtifactValidationError("Expected an exact gs://bucket/object URI")
        # ADC and network work run in the startup worker thread. Neither the
        # client nor the downloaded bytes are retained in the request path.
        from google.cloud import storage

        with storage.Client() as client:
            blob = client.bucket(parts.netloc).blob(parts.path[1:])
            # The SDK accepts None to disable retries despite its narrower annotation.
            data = blob.download_as_bytes(timeout=30, retry=None)  # type: ignore[arg-type]
        artifact = parse_artifact(data)
    elif parts.scheme:
        raise ArtifactValidationError("Expected a local file path or gs://bucket/object URI")
    else:
        artifact, _ = load_artifact(Path(uri).expanduser())

    policy = artifact["history_policy"]
    if (
        policy["limit"] != USER_HISTORY_LIMIT
        or set(policy["sources"]) != {"posts", "replies"}
        or policy["embedding_key"] != MINILM_L12_EMBEDDING_KEY
    ):
        raise ArtifactValidationError(
            "Artifact history policy does not match the production loader"
        )

    return AverageUserEmbedding(
        embedding=tuple(artifact["embedding"]),
        dimension=artifact["dimension"],
        user_model_uuid=artifact["user_model_uuid"],
        post_model_uuid=artifact["post_model_uuid"],
        run_id=artifact["run_id"],
        contributing_users=artifact["contributing_users"],
    )


async def init_average_user_embedding() -> None:
    """Called once by the API lifespan; failures preserve actual-user retrieval."""
    uri = os.environ.get("GE_AVERAGE_USER_EMBEDDING_URI", "").strip()
    set_average_user_embedding(None, "not_configured")
    if not uri:
        logger.info("Average user embedding is not configured; using actual-user retrieval")
        return

    try:
        prior = await asyncio.to_thread(_load_average_user_embedding, uri)
    except Exception as exc:
        set_average_user_embedding(None, "load_failed")
        # Only our validation exceptions have controlled messages. Cloud SDK
        # exceptions or malformed URIs can contain credentials; omit their text.
        detail = str(exc) if isinstance(exc, ArtifactValidationError) else type(exc).__name__
        logger.warning(
            "Average user embedding could not be loaded; using actual-user retrieval: %s", detail
        )
        return

    set_average_user_embedding(prior)
    logger.info(
        "Loaded average user embedding uri=%s run_id=%s user_model_uuid=%s "
        "post_model_uuid=%s dimension=%d contributing_users=%d",
        uri,
        prior.run_id,
        prior.user_model_uuid,
        prior.post_model_uuid,
        prior.dimension,
        prior.contributing_users,
    )

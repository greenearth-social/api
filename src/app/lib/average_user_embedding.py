"""Load the optional average-user prior once, outside the feed request path."""

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .average_user_embedding_artifact import (
    ArtifactValidationError,
    load_artifact,
    parse_artifact,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AverageUserEmbedding:
    # Keep only retrieval inputs and provenance from the validated artifact.
    # The frozen record and tuple prevent requests from mutating the shared prior.
    embedding: tuple[float, ...]
    dimension: int
    user_model_uuid: str
    post_model_uuid: str
    run_id: str
    contributing_users: int


# Each API worker owns one startup snapshot. Getters never read files or GCS;
# replacing the artifact or recovering from a failed load requires a restart.
_average_user_embedding: AverageUserEmbedding | None = None
# Distinguish an intentionally disabled prior from a failed configured load so
# the generator can log expected fallback at INFO and a load failure at WARNING.
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
    # deploy.sh resolves default.json to an exact artifact URI before deploying.
    # Runtime loading does not follow the mutable default or select a model.
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
            # This optional startup dependency gets one attempt, with a 30-second
            # request timeout; SDK retries must not prolong startup.
            # The SDK accepts None to disable retries despite its narrower annotation.
            data = blob.download_as_bytes(timeout=30, retry=None)  # type: ignore[arg-type]
        artifact = parse_artifact(data)
    elif parts.scheme:
        raise ArtifactValidationError("Expected a local file path or gs://bucket/object URI")
    else:
        artifact, _ = load_artifact(Path(uri).expanduser())

    # Both loaders apply Part 1's normalized version-1 contract, including its
    # declared dimension. Compatibility with a live prediction is checked later
    # by the generator, since model versions can change while the API is running.
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
        # File reads, ADC discovery, and the synchronous GCS client would block
        # the event loop. Startup still awaits completion before serving requests.
        prior = await asyncio.to_thread(_load_average_user_embedding, uri)
    except Exception as exc:
        # A prior is optional: keep the API and actual-user retrieval available.
        # Store a reason instead of retrying on subsequent feed requests.
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

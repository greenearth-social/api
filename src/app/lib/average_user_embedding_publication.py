"""Immutable artifact publication and environment-specific default selection."""

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from google.api_core.exceptions import Conflict, NotFound, PreconditionFailed
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from app.lib.average_user_embedding_artifact import (
    ArtifactValidationError,
    load_artifact,
    parse_artifact,
)

logger = logging.getLogger(__name__)
DEFAULT_PROJECT_ID = "greenearth-471522"
RETRY = DEFAULT_RETRY.with_deadline(180)
REQUEST_TIMEOUT = 60


class PublicationError(Exception):
    """A safe, credential-free publication or promotion failure."""


def environment_prefix(environment: str, project_id: str = DEFAULT_PROJECT_ID) -> str:
    if environment not in ("stage", "prod"):
        raise PublicationError("Environment must be stage or prod")
    if not re.fullmatch(r"[a-z][a-z0-9-]*[a-z0-9]", project_id):
        raise PublicationError("Invalid Google Cloud project ID")
    return f"gs://{project_id}-engagement-prediction-model-{environment}/average_user_embeddings"


def _gcs_parts(uri: str) -> tuple[str, str]:
    try:
        parts = urlsplit(uri)
    except ValueError:
        raise PublicationError(
            "Use an exact gs://bucket/object URI without query or fragment"
        ) from None
    if (
        parts.scheme != "gs"
        or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*[a-z0-9]", parts.netloc)
        or parts.query
        or parts.fragment
        or not re.fullmatch(r"/[A-Za-z0-9/_.-]+", parts.path)
        or any(part in ("", ".", "..") for part in parts.path[1:].split("/"))
    ):
        raise PublicationError("Use an exact gs://bucket/object URI without query or fragment")
    return parts.netloc, parts.path[1:]


def _artifact_filename(artifact: dict[str, Any]) -> str:
    return f"average_user_embedding_{artifact['run_id']}.json"


def _require_artifact_name(uri: str, artifact: dict[str, Any]) -> None:
    _, name = _gcs_parts(uri)
    if name.rsplit("/", 1)[-1] != _artifact_filename(artifact):
        raise PublicationError("Artifact URI filename does not match its run ID")


def _identity(artifact: dict[str, Any], uri: str) -> dict[str, Any]:
    return {
        "artifact_uri": uri,
        **{
            key: artifact[key]
            for key in (
                "run_id",
                "user_model_uuid",
                "post_model_uuid",
                "dimension",
                "contributing_users",
            )
        },
    }


def _download(client: storage.Client, uri: str) -> tuple[bytes, int]:
    """Pin the generation observed before downloading, including mutable pointers."""
    bucket_name, name = _gcs_parts(uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(name)
    blob.reload(timeout=REQUEST_TIMEOUT, retry=RETRY)
    if blob.generation is None:
        raise PublicationError("Cloud object has no generation")
    generation = int(blob.generation)
    data = bucket.blob(name, generation=generation).download_as_bytes(
        if_generation_match=generation, timeout=REQUEST_TIMEOUT, retry=RETRY
    )
    return data, generation


def _read_default(client: storage.Client, prefix: str) -> tuple[str | None, int]:
    try:
        data, generation = _download(client, f"{prefix}/default.json")
    except NotFound:
        return None, 0

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise PublicationError("Default pointer contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        pointer = json.loads(data, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError):
        raise PublicationError("Default pointer is not valid JSON") from None
    if (
        not isinstance(pointer, dict)
        or set(pointer) != {"artifact_uri"}
        or not isinstance(pointer["artifact_uri"], str)
    ):
        raise PublicationError("Default pointer must contain only artifact_uri")
    uri = pointer["artifact_uri"]
    _gcs_parts(uri)
    # The default may never redirect production to a test/staging bucket or nested prefix.
    filename = uri.removeprefix(f"{prefix}/")
    if uri != f"{prefix}/{filename}" or not re.fullmatch(
        r"average_user_embedding_\d{8}T\d{6}\.\d{6}Z_[0-9a-f]{8}\.json", filename
    ):
        raise PublicationError("Default pointer must select an artifact in its environment prefix")
    return uri, generation


def _upload_immutable(client: storage.Client, uri: str, data: bytes) -> dict[str, str]:
    bucket_name, name = _gcs_parts(uri)
    blob = client.bucket(bucket_name).blob(name)
    logger.info("Publication: uploading immutable artifact to %s", uri)
    try:
        blob.upload_from_string(
            data,
            content_type="application/json",
            if_generation_match=0,
            timeout=REQUEST_TIMEOUT,
            retry=RETRY,
        )
        generation = blob.generation
        if generation is None:
            raise PublicationError("Cloud publication returned no object generation")
    except (PreconditionFailed, Conflict):
        existing, generation = _download(client, uri)
        if existing != data:
            raise PublicationError(
                "Cloud object already exists with different bytes; refusing overwrite"
            ) from None
        logger.info("Publication: existing generation has identical bytes; idempotent success")
    return {"uri": uri, "generation": str(generation), "sha256": hashlib.sha256(data).hexdigest()}


def promote_artifact(
    source: str | Path, environment: str, project_id: str = DEFAULT_PROJECT_ID
) -> dict[str, Any]:
    """Copy exact artifact bytes, then compare-and-swap the environment's default."""
    prefix = environment_prefix(environment, project_id)
    source_uri = str(source)
    # Validate local artifacts before creating a cloud client or attempting any writes.
    local = None if source_uri.startswith("gs://") else load_artifact(Path(source).expanduser())
    try:
        client = storage.Client()
        try:
            if local is None:
                data, _ = _download(client, source_uri)
                artifact = parse_artifact(data)
                _require_artifact_name(source_uri, artifact)
            else:
                artifact, data = local
            previous_uri, previous_generation = _read_default(client, prefix)
            uri = f"{prefix}/{_artifact_filename(artifact)}"
            publication = _upload_immutable(client, uri, data)
            default_uri = f"{prefix}/default.json"
            bucket_name, name = _gcs_parts(default_uri)
            try:
                client.bucket(bucket_name).blob(name).upload_from_string(
                    (json.dumps({"artifact_uri": uri}, indent=2) + "\n").encode(),
                    content_type="application/json",
                    if_generation_match=previous_generation,
                    timeout=REQUEST_TIMEOUT,
                    retry=RETRY,
                )
            except (Conflict, PreconditionFailed):
                raise PublicationError(
                    "Default changed during promotion; inspect the current selection and retry"
                ) from None
            return {
                **_identity(artifact, uri),
                "previous_artifact_uri": previous_uri,
                "default_uri": default_uri,
                "publication": publication,
            }
        finally:
            client.close()
    except (PublicationError, ArtifactValidationError):
        raise
    except Exception as error:
        raise PublicationError(
            f"Promotion failed ({type(error).__name__}); default selection was not confirmed"
        ) from None

"""Shared validation and loading for normalized average-embedding artifacts."""

# The adjacent JSON schema validates the structure. Python handles vector math and
# relationships between fields without changing the artifact's original values.

import json
import math
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeGuard

from jsonschema import Draft202012Validator, ValidationError

# Load once from the application package so offline scripts and the deployed API
# share the same contract regardless of the current working directory.
_ARTIFACT_VALIDATOR = Draft202012Validator(
    json.loads(Path(__file__).with_name("average_user_embedding.schema.json").read_text()),
)


class ArtifactValidationError(Exception):
    """An artifact or its metadata does not satisfy the shared contract."""


def is_count(value: object) -> TypeGuard[int]:
    # Python considers bool an int; JSON true/false must not pass as numeric counts.
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def is_finite_number(value):
    try:
        return (
            isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        )
    except OverflowError:
        # Very large JSON integers can overflow conversion inside math.isfinite.
        return False


def model_id(value):
    """Normalize UUID spelling without inventing a model identity."""
    try:
        parsed = uuid.UUID(value) if isinstance(value, str) else None
        if parsed is None or parsed.int == 0:
            raise ValueError
        return parsed.hex
    except ValueError:
        raise ArtifactValidationError("Model identifiers must be nonzero UUIDs") from None


def validate_artifact(artifact):
    """Validate the schema, then the arithmetic relationships it cannot express."""
    try:
        _ARTIFACT_VALIDATOR.validate(artifact)
    except ValidationError as error:
        # Library messages include rejected values, which may contain private data.
        # Report only the field location and failed schema rule to callers/logs.
        raise ArtifactValidationError(
            "Artifact does not match the version 1 compact schema "
            f"at {error.json_path} ({error.validator})"
        ) from None

    vector, dimension = artifact["embedding"], artifact["dimension"]
    if not all(is_finite_number(value) for value in vector) or dimension != len(vector):
        raise ArtifactValidationError("Artifact requires a finite vector and matching dimension")
    # Checking unit magnitude also rejects zero vectors; never renormalize on load.
    if not math.isclose(math.hypot(*vector), 1.0, rel_tol=0.0, abs_tol=1e-6):
        raise ArtifactValidationError("Artifact embedding must have unit L2 magnitude within 1e-6")

    cohort = artifact["cohort"]
    try:
        # The schema checks the run ID's spelling; parsing checks its calendar date.
        started = datetime.strptime(artifact["run_id"].split("_")[0], "%Y%m%dT%H%M%S.%fZ").replace(
            tzinfo=UTC
        )
        completed = datetime.fromisoformat(artifact["source_completed_at"])
        cutoff = datetime.fromisoformat(cohort["cutoff"])
    except ValueError:
        raise ArtifactValidationError("Invalid artifact timestamp") from None
    if not cutoff <= started <= completed:
        raise ArtifactValidationError(
            "Artifact timestamps must satisfy cutoff <= run start <= completion"
        )

    # Coverage must balance in both stages: PostHog users split into eligible and
    # below-threshold users, and eligible users split into contributors and skips.
    if (
        artifact["contributing_users"] + cohort["skipped_users"] != cohort["eligible_users"]
        or cohort["eligible_users"] + cohort["below_min_likes"] != cohort["posthog_users"]
    ):
        raise ArtifactValidationError("Invalid artifact cohort or coverage metadata")
    return artifact


def parse_artifact(data: bytes):
    """Decode and validate artifact bytes without changing the saved vector."""

    # Duplicate fields are ambiguous across JSON readers; never publish them.
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ArtifactValidationError("Artifact contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        artifact = json.loads(data, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError):
        raise ArtifactValidationError("Artifact is not valid JSON") from None
    return validate_artifact(artifact)


def load_artifact(path: Path):
    """Return a validated artifact and its unchanged bytes for publication."""
    # Returning the original bytes lets promotion preserve even whitespace instead
    # of reserializing the object a person already inspected.
    data = path.read_bytes()
    return parse_artifact(data), data

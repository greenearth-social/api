"""Shared validation and loading for normalized average-embedding artifacts."""

# scripts/average_user_embedding.schema.json describes the JSON shape. This module
# also checks relationships that schema alone does not enforce, such as vector
# magnitude, matching counts, and timestamp ordering. It validates without repairing
# the input so generation, promotion, and consumers agree on the inspected artifact.

import json
import math
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeGuard


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


def validate_history_policy(policy):
    # This is provenance, not a hardcoded serving configuration. A consumer must
    # separately decide whether the declared window/sources/embedding key suit it.
    if (
        not isinstance(policy, dict)
        or set(policy) != {"limit", "sources", "embedding_key"}
        or not is_count(policy.get("limit"))
        or policy["limit"] == 0
        or not isinstance(policy.get("sources"), list)
        or not policy["sources"]
        or any(source not in ("posts", "replies") for source in policy["sources"])
        or len(set(policy["sources"])) != len(policy["sources"])
        or not isinstance(policy.get("embedding_key"), str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]+", policy["embedding_key"])
    ):
        raise ArtifactValidationError("Invalid history policy metadata")
    return policy


def utc_timestamp(value: object) -> datetime:
    try:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise ValueError
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() != timedelta(0):
            raise ValueError
        return parsed
    except ValueError:
        raise ArtifactValidationError("Expected a UTC timestamp ending in Z") from None


def validate_artifact(artifact):
    """Strict allowlist protects publication from leaking prototype user reports."""
    keys = {
        "artifact_type",
        "format_version",
        "embedding",
        "dimension",
        "user_model_uuid",
        "post_model_uuid",
        "run_id",
        "source_completed_at",
        "contributing_users",
        "cohort",
        "history_policy",
    }
    if not isinstance(artifact, dict) or set(artifact) != keys:
        raise ArtifactValidationError("Artifact does not match the version 1 compact schema")
    if (
        artifact["artifact_type"] != "average_user_embedding"
        or type(artifact["format_version"]) is not int
        or artifact["format_version"] != 1
    ):
        raise ArtifactValidationError("Unsupported artifact type or version")
    vector, dimension = artifact["embedding"], artifact["dimension"]
    # Version 1 stores the normalized mean. Loading must reject malformed data,
    # not renormalize it and silently change the artifact's original coordinates.
    if (
        not isinstance(vector, list)
        or not vector
        or not all(is_finite_number(value) for value in vector)
        or not any(value != 0 for value in vector)
        or not is_count(dimension)
        or dimension != len(vector)
    ):
        raise ArtifactValidationError(
            "Artifact requires a finite nonzero vector and matching dimension"
        )
    if not math.isclose(math.hypot(*vector), 1.0, rel_tol=0.0, abs_tol=1e-6):
        raise ArtifactValidationError("Artifact embedding must have unit L2 magnitude within 1e-6")
    for key in ("user_model_uuid", "post_model_uuid"):
        # Accept canonical identifiers in saved artifacts, even though API responses
        # can be canonicalized from other valid UUID spellings before saving.
        if model_id(artifact[key]) != artifact[key]:
            raise ArtifactValidationError("Artifact model UUIDs must use lowercase 32-hex format")
    run_id = artifact["run_id"]
    # The run ID becomes part of the filename and contains the producer's UTC start
    # time. Check both its safe spelling and whether the date itself actually exists.
    if not isinstance(run_id, str) or not re.fullmatch(r"\d{8}T\d{6}\.\d{6}Z_[0-9a-f]{8}", run_id):
        raise ArtifactValidationError("Invalid artifact run ID")
    try:
        started = datetime.strptime(run_id.split("_")[0], "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        raise ArtifactValidationError("Invalid artifact run timestamp") from None
    completed = utc_timestamp(artifact["source_completed_at"])
    if completed < started:
        raise ArtifactValidationError("Artifact completion precedes its run timestamp")
    count = artifact["contributing_users"]
    if not is_count(count) or count == 0:
        raise ArtifactValidationError("Artifact requires at least one contributor")
    validate_history_policy(artifact["history_policy"])
    cohort = artifact["cohort"]
    # Coverage must balance in both stages: PostHog users split into eligible and
    # below-threshold users, and eligible users split into contributors and skips.
    # There is no failed-user count because failed runs must not produce artifacts.
    count_keys = {
        "min_interaction_seen",
        "min_likes",
        "posthog_users",
        "below_min_likes",
        "eligible_users",
        "skipped_users",
    }
    if (
        not isinstance(cohort, dict)
        or set(cohort) != count_keys | {"posthog_project_id", "event", "scope", "cutoff"}
        or any(not is_count(cohort.get(key)) for key in count_keys)
        or not is_count(cohort.get("posthog_project_id"))
        or cohort["posthog_project_id"] == 0
        or cohort.get("event") != "interactionSeen"
        or cohort.get("scope") != "all_history_all_feeds"
        or count + cohort["skipped_users"] != cohort["eligible_users"]
        or cohort["eligible_users"] + cohort["below_min_likes"] != cohort["posthog_users"]
    ):
        raise ArtifactValidationError("Invalid artifact cohort or coverage metadata")
    if utc_timestamp(cohort["cutoff"]) > started:
        raise ArtifactValidationError("Artifact cohort cutoff follows run start")
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

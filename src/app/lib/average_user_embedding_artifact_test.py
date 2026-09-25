"""Consumer-independent coverage of the average-embedding artifact contract."""

import json
from pathlib import Path

import pytest

from app.lib.average_user_embedding_artifact import (
    ArtifactValidationError,
    load_artifact,
    parse_artifact,
    validate_artifact,
)

# The JSON fixture is an illustrative two-dimensional normalized mean, not a serving
# model artifact. Its four-user cohort splits into two contributors, one history
# skip, and one user below the like threshold. Keep it plain JSON for real loaders.
FIXTURE = Path(__file__).parents[3] / "scripts/fixtures/average_user_embedding_v1.json"
USER_MODEL = "1affd684bc7f45f895e488f83dd0a2fa"


def artifact_fixture():
    # Parse anew so one negative case's nested mutations cannot affect another case.
    return json.loads(FIXTURE.read_bytes())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda a: a.update(format_version=2),
        lambda a: a.update(format_version=True),
        lambda a: a.update(contributors=[{"user_did": "did:plc:private"}]),
        lambda a: a["cohort"].update(user_dids=["did:plc:private"]),
        lambda a: a.update(embedding=[0, 0]),
        lambda a: a.update(embedding=[float("nan"), 1]),
        lambda a: a.update(embedding=[float("inf"), 1]),
        lambda a: a.update(embedding=[True, 1]),
        lambda a: a.update(dimension=3),
        lambda a: a.update(user_model_uuid="invalid"),
        lambda a: a.update(user_model_uuid=USER_MODEL.upper()),
        lambda a: a.update(run_id="../../other.json"),
        lambda a: a.update(run_id="20261321T163045.123456Z_a1b2c3d4"),
        lambda a: a.update(contributing_users=0),
        lambda a: a["cohort"].update(eligible_users=999),
        lambda a: a["cohort"].update(cutoff="2099-01-01T00:00:00Z"),
        lambda a: a.update(source_completed_at="2000-01-01T00:00:00Z"),
    ],
)
def test_artifact_validation_rejects_malformed_or_private_data(mutate):
    # Mutate one otherwise-valid field to isolate each contract/privacy boundary.
    artifact = artifact_fixture()
    mutate(artifact)
    with pytest.raises(ArtifactValidationError):
        validate_artifact(artifact)


@pytest.mark.parametrize("magnitude", [1.0, 1.0 - 0.999e-6, 1.0 + 0.999e-6])
def test_artifact_validation_accepts_unit_magnitude_within_absolute_tolerance(magnitude):
    # The tolerance allows numeric roundoff; validation must not rewrite the vector.
    artifact = artifact_fixture()
    artifact["embedding"] = [magnitude, 0]
    assert validate_artifact(artifact) == artifact
    assert artifact["embedding"] == [magnitude, 0]


@pytest.mark.parametrize("vector", [[4, 6], [1.0 - 1.001e-6, 0], [1.0 + 1.001e-6, 0]])
def test_artifact_validation_rejects_nonunit_magnitude(vector):
    artifact = artifact_fixture()
    artifact["embedding"] = vector
    with pytest.raises(ArtifactValidationError):
        validate_artifact(artifact)


def test_json_duplicate_keys_are_rejected(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{"format_version":2,"format_version":1}')
    with pytest.raises(ArtifactValidationError, match="duplicate JSON keys"):
        load_artifact(path)


def test_parsing_and_loading_preserve_the_artifact_and_original_bytes(tmp_path):
    # Unusual but valid whitespace catches accidental reserialization before upload.
    artifact = artifact_fixture()
    data = ("\n  " + json.dumps(artifact, indent=3) + "\r\n").encode()
    path = tmp_path / "average.json"
    path.write_bytes(data)

    assert parse_artifact(data) == artifact
    loaded, original = load_artifact(path)
    assert loaded == artifact
    assert original == data
    assert path.read_bytes() == data


@pytest.mark.parametrize("key", ["format_version", "min_likes"])
def test_parser_rejects_duplicate_keys_at_every_object_level(key):
    # object_pairs_hook must guard the nested cohort object as well as the root.
    data = json.dumps(artifact_fixture())
    data = data.replace(f'"{key}":', f'"{key}": 1, "{key}":', 1).encode()
    with pytest.raises(ArtifactValidationError, match="duplicate JSON keys"):
        parse_artifact(data)


@pytest.mark.parametrize("data", [b"", b"{broken", b"{} trailing", b'"\xff"'])
def test_invalid_json_or_encoding_has_a_useful_validation_error(data):
    with pytest.raises(ArtifactValidationError, match="Artifact is not valid JSON"):
        parse_artifact(data)


@pytest.mark.parametrize("data", [b"null", b"[]", b"true", b"{}"])
def test_parser_validates_decoded_objects(data):
    with pytest.raises(ArtifactValidationError, match="version 1 compact schema"):
        parse_artifact(data)

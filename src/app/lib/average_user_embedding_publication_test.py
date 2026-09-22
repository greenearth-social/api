"""Offline checks for immutable artifacts and conditional environment promotion."""

import hashlib
import json
from pathlib import Path

import pytest
from google.api_core.exceptions import Forbidden, NotFound, PreconditionFailed

from app.lib import average_user_embedding_publication as publication
from app.lib.average_user_embedding_artifact import ArtifactValidationError

FIXTURE = Path(__file__).resolve().parents[3] / "scripts/fixtures/average_user_embedding_v1.json"


class MemoryBlob:
    def __init__(self, store, bucket, name, generation=None):
        self.store, self.key, self.generation = store, (bucket, name), generation

    def reload(self, **kwargs):
        self.store.reads.append(("reload", self.key, kwargs))
        if self.key not in self.store.objects:
            raise NotFound("secret raw error")
        self.generation = self.store.objects[self.key][1]

    def download_as_bytes(self, **kwargs):
        self.store.reads.append(("download", self.key, kwargs))
        data, generation = self.store.objects[self.key]
        assert self.generation == generation == kwargs["if_generation_match"]
        return data

    def upload_from_string(self, data, **kwargs):
        self.store.writes.append((self.key, data, kwargs))
        if self.key[1].endswith("/default.json") and self.store.pointer_race:
            raise PreconditionFailed("secret raw error")
        if not self.key[1].endswith("/default.json") and self.store.upload_failure:
            raise Forbidden("secret raw error")
        expected = self.store.objects.get(self.key, (None, 0))[1]
        if expected != kwargs["if_generation_match"]:
            raise PreconditionFailed("secret raw error")
        self.store.next_generation += 1
        self.generation = self.store.next_generation
        self.store.objects[self.key] = (data, self.generation)


class MemoryCloud:
    def __init__(self):
        self.objects = {}
        self.reads, self.writes = [], []
        self.next_generation = 100
        self.pointer_race = self.upload_failure = False
        self.closed = False

    def bucket(self, bucket):
        store = self

        class Bucket:
            def blob(self, name, generation=None):
                return MemoryBlob(store, bucket, name, generation)

        return Bucket()

    def close(self):
        self.closed = True

    def put(self, uri, data):
        self.next_generation += 1
        self.objects[publication._gcs_parts(uri)] = (data, self.next_generation)

    def data(self, uri):
        return self.objects[publication._gcs_parts(uri)][0]


@pytest.fixture
def cloud(monkeypatch):
    cloud = MemoryCloud()
    monkeypatch.setattr(publication.storage, "Client", lambda: cloud)
    return cloud


@pytest.fixture
def artifact_path(tmp_path):
    path = tmp_path / "inspected artifact.json"
    path.write_bytes(FIXTURE.read_bytes())
    return path


def artifact_uri(environment="stage", artifact=None):
    artifact = artifact or json.loads(FIXTURE.read_bytes())
    prefix = publication.environment_prefix(environment)
    return f"{prefix}/average_user_embedding_{artifact['run_id']}.json"


def default_uri(environment="stage"):
    return f"{publication.environment_prefix(environment)}/default.json"


@pytest.mark.parametrize("environment", ["stage", "prod"])
def test_environment_mapping(environment):
    assert publication.environment_prefix(environment) == (
        f"gs://greenearth-471522-engagement-prediction-model-{environment}/average_user_embeddings"
    )
    assert publication.environment_prefix(environment, "another-project") == (
        f"gs://another-project-engagement-prediction-model-{environment}/average_user_embeddings"
    )


@pytest.mark.parametrize("environment,project", [("test", "project"), ("prod", "../secret")])
def test_invalid_destination(environment, project):
    with pytest.raises(publication.PublicationError):
        publication.environment_prefix(environment, project)


def test_first_promotion_exact_bytes_summary_and_bounded_calls(cloud, artifact_path):
    result = publication.promote_artifact(artifact_path, "stage")
    assert result["previous_artifact_uri"] is None
    assert result["artifact_uri"] == artifact_uri()
    assert result["default_uri"] == default_uri()
    assert result["dimension"] == 2 and result["contributing_users"] == 2
    assert "embedding" not in result
    assert cloud.data(artifact_uri()) == artifact_path.read_bytes()
    assert result["publication"] == {
        "uri": artifact_uri(),
        "generation": str(cloud.objects[publication._gcs_parts(artifact_uri())][1]),
        "sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
    }
    assert json.loads(cloud.data(default_uri())) == {"artifact_uri": artifact_uri()}
    assert [write[2]["if_generation_match"] for write in cloud.writes] == [0, 0]
    for _, _, options in cloud.reads + cloud.writes:
        assert options["timeout"] == 60
        assert options["retry"].deadline == 180
    assert cloud.closed


def test_home_path_expansion(cloud, artifact_path, monkeypatch):
    monkeypatch.setenv("HOME", str(artifact_path.parent))
    publication.promote_artifact(f"~/{artifact_path.name}", "stage")
    assert cloud.data(artifact_uri()) == artifact_path.read_bytes()


def test_replacement_rollback_and_idempotent_artifact(cloud, artifact_path):
    first = publication.promote_artifact(artifact_path, "stage")
    first_bytes = artifact_path.read_bytes()
    second = json.loads(first_bytes)
    second["run_id"] = second["run_id"][:-8] + "12345678"
    artifact_path.write_text(json.dumps(second))
    previous_generation = cloud.objects[publication._gcs_parts(default_uri())][1]
    replacement = publication.promote_artifact(artifact_path, "stage")
    assert replacement["previous_artifact_uri"] == first["artifact_uri"]
    assert cloud.writes[-1][2]["if_generation_match"] == previous_generation
    assert cloud.data(first["artifact_uri"]) == first_bytes
    rollback = publication.promote_artifact(first["artifact_uri"], "stage")
    assert rollback["previous_artifact_uri"] == replacement["artifact_uri"]
    assert json.loads(cloud.data(default_uri())) == {"artifact_uri": first["artifact_uri"]}
    repeat = publication.promote_artifact(first["artifact_uri"], "stage")
    assert repeat["publication"] == first["publication"]


def test_cross_bucket_promotion_preserves_exact_bytes_and_local_destination(cloud):
    source = (
        "gs://greenearth-471522-engagement-prediction-test/experiments/"
        + artifact_uri().rsplit("/", 1)[1]
    )
    data = FIXTURE.read_bytes() + b"\n\n"
    cloud.put(source, data)
    result = publication.promote_artifact(source, "prod")
    assert result["artifact_uri"] == artifact_uri("prod")
    assert cloud.data(result["artifact_uri"]) == data
    assert json.loads(cloud.data(default_uri("prod"))) == {"artifact_uri": artifact_uri("prod")}
    downloads = [entry for entry in cloud.reads if entry[0] == "download"]
    assert downloads[0][2]["if_generation_match"] == 101


def test_invalid_local_artifact_makes_no_cloud_requests(cloud, artifact_path):
    artifact_path.write_text("{}")
    with pytest.raises(ArtifactValidationError):
        publication.promote_artifact(artifact_path, "stage")
    assert not cloud.reads and not cloud.writes


def test_invalid_remote_artifact_makes_no_writes(cloud):
    cloud.put(artifact_uri(), b"{}")
    with pytest.raises(ArtifactValidationError):
        publication.promote_artifact(artifact_uri(), "prod")
    assert not cloud.writes


def test_upload_failure_leaves_default_unchanged(cloud, artifact_path):
    first = publication.promote_artifact(artifact_path, "stage")
    pointer_before = cloud.data(default_uri())
    cloud.upload_failure = True
    with pytest.raises(publication.PublicationError, match=r"Promotion failed \(Forbidden\)"):
        publication.promote_artifact(artifact_path, "stage")
    assert cloud.data(default_uri()) == pointer_before
    assert cloud.data(first["artifact_uri"]) == artifact_path.read_bytes()
    assert not cloud.writes[-1][0][1].endswith("default.json")


def test_pointer_race_does_not_replace_current_default(cloud, artifact_path):
    first = publication.promote_artifact(artifact_path, "stage")
    before = cloud.data(default_uri())
    artifact = json.loads(artifact_path.read_bytes())
    artifact["run_id"] = artifact["run_id"][:-8] + "12345678"
    artifact_path.write_text(json.dumps(artifact))
    cloud.pointer_race = True
    with pytest.raises(publication.PublicationError, match="Default changed during promotion"):
        publication.promote_artifact(artifact_path, "stage")
    assert cloud.data(default_uri()) == before
    assert cloud.data(artifact_uri(artifact=artifact)) == artifact_path.read_bytes()
    assert first["artifact_uri"] != artifact_uri(artifact=artifact)


def test_existing_different_bytes_refuse_overwrite(cloud, artifact_path):
    cloud.put(artifact_uri(), b"different")
    with pytest.raises(publication.PublicationError, match="different bytes"):
        publication.promote_artifact(artifact_path, "stage")
    assert cloud.data(artifact_uri()) == b"different"
    assert publication._gcs_parts(default_uri()) not in cloud.objects


@pytest.mark.parametrize(
    "pointer",
    [
        b"{}",
        b"not-json",
        b'{"artifact_uri":"one","artifact_uri":"two"}',
        json.dumps({"artifact_uri": artifact_uri("prod")}).encode(),
        json.dumps(
            {
                "artifact_uri": artifact_uri().replace(
                    "/average_user_embedding_", "/nested/average_user_embedding_"
                )
            }
        ).encode(),
        json.dumps({"artifact_uri": default_uri()}).encode(),
        json.dumps({"artifact_uri": artifact_uri(), "extra": True}).encode(),
    ],
)
def test_invalid_pointer_cannot_redirect_and_prevents_promotion(cloud, artifact_path, pointer):
    cloud.put(default_uri(), pointer)
    with pytest.raises(publication.PublicationError):
        publication.promote_artifact(artifact_path, "stage")
    with pytest.raises(publication.PublicationError):
        publication.resolve_artifact("stage")
    assert not cloud.writes
    assert all(key[0].endswith("-stage") for _, key, _ in cloud.reads)


def test_mismatched_artifact_filename_rejected(cloud):
    wrong_uri = artifact_uri().replace(".json", "0.json")
    cloud.put(wrong_uri, FIXTURE.read_bytes())
    with pytest.raises(publication.PublicationError, match="filename does not match"):
        publication.promote_artifact(wrong_uri, "prod")
    with pytest.raises(publication.PublicationError, match="filename does not match"):
        publication.resolve_artifact("prod", artifact_uri=wrong_uri)
    assert not cloud.writes


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket",
        "gs://user:secret@bucket/file",
        "gs://bucket/../file",
        "gs://bucket/file?secret=yes",
        "gs://bucket/path with spaces",
        "gs://bucket/file#fragment",
    ],
)
def test_invalid_source_uri_rejected_without_cloud_reads(cloud, uri):
    with pytest.raises(publication.PublicationError):
        publication.promote_artifact(uri, "stage")
    with pytest.raises(publication.PublicationError):
        publication.resolve_artifact("stage", artifact_uri=uri)
    assert not cloud.reads and not cloud.writes


def test_unreadable_artifact_hides_upstream_error(cloud):
    with pytest.raises(
        publication.PublicationError, match=r"Promotion failed \(NotFound\)"
    ) as result:
        publication.promote_artifact(artifact_uri(), "stage")
    assert "secret" not in str(result.value)


@pytest.mark.parametrize(
    "environment,project_id",
    [
        ("stage", publication.DEFAULT_PROJECT_ID),
        ("prod", publication.DEFAULT_PROJECT_ID),
        ("stage", "another-project"),
    ],
)
def test_resolve_default_is_read_only_with_pinned_bounded_downloads(cloud, environment, project_id):
    prefix = publication.environment_prefix(environment, project_id)
    uri = f"{prefix}/{artifact_uri().rsplit('/', 1)[1]}"
    cloud.put(uri, FIXTURE.read_bytes())
    cloud.put(f"{prefix}/default.json", json.dumps({"artifact_uri": uri}).encode())

    result = publication.resolve_artifact(environment, project_id)

    artifact = json.loads(FIXTURE.read_bytes())
    assert result == {
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
    assert not cloud.writes
    downloads = [entry for entry in cloud.reads if entry[0] == "download"]
    assert [entry[2]["if_generation_match"] for entry in downloads] == [102, 101]
    for _, _, options in cloud.reads:
        assert options["timeout"] == 60
        assert options["retry"].deadline == 180
    assert cloud.closed


def test_explicit_override_bypasses_default_and_can_use_another_bucket(cloud):
    uri = artifact_uri("stage")
    cloud.put(uri, FIXTURE.read_bytes())
    cloud.put(default_uri("prod"), b"malformed pointer must not be consulted")

    result = publication.resolve_artifact("prod", artifact_uri=uri)

    assert result["artifact_uri"] == uri
    assert not cloud.writes
    assert all(key == publication._gcs_parts(uri) for _, key, _ in cloud.reads)


def test_missing_default_has_helpful_error(cloud):
    with pytest.raises(
        publication.PublicationError, match="No default artifact for stage; promote"
    ):
        publication.resolve_artifact("stage")
    assert not cloud.writes
    assert cloud.closed


@pytest.mark.parametrize("use_default", [False, True])
def test_missing_selected_artifact_has_safe_error(cloud, use_default):
    if use_default:
        cloud.put(default_uri(), json.dumps({"artifact_uri": artifact_uri()}).encode())
    with pytest.raises(
        publication.PublicationError, match=r"Artifact resolution failed \(NotFound\)"
    ) as result:
        publication.resolve_artifact("stage", artifact_uri=None if use_default else artifact_uri())
    assert "secret" not in str(result.value)
    assert not cloud.writes
    assert cloud.closed


@pytest.mark.parametrize("failure_target", ["default", "artifact"])
def test_forbidden_default_or_artifact_read_has_safe_error(cloud, monkeypatch, failure_target):
    cloud.put(default_uri(), json.dumps({"artifact_uri": artifact_uri()}).encode())
    target_uri = default_uri() if failure_target == "default" else artifact_uri()
    original_reload = MemoryBlob.reload

    def denied_reload(blob, **kwargs):
        if blob.key == publication._gcs_parts(target_uri):
            raise Forbidden("secret cloud response")
        return original_reload(blob, **kwargs)

    monkeypatch.setattr(MemoryBlob, "reload", denied_reload)
    with pytest.raises(
        publication.PublicationError, match=r"Artifact resolution failed \(Forbidden\)"
    ) as result:
        publication.resolve_artifact("stage")
    assert "secret" not in str(result.value)
    assert not cloud.writes
    assert cloud.closed


@pytest.mark.parametrize("embedding", [[0, 0], [4, 6], [float("nan"), 1]])
def test_resolution_rejects_invalid_normalized_vector(cloud, embedding):
    artifact = json.loads(FIXTURE.read_bytes())
    artifact["embedding"] = embedding
    cloud.put(artifact_uri(), json.dumps(artifact).encode())
    with pytest.raises(ArtifactValidationError):
        publication.resolve_artifact("stage", artifact_uri=artifact_uri())
    assert not cloud.writes


def test_failed_cloud_identity_is_safe(monkeypatch):
    from google.auth.exceptions import DefaultCredentialsError

    def no_identity():
        raise DefaultCredentialsError("secret credential file path")

    monkeypatch.setattr(publication.storage, "Client", no_identity)
    with pytest.raises(
        publication.PublicationError,
        match=r"Artifact resolution failed \(DefaultCredentialsError\)",
    ) as result:
        publication.resolve_artifact("stage")
    assert "secret" not in str(result.value)

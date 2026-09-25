"""Startup loading for the optional average-user prior."""

import asyncio
import json
import logging
import os
import threading
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from . import average_user_embedding as module

FIXTURE = Path(__file__).resolve().parents[3] / "scripts/fixtures/average_user_embedding_v1.json"


@pytest.fixture(autouse=True)
def reset_prior(monkeypatch):
    monkeypatch.delenv("GE_AVERAGE_USER_EMBEDDING_URI", raising=False)
    module.set_average_user_embedding(None, "not_configured")
    yield
    module.set_average_user_embedding(None, "not_configured")


@pytest.fixture
def artifact():
    return json.loads(FIXTURE.read_bytes())


def write_artifact(tmp_path, monkeypatch, artifact):
    path = tmp_path / "prior.json"
    path.write_text(json.dumps(artifact))
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", str(path))
    return path


@pytest.mark.asyncio
async def test_local_artifact_preserves_vector_and_metadata(
    tmp_path, monkeypatch, artifact, caplog
):
    path = write_artifact(tmp_path, monkeypatch, artifact)
    with caplog.at_level(logging.INFO, logger=module.__name__):
        await module.init_average_user_embedding()

    prior = module.get_average_user_embedding()
    assert prior is not None
    assert prior.embedding == tuple(artifact["embedding"])
    assert prior.dimension == 2  # The contract is not tied to a 128-dimensional model.
    assert prior.user_model_uuid == artifact["user_model_uuid"]
    assert prior.post_model_uuid == artifact["post_model_uuid"]
    assert prior.run_id == artifact["run_id"]
    assert prior.contributing_users == artifact["contributing_users"]
    assert module.get_average_user_embedding_error() is None
    assert str(path) in caplog.text
    assert prior.run_id in caplog.text
    assert prior.user_model_uuid in caplog.text
    assert prior.post_model_uuid in caplog.text
    assert "dimension=2 contributing_users=2" in caplog.text
    assert str(artifact["embedding"][0]) not in caplog.text
    with pytest.raises(FrozenInstanceError):
        setattr(prior, "dimension", 128)  # noqa: B010 - exercise runtime frozen validation


@pytest.mark.asyncio
async def test_local_path_expands_home(tmp_path, monkeypatch, artifact):
    path = write_artifact(tmp_path, monkeypatch, artifact)
    relative_path = os.path.relpath(path, Path.home())
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", f"~/{relative_path}")
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is not None


@pytest.mark.asyncio
async def test_gcs_download_uses_adc_one_attempt_and_no_request_path_io(monkeypatch):
    from google.cloud import storage

    client = MagicMock()
    client.__enter__.return_value = client
    blob = client.bucket.return_value.blob.return_value
    blob.download_as_bytes.return_value = FIXTURE.read_bytes()
    constructor = MagicMock(return_value=client)
    monkeypatch.setattr(storage, "Client", constructor)
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", "gs://test-models/path/prior.json")

    await module.init_average_user_embedding()
    prior = module.get_average_user_embedding()
    assert prior is not None
    for _ in range(3):
        assert module.get_average_user_embedding() is prior
        assert module.get_average_user_embedding_error() is None
    constructor.assert_called_once_with()
    client.bucket.assert_called_once_with("test-models")
    client.bucket.return_value.blob.assert_called_once_with("path/prior.json")
    blob.download_as_bytes.assert_called_once_with(timeout=30, retry=None)
    client.__exit__.assert_called_once()


@pytest.mark.asyncio
async def test_loading_occurs_off_the_event_loop(tmp_path, monkeypatch, artifact):
    write_artifact(tmp_path, monkeypatch, artifact)
    thread_ids = []
    load = module._load_average_user_embedding

    def tracked_load(uri):
        thread_ids.append(threading.get_ident())
        return load(uri)

    monkeypatch.setattr(module, "_load_average_user_embedding", tracked_load)
    main_thread = threading.get_ident()
    await module.init_average_user_embedding()
    assert len(thread_ids) == 1
    assert thread_ids[0] != main_thread


@pytest.mark.asyncio
async def test_artifact_and_configuration_changes_wait_for_restart(tmp_path, monkeypatch, artifact):
    path = write_artifact(tmp_path, monkeypatch, artifact)
    await module.init_average_user_embedding()
    original = module.get_average_user_embedding()
    path.write_text("invalid")
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", str(tmp_path / "missing.json"))
    assert module.get_average_user_embedding() is original
    assert module.get_average_user_embedding_error() is None

    # A new lifespan invokes initialization again; getters never reload.
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "load_failed"


@pytest.mark.asyncio
async def test_unconfigured_prior_does_not_load(monkeypatch):
    load = MagicMock()
    monkeypatch.setattr(module, "_load_average_user_embedding", load)
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "not_configured"
    load.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates,reason",
    [
        ({"embedding": [0.0, 0.0]}, "finite nonzero"),
        ({"embedding": [float("nan"), 1.0]}, "finite nonzero"),
        ({"embedding": [float("inf"), 1.0]}, "finite nonzero"),
        ({"embedding": [1.0, 1.0]}, "unit L2 magnitude"),
        ({"dimension": 128}, "matching dimension"),
        ({"format_version": 2}, "Unsupported artifact version"),
        ({"user_model_uuid": "not-a-uuid"}, "Model identifiers"),
        ({"contributing_users": 0}, "at least one contributor"),
        ({"source_completed_at": "invalid"}, "UTC timestamp"),
    ],
)
async def test_invalid_artifact_is_unavailable(
    tmp_path, monkeypatch, artifact, caplog, updates, reason
):
    artifact.update(updates)
    write_artifact(tmp_path, monkeypatch, artifact)
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "load_failed"
    assert reason in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [b"not json", b'{"format_version": 1, "format_version": 1}'])
async def test_malformed_json_is_unavailable(tmp_path, monkeypatch, data):
    path = tmp_path / "prior.json"
    path.write_bytes(data)
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", str(path))
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "load_failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uri",
    [
        "gs://test-models",
        "gs:///prior.json",
        "gs://test-models/prior.json?token=secret",
        "gs://test-models/prior.json#version",
        "gs://user:secret@test-models/prior.json",
        "https://example.com/prior.json?token=secret",
    ],
)
async def test_invalid_uri_is_unavailable_without_logging_secrets(monkeypatch, caplog, uri):
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", uri)
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "load_failed"
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_gcs_failure_is_unavailable_without_logging_exception_secrets(monkeypatch, caplog):
    from google.cloud import storage

    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", "gs://test-models/prior.json")
    monkeypatch.setattr(storage, "Client", MagicMock(side_effect=OSError("secret")))
    await module.init_average_user_embedding()
    assert module.get_average_user_embedding() is None
    assert module.get_average_user_embedding_error() == "load_failed"
    assert "OSError" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_startup_cancellation_propagates(monkeypatch):
    monkeypatch.setenv("GE_AVERAGE_USER_EMBEDDING_URI", "prior.json")
    monkeypatch.setattr(
        module, "_load_average_user_embedding", MagicMock(side_effect=asyncio.CancelledError)
    )
    with pytest.raises(asyncio.CancelledError):
        await module.init_average_user_embedding()

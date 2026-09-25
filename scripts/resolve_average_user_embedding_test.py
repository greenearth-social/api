"""The deployment resolver prints only the selected immutable URI to stdout."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import resolve_average_user_embedding as resolver

from app.lib.average_user_embedding_artifact import ArtifactValidationError
from app.lib.average_user_embedding_publication import PublicationError


def test_resolver_prints_uri_and_identity_on_separate_streams(monkeypatch, capsys):
    uri = "gs://example-model-stage/average_user_embeddings/average_user_embedding_run.json"
    result = {
        "artifact_uri": uri,
        "run_id": "example-run",
        "user_model_uuid": "1" * 32,
        "post_model_uuid": "2" * 32,
        "dimension": 128,
        "contributing_users": 407,
    }
    resolve = Mock(return_value=result)
    monkeypatch.setattr(resolver, "resolve_artifact", resolve)

    assert resolver.main(["--environment", "stage"]) == 0

    resolve.assert_called_once_with("stage", "greenearth-471522", None)
    output = capsys.readouterr()
    # Shell command substitution consumes stdout verbatim. Identity logs must
    # stay on stderr so they cannot become part of the Cloud Run environment value.
    assert output.out == uri + "\n"
    for field in (
        "run_id",
        "user_model_uuid",
        "post_model_uuid",
        "dimension",
        "contributing_users",
    ):
        assert f"{field}={result[field]}" in output.err


def test_resolver_forwards_explicit_selection_and_project(monkeypatch, capsys):
    uri = "gs://example-test/average_user_embedding_run.json"
    resolve = Mock(side_effect=PublicationError("selection unavailable"))
    monkeypatch.setattr(resolver, "resolve_artifact", resolve)

    assert (
        resolver.main(
            ["--environment", "prod", "--project-id", "example-project", "--artifact-uri", uri]
        )
        == 1
    )

    resolve.assert_called_once_with("prod", "example-project", uri)
    output = capsys.readouterr()
    assert output.out == ""
    assert "selection unavailable" in output.err


@pytest.mark.parametrize(
    "error,reason",
    [
        (PublicationError("default not found"), "default not found"),
        (ArtifactValidationError("invalid vector"), "invalid vector"),
        (RuntimeError("SECRET RESPONSE BODY"), "RuntimeError"),
    ],
)
def test_resolution_failure_cannot_be_mistaken_for_an_artifact_uri(
    monkeypatch, capsys, error, reason
):
    monkeypatch.setattr(resolver, "resolve_artifact", Mock(side_effect=error))

    assert resolver.main(["--environment", "prod"]) == 1

    output = capsys.readouterr()
    assert output.out == ""
    assert reason in output.err
    assert "SECRET RESPONSE BODY" not in output.err


def test_help_from_outside_repository(tmp_path):
    result = subprocess.run(
        [sys.executable, str(Path(resolver.__file__).resolve()), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "--environment" in result.stdout
    assert "--artifact-uri" in result.stdout

"""Promotion CLI uses normal repository imports and prints vector-free summaries."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import promote_average_user_embedding as promote
import pytest

from app.lib.average_user_embedding_artifact import ArtifactValidationError
from app.lib.average_user_embedding_publication import PublicationError


# Cloud semantics are covered by average_user_embedding_publication_test.py.
# These tests focus on CLI arguments, exit status, and separating stdout from stderr.
def test_cli_promotes_with_explicit_environment_and_prints_selection(monkeypatch, capsys):
    result = {
        "previous_artifact_uri": "gs://bucket/previous.json",
        "artifact_uri": "gs://bucket/selected.json",
        "default_uri": "gs://bucket/default.json",
        "run_id": "example",
    }
    call = Mock(return_value=result)
    monkeypatch.setattr(promote, "promote_artifact", call)
    assert promote.main(["/tmp/inspected artifact.json", "--environment", "prod"]) == 0
    call.assert_called_once_with("/tmp/inspected artifact.json", "prod", "greenearth-471522")
    output = capsys.readouterr()
    assert json.loads(output.out) == {"status": "success", **result}
    assert "Previous: gs://bucket/previous.json" in output.err
    assert "Selected: gs://bucket/selected.json" in output.err
    assert "does not deploy the API or change running services" in output.err


def test_custom_project(monkeypatch, capsys):
    call = Mock(return_value={"previous_artifact_uri": None, "artifact_uri": "gs://bucket/file"})
    monkeypatch.setattr(promote, "promote_artifact", call)
    assert (
        promote.main(["file.json", "--environment", "stage", "--project-id", "another-project"])
        == 0
    )
    call.assert_called_once_with("file.json", "stage", "another-project")
    assert "Previous: (none)" in capsys.readouterr().err


@pytest.mark.parametrize(
    "error",
    [
        PublicationError("safe reason"),
        ArtifactValidationError("safe reason"),
        FileNotFoundError("secret local error"),
    ],
)
def test_failures_are_safe_json_nonzero(monkeypatch, capsys, error):
    # Controlled validation messages may be shown, but filesystem exception text
    # is untrusted and should be reduced to its exception class.
    monkeypatch.setattr(promote, "promote_artifact", Mock(side_effect=error))
    assert promote.main(["file.json", "--environment", "stage"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "failed"
    assert "secret" not in output.out + output.err
    assert "Promotion failed:" in output.err


def test_environment_is_required(capsys):
    with pytest.raises(SystemExit) as result:
        promote.main(["file.json"])
    assert result.value.code == 2
    assert "--environment" in capsys.readouterr().err


def test_help_from_outside_repository(tmp_path):
    # A subprocess avoids inheriting pytest's import-path setup and checks the
    # repository-relative imports a person invoking the script actually relies on.
    script = Path(promote.__file__).resolve()
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "--environment" in result.stdout
    assert "--project-id" in result.stdout

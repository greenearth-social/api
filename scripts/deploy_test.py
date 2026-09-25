"""Exercise embedding selection and deployment ordering without cloud access."""

import os
import subprocess
from pathlib import Path

import pytest

DEPLOY_SCRIPT = Path(__file__).with_name("deploy.sh")
ARTIFACT_NAME = "average_user_embedding_20260922T120000.000000Z_12345678.json"

# Run the real argument parser, main orchestration, and Cloud Run command
# builder. Only external commands and unrelated preflight steps are replaced.
HARNESS = r"""
record() { printf '%s\n' "$1" >> "$TEST_DIRECTORY/events"; }

gcloud() {
    case "$1 $2 $3" in
        "config set project") record configure_project ;;
        "run deploy greenearth-api-"*)
            record deploy
            printf '%s\0' "$@" > "$TEST_DIRECTORY/deploy_args"
            ;;
        "run services describe") printf '%s\n' "https://test-api.run.app" ;;
        "run services update-traffic") record update_traffic ;;
        *) record unexpected_gcloud_command; return 90 ;;
    esac
}

pipenv() {
    record resolve_embedding
    printf '%s\0' "$@" > "$TEST_DIRECTORY/resolver_args"
    if [ "${FAKE_RESOLVER_STATUS:-0}" != 0 ]; then
        printf '%s\n' "No promoted default was found" >&2
        return "$FAKE_RESOLVER_STATUS"
    fi
    if [ "${FAKE_RESOLVER_OUTPUT+x}" ]; then
        printf '%s\n' "$FAKE_RESOLVER_OUTPUT"
        return
    fi
    local artifact_uri="gs://${PROJECT_ID}-engagement-prediction-model-${ENVIRONMENT}/average_user_embeddings/$ARTIFACT_NAME"
    while [ $# -gt 0 ]; do
        if [ "$1" = --artifact-uri ]; then
            artifact_uri="$2"
            break
        fi
        shift
    done
    printf '%s\n' "$artifact_uri"
}

deploy_script="$1"
shift
source "$deploy_script" "$@"

require_clean_worktree() { record clean_worktree; GIT_SHA=1234567; }
preflight_bsky_publishers() { record publisher_preflight; }
verify_vpc_connector() { record check_connector; VPC_CONNECTOR_EXISTS=false; }
configure_kubectl() { record configure_kubectl; }
get_elasticsearch_internal_lb_ip() { GE_ELASTICSEARCH_URL=https://127.0.0.1:9200; }
generate_requirements() { record generate_requirements; }
prepare_ux_posts() { record prepare_ux_posts; }
sync_feeds() { record sync_feeds; }
main
"""


def run_deploy(
    tmp_path: Path,
    arguments: list[str] | None = None,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for name in ("GE_AVERAGE_USER_EMBEDDING_URI", "FAKE_RESOLVER_STATUS", "FAKE_RESOLVER_OUTPUT"):
        env.pop(name, None)
    env.update(
        TEST_DIRECTORY=str(tmp_path),
        ARTIFACT_NAME=ARTIFACT_NAME,
        # Also prevent accidental calls to locally installed cloud tools.
        PATH="/usr/bin:/bin",
    )
    env.update(environment or {})
    return subprocess.run(
        ["/bin/bash", "-c", HARNESS, "deployment-test", str(DEPLOY_SCRIPT), *(arguments or [])],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )


def recorded_args(tmp_path: Path, name: str) -> list[str]:
    return (tmp_path / name).read_text().removesuffix("\0").split("\0")


@pytest.mark.parametrize(
    ("arguments", "environment_name", "project"),
    [
        ([], "stage", "greenearth-471522"),
        (["--environment", "prod"], "prod", "greenearth-471522"),
        (["--project-id", "another-project"], "stage", "another-project"),
    ],
)
def test_default_selection_is_resolved_and_pinned_before_deployment(
    tmp_path: Path, arguments: list[str], environment_name: str, project: str
) -> None:
    result = run_deploy(tmp_path, arguments)

    assert result.returncode == 0, result.stdout + result.stderr
    assert recorded_args(tmp_path, "resolver_args") == [
        "run",
        "python",
        "scripts/resolve_average_user_embedding.py",
        "--environment",
        environment_name,
        "--project-id",
        project,
    ]
    artifact_uri = (
        f"gs://{project}-engagement-prediction-model-{environment_name}"
        f"/average_user_embeddings/{ARTIFACT_NAME}"
    )
    deploy_args = recorded_args(tmp_path, "deploy_args")
    assert deploy_args[:3] == ["run", "deploy", f"greenearth-api-{environment_name}"]
    assert f"--set-env-vars=GE_AVERAGE_USER_EMBEDDING_URI={artifact_uri}" in deploy_args
    events = (tmp_path / "events").read_text().splitlines()
    assert events[:5] == [
        "clean_worktree",
        "configure_project",
        "resolve_embedding",
        "prepare_ux_posts",
        "publisher_preflight",
    ]
    assert events.index("prepare_ux_posts") < events.index("deploy")


@pytest.mark.parametrize("use_cli_override", [False, True])
def test_explicit_artifact_is_validated_and_cli_overrides_environment(
    tmp_path: Path, use_cli_override: bool
) -> None:
    environment_uri = f"gs://custom-bucket/from-environment/{ARTIFACT_NAME}"
    cli_uri = f"gs://custom-bucket/from-cli/{ARTIFACT_NAME}"
    arguments = ["--average-user-embedding-uri", cli_uri] if use_cli_override else []

    result = run_deploy(tmp_path, arguments, {"GE_AVERAGE_USER_EMBEDDING_URI": environment_uri})

    assert result.returncode == 0, result.stdout + result.stderr
    expected_uri = cli_uri if use_cli_override else environment_uri
    assert recorded_args(tmp_path, "resolver_args")[-2:] == ["--artifact-uri", expected_uri]
    assert f"--set-env-vars=GE_AVERAGE_USER_EMBEDDING_URI={expected_uri}" in recorded_args(
        tmp_path, "deploy_args"
    )


def test_explicit_disable_clears_inherited_uri_and_skips_resolver(tmp_path: Path) -> None:
    result = run_deploy(
        tmp_path,
        ["--without-average-user-embedding"],
        {
            "GE_AVERAGE_USER_EMBEDDING_URI": "gs://ignored/artifact.json",
            "FAKE_RESOLVER_STATUS": "1",
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert not (tmp_path / "resolver_args").exists()
    assert "--set-env-vars=GE_AVERAGE_USER_EMBEDDING_URI=" in recorded_args(tmp_path, "deploy_args")


def test_missing_or_invalid_selection_stops_before_external_writes(tmp_path: Path) -> None:
    result = run_deploy(tmp_path, environment={"FAKE_RESOLVER_STATUS": "1"})

    assert result.returncode != 0
    assert "No promoted default was found" in result.stderr
    assert "Promote a valid artifact" in result.stdout
    assert "--without-average-user-embedding" in result.stdout
    assert (tmp_path / "events").read_text().splitlines() == [
        "clean_worktree",
        "configure_project",
        "resolve_embedding",
    ]
    assert not (tmp_path / "deploy_args").exists()


@pytest.mark.parametrize(
    "resolver_output",
    ["", "gs://bucket/a.json\ngs://bucket/b.json", "gs://bucket/$(touch injected).json"],
)
def test_unexpected_resolver_output_cannot_reach_deployment(
    tmp_path: Path, resolver_output: str
) -> None:
    result = run_deploy(tmp_path, environment={"FAKE_RESOLVER_OUTPUT": resolver_output})

    assert result.returncode != 0
    assert "single valid GCS URI" in result.stdout
    assert not (tmp_path / "deploy_args").exists()
    assert not (tmp_path / "injected").exists()


@pytest.mark.parametrize(
    "arguments",
    [
        ["--average-user-embedding-uri"],
        ["--average-user-embedding-uri", "--without-average-user-embedding"],
        ["--average-user-embedding-uri", "gs://bucket/a.json", "--without-average-user-embedding"],
        ["--without-average-user-embedding", "--average-user-embedding-uri", "gs://bucket/a.json"],
    ],
)
def test_invalid_options_fail_before_resolving_or_deploying(
    tmp_path: Path, arguments: list[str]
) -> None:
    result = run_deploy(tmp_path, arguments)

    assert result.returncode != 0
    assert not (tmp_path / "events").exists()

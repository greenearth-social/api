"""Check setup permissions without invoking cloud commands."""

import os
import subprocess
from pathlib import Path

import pytest

SETUP_SCRIPT = Path(__file__).with_name("gcp_setup.sh")

# Exercise the actual parser, main sequence, and bucket grant. All other setup
# operations are stubs, and gcloud only records the intended bucket IAM command.
HARNESS = r"""
record() { printf '%s\n' "$1" >> "$TEST_DIRECTORY/events"; }
gcloud() {
    if [ "$1 $2 $3" != "storage buckets add-iam-policy-binding" ]; then
        printf '%s\n' "Unexpected cloud command" >&2
        return 90
    fi
    record grant_bucket_access
    printf '%s\0' "$@" > "$TEST_DIRECTORY/grant_args"
    return "${FAKE_GRANT_STATUS:-0}"
}

setup_script="$1"
shift
source "$setup_script" "$@"

for step in \
    check_prerequisites setup_gcp_project create_service_account \
    ensure_frontend_deployer_roles ensure_firestore_database \
    ensure_firestore_api_key_secret ensure_inference_api_key_secret_access \
    ensure_feed_context_secret ensure_probe_secret ensure_load_test_secret \
    setup_secrets setup_bsky_secret setup_perspective_secret setup_posthog_secret \
    check_vpc_connector setup_feed_probe_cloud_scheduler; do
    eval "$step() { record '$step'; }"
done
main
"""


def run_setup(
    tmp_path: Path, arguments: list[str] | None = None, *, grant_status: int = 0
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        TEST_DIRECTORY=str(tmp_path),
        FAKE_GRANT_STATUS=str(grant_status),
        # Prevent accidentally finding locally installed cloud tools as well.
        PATH="/usr/bin:/bin",
    )
    return subprocess.run(
        [
            "/bin/bash",
            "-c",
            HARNESS,
            "setup-test",
            str(SETUP_SCRIPT),
            "--no-fetch-es-key",
            *(arguments or []),
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )


def test_grants_frontend_deployer_firestore_configuration_role():
    script = SETUP_SCRIPT.read_text()

    assert '"roles/datastore.indexAdmin"' in script


@pytest.mark.parametrize(
    ("arguments", "environment_name", "project"),
    [
        ([], "stage", "greenearth-471522"),
        (["--environment", "prod"], "prod", "greenearth-471522"),
        (["--project-id", "another-project"], "stage", "another-project"),
    ],
)
def test_grants_runtime_read_access_only_to_environment_bucket(
    tmp_path: Path, arguments: list[str], environment_name: str, project: str
) -> None:
    result = run_setup(tmp_path, arguments)

    assert result.returncode == 0, result.stdout + result.stderr
    grant_args = (tmp_path / "grant_args").read_text().removesuffix("\0").split("\0")
    assert grant_args == [
        "storage",
        "buckets",
        "add-iam-policy-binding",
        f"gs://{project}-engagement-prediction-model-{environment_name}",
        f"--member=serviceAccount:api-runner-{environment_name}@{project}.iam.gserviceaccount.com",
        "--role=roles/storage.objectViewer",
        f"--project={project}",
        "--condition=None",
    ]
    events = (tmp_path / "events").read_text().splitlines()
    account_index = events.index("create_service_account")
    assert events[account_index : account_index + 3] == [
        "create_service_account",
        "grant_bucket_access",
        "ensure_frontend_deployer_roles",
    ]
    assert events.count("grant_bucket_access") == 1
    assert events[-1] == "setup_feed_probe_cloud_scheduler"
    assert "GCP setup complete!" in result.stdout


def test_failed_bucket_grant_stops_setup_without_reporting_success(tmp_path: Path) -> None:
    result = run_setup(tmp_path, grant_status=1)

    assert result.returncode != 0
    assert "Could not grant average user embedding read access" in result.stdout + result.stderr
    assert "greenearth-471522-engagement-prediction-model-stage" in result.stdout + result.stderr
    assert "GCP setup complete!" not in result.stdout
    assert (tmp_path / "events").read_text().splitlines() == [
        "check_prerequisites",
        "setup_gcp_project",
        "create_service_account",
        "grant_bucket_access",
    ]

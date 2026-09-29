import subprocess
from pathlib import Path

SCRIPT = Path(__file__).with_name("deploy.sh")

# deploy.sh calls main() unconditionally as its last line, and main() shells out to
# git/gcloud. Source everything up to (but not including) that final call so the
# function definitions load without actually deploying anything, then invoke
# resolve_oauth_revoke_url() directly with a controlled environment.
_SCRIPT_BODY = SCRIPT.read_text().rsplit("\nmain\n", 1)[0]


def _run_resolve(environment: str, url: str | None) -> subprocess.CompletedProcess:
    env_lines = [f'ENVIRONMENT="{environment}"']
    env_lines.append(f'GE_OAUTH_REVOKE_URL="{url}"' if url is not None else "unset GE_OAUTH_REVOKE_URL")
    script = "\n".join(
        [_SCRIPT_BODY, *env_lines, "resolve_oauth_revoke_url", 'echo "RESULT=$GE_OAUTH_REVOKE_URL"']
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


def test_defaults_to_the_stage_function_url_when_unset():
    result = _run_resolve("stage", None)
    assert result.returncode == 0, result.stderr
    assert "RESULT=https://us-central1-greenearth-471522.cloudfunctions.net/oauthRevokeStage" in result.stdout


def test_defaults_to_the_prod_function_url_when_unset():
    result = _run_resolve("prod", None)
    assert result.returncode == 0, result.stderr
    assert "RESULT=https://us-central1-greenearth-471522.cloudfunctions.net/oauthRevoke" in result.stdout
    assert "oauthRevokeStage" not in result.stdout


def test_accepts_an_https_override_in_stage():
    url = "https://oauthrevokestage-abc-uc.a.run.app"
    result = _run_resolve("stage", url)
    assert result.returncode == 0, result.stderr
    assert f"RESULT={url}" in result.stdout


def test_rejects_a_plain_http_override_in_stage():
    result = _run_resolve("stage", "http://firebase:15001/greenearth-471522/us-central1/oauthRevoke")
    assert result.returncode != 0
    assert "https://" in result.stdout


def test_rejects_a_plain_http_override_in_prod():
    result = _run_resolve("prod", "http://firebase:15001/greenearth-471522/us-central1/oauthRevoke")
    assert result.returncode != 0
    assert "https://" in result.stdout

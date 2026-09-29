from pathlib import Path


def test_grants_frontend_deployer_firestore_configuration_role():
    script = Path(__file__).with_name("gcp_setup.sh").read_text()

    assert '"roles/datastore.indexAdmin"' in script
    assert "create_service_account\n    ensure_frontend_deployer_roles" in script


def _function_body(script: str, name: str) -> str:
    start = script.index(f"{name}() {{")
    return script[start : script.index("\n}\n", start)]


def test_oauth_session_key_secret_names_match_the_firebase_function_secrets():
    body = _function_body(Path(__file__).with_name("gcp_setup.sh").read_text(), "get_oauth_session_key_secret")
    assert 'echo "OAUTH_SESSION_ENCRYPTION_KEY"' in body
    assert 'echo "OAUTH_SESSION_ENCRYPTION_KEY_STAGE"' in body


def test_oauth_session_key_is_generated_once_and_never_rotated():
    script = Path(__file__).with_name("gcp_setup.sh").read_text()
    body = _function_body(script, "ensure_oauth_session_key_secret")
    assert "openssl rand -hex 32" in body
    assert "gcloud secrets describe" in body
    assert "gcloud secrets create" in body
    assert "versions add" not in body
    assert "preserving value" in body
    assert "add-iam-policy-binding" not in body


def test_main_ensures_the_oauth_session_key_after_the_feed_context_secret():
    script = Path(__file__).with_name("gcp_setup.sh").read_text()
    main_body = script[script.index("\nmain() {") :]
    assert main_body.index("ensure_feed_context_secret") < main_body.index(
        "ensure_oauth_session_key_secret"
    )

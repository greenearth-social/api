from pathlib import Path


def test_grants_frontend_deployer_firestore_configuration_role():
    script = Path(__file__).with_name("gcp_setup.sh").read_text()

    assert '"roles/datastore.indexAdmin"' in script
    assert "create_service_account\n    ensure_frontend_deployer_roles" in script

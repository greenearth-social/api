from pathlib import Path


def test_configures_feed_snapshot_ttl_policy():
    script = Path(__file__).with_name("gcp_setup.sh").read_text()

    assert "ensure_ttl_policy feed_snapshots" in script

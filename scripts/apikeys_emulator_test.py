"""Real transaction tests, opt-in and restricted to a local demo-project emulator."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from google.auth.credentials import AnonymousCredentials
from google.cloud.firestore import AsyncClient

from app.lib import api_keys as keys

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db(monkeypatch):
    host = os.environ.get("FIRESTORE_API_KEYS_TEST_EMULATOR_HOST")
    if not host:
        pytest.skip("Set FIRESTORE_API_KEYS_TEST_EMULATOR_HOST for local transaction tests")
    if host.split(":")[0] not in {"127.0.0.1", "localhost"}:
        pytest.fail("API key tests require a loopback emulator")
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", host)
    client = AsyncClient(
        project="demo-api-keys-" + uuid4().hex[:12], credentials=AnonymousCredentials()
    )
    yield client
    for collection in [keys.API_KEYS_COLLECTION, keys.API_KEY_OWNERS_COLLECTION]:
        async for snap in client.collection(collection).stream():
            await snap.reference.delete()
    client.close()


async def seed(db, key_id, email="alice@example.com", used_days=0, admin=False):
    now = datetime.now(UTC)
    data = dict(
        key_id=key_id,
        email=email,
        key_hash="legacy-hash",
        is_active=True,
        is_admin=admin,
        created_at=now - timedelta(days=10),
        last_used_at=now - timedelta(days=used_days),
        monthly_call_count=1,
        monthly_period=now.strftime("%Y-%m"),
    )
    await db.collection(keys.API_KEYS_COLLECTION).document(key_id).set(data)


async def test_concurrent_generate_allows_exactly_one_normalized_owner(db):
    results = await asyncio.gather(
        keys.create_api_key(db, " Alice@Example.com "),
        keys.create_api_key(db, "alice@example.com"),
        return_exceptions=True,
    )
    assert sum(isinstance(result, tuple) for result in results) == 1, results
    assert sum(isinstance(result, ValueError) for result in results) == 1, results
    docs = await keys.list_api_keys(db)
    assert len(docs) == 1 and docs[0].email == "alice@example.com"


async def test_generate_detects_legacy_mixed_case_owner_without_index(db):
    await seed(db, "legacy01", " Alice@EXAMPLE.COM ")
    with pytest.raises(ValueError, match="rotate"):
        await keys.create_api_key(db, "alice@example.com")
    assert len(await keys.list_api_keys(db)) == 1


async def test_rotation_is_atomic_preserves_admin_and_cannot_be_replayed(db):
    original, old_key = await keys.create_api_key(db, "alice@example.com", is_admin=True)
    replacement, new_key = await keys.rotate_api_key(db, original.key_id)
    assert replacement.is_admin and replacement.key_id != original.key_id
    assert await keys.authenticate_api_key(db, old_key) is None
    assert (await keys.authenticate_api_key(db, new_key)).key_id == replacement.key_id
    with pytest.raises(ValueError):
        await keys.rotate_api_key(db, original.key_id)
    assert sum(d.is_active for d in await keys.list_api_keys(db)) == 1


async def test_concurrent_rotations_create_only_one_replacement(db):
    original, _ = await keys.create_api_key(db, "alice@example.com")
    results = await asyncio.gather(
        *(keys.rotate_api_key(db, original.key_id) for _ in range(2)),
        return_exceptions=True,
    )
    assert sum(isinstance(result, tuple) for result in results) == 1, results
    assert len(await keys.list_api_keys(db)) == 2
    assert sum(d.is_active for d in await keys.list_api_keys(db)) == 1


async def test_reconcile_dry_run_and_apply_keep_latest_used_and_seed_index(db):
    await seed(db, "older001", used_days=3)
    await seed(db, "latest01", used_days=1)
    decision = (await keys.reconcile_api_keys(db))[0]
    assert decision.keep_id == "latest01" and decision.revoke_ids == ("older001",)
    assert all(d.is_active for d in await keys.list_api_keys(db))
    assert not [s async for s in db.collection(keys.API_KEY_OWNERS_COLLECTION).stream()]
    await keys.reconcile_api_keys(db, apply=True)
    docs = {d.key_id: d for d in await keys.list_api_keys(db)}
    assert docs["latest01"].is_active and not docs["older001"].is_active
    assert docs["older001"].key_hash == "legacy-hash"
    with pytest.raises(ValueError):
        await keys.create_api_key(db, "alice@example.com")
    await keys.revoke_api_key(db, "latest01")
    await keys.create_api_key(db, "alice@example.com")
    assert sum(d.is_active for d in await keys.list_api_keys(db)) == 1


async def test_cleanup_rechecks_latest_usage_at_apply_time(db):
    await seed(db, "older001", used_days=3)
    await seed(db, "latest01", used_days=1)
    assert (await keys.reconcile_api_keys(db))[0].keep_id == "latest01"
    await (
        db.collection(keys.API_KEYS_COLLECTION)
        .document("older001")
        .update({"last_used_at": datetime.now(UTC)})
    )
    assert (await keys.reconcile_api_keys(db, apply=True))[0].keep_id == "older001"


async def test_bad_keep_id_or_ambiguous_owner_leaves_every_record_unchanged(db):
    await seed(db, "older001", used_days=3)
    await seed(db, "latest01", used_days=1)
    with pytest.raises(ValueError):
        await keys.reconcile_api_keys(db, apply=True, keep_ids=["missing1"])
    assert all(d.is_active for d in await keys.list_api_keys(db))


async def test_scoped_rotation_preserves_all_capabilities(db):
    original, old_key = await keys.create_api_key(
        db, "scoped@example.com", is_admin=True, scopes=["user-config", "reports"]
    )
    replacement, new_key = await keys.rotate_api_key(db, original.key_id)
    assert replacement.scopes == ["default", "user-config", "reports"]
    assert await keys.authenticate_api_key(db, old_key) is None
    assert (await keys.authenticate_api_key(db, new_key)).scopes == replacement.scopes

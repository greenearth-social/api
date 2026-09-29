import asyncio
import os

import httpx
import pytest
from google.cloud.firestore import FieldFilter

import delete_user
from app.lib.user_history_cache import USER_HISTORY_CACHE_COLLECTION, _user_history_cache_key
from delete_user import NOT_DELETED_FROM_CODE, grant_state, parse_args, revoke_via_api, run

DID = "did:plc:target"
KEY = "target"
OTHER = "did:plc:other"

_FIRESTORE_ENV_VARS = (
    "GE_FIRESTORE_PROJECT",
    "GE_FIRESTORE_DATABASE",
    "GE_FIRESTORE_EMULATOR_HOST",
    "FIRESTORE_EMULATOR_HOST",
)


@pytest.fixture(autouse=True)
def _isolate_firestore_env(monkeypatch):
    """Guarantee every var ``_configure_environment`` can write is restored.

    ``monkeypatch.delenv(name, raising=False)`` is a no-op — and registers no
    teardown — when ``name`` is already absent, so a test that only calls
    ``delenv`` on a currently-unset var leaves whatever ``_configure_environment``
    later writes via a raw ``os.environ[...] = `` assignment permanently in
    place. Forcing a ``setenv`` here first makes monkeypatch record the true
    pre-test state (present or absent) for every var this module touches, so
    it's always undone at teardown regardless of what an individual test does.
    """
    for name in _FIRESTORE_ENV_VARS:
        monkeypatch.setenv(name, "autouse-sentinel")


class FakeSnap:
    def __init__(self, ref, data):
        self.reference = ref
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return self._data


class FakeDocRef:
    def __init__(self, db, path):
        self._db, self.path = db, path

    async def get(self):
        return FakeSnap(self, self._db.docs.get(self.path))

    async def delete(self):
        self._db.docs.pop(self.path, None)

    def collection(self, name):
        return FakeCollRef(self._db, self.path + (name,))

    async def collections(self):
        n = len(self.path)
        names = sorted({p[n] for p in self._db.docs if p[:n] == self.path and len(p) > n + 1})
        for name in names:
            yield self.collection(name)


class FakeQuery:
    def __init__(self, coll, field, value):
        self._coll, self._field, self._value = coll, field, value

    def select(self, _fields):
        return self

    async def stream(self):
        n = len(self._coll.path)
        for path, data in list(self._coll._db.docs.items()):
            if len(path) == n + 1 and path[:n] == self._coll.path:
                if data.get(self._field) == self._value:
                    yield FakeSnap(FakeDocRef(self._coll._db, path), data)


class FakeCollRef:
    def __init__(self, db, path):
        self._db, self.path = db, path

    def document(self, doc_id):
        return FakeDocRef(self._db, self.path + (doc_id,))

    def where(self, *, filter):
        assert isinstance(filter, FieldFilter) and filter.op_string == "=="
        return FakeQuery(self, filter.field_path, filter.value)

    async def list_documents(self):
        n = len(self.path)
        ids = sorted({p[n] for p in self._db.docs if p[:n] == self.path and len(p) > n})
        for doc_id in ids:
            yield self.document(doc_id)


class FakeBatch:
    def __init__(self, db):
        self._db, self._refs = db, []

    def delete(self, ref):
        self._refs.append(ref)

    async def commit(self):
        self._db.commits.append(len(self._refs))
        for ref in self._refs:
            self._db.docs.pop(ref.path, None)


class FakeDb:
    def __init__(self):
        self.docs = {}
        self.commits = []
        self.recursive_deletes = []

    def collection(self, name):
        return FakeCollRef(self, (name,))

    def batch(self):
        return FakeBatch(self)

    async def recursive_delete(self, ref):
        self.recursive_deletes.append(ref.path)
        n = len(ref.path)
        doomed = [p for p in self.docs if p[:n] == ref.path]
        for p in doomed:
            del self.docs[p]
        return len(doomed)


class FakeAuth:
    def __init__(self, uids=()):
        self.uids, self.deleted = set(uids), []

    def exists(self, uid):
        return uid in self.uids

    def delete(self, uid):
        self.uids.discard(uid)
        self.deleted.append(uid)


def seeded_db(interactions=2):
    db = FakeDb()
    db.docs[("users", KEY)] = {"user_did": DID}
    db.docs[("users", KEY, "feed_debug", "r1")] = {"x": 1}
    db.docs[("users", KEY, "seen_posts", "2026-01-01")] = {"x": 1}
    db.docs[("users", KEY, "feed_snapshots", "s1")] = {"x": 1}
    db.docs[("followed_users_cache", KEY)] = {"x": 1}
    db.docs[("feed_cache", "c1")] = {"user_did": DID}
    db.docs[("feed_cache", "c2")] = {"user_did": OTHER}
    db.docs[(USER_HISTORY_CACHE_COLLECTION, _user_history_cache_key(DID))] = {"x": 1}
    db.docs[(USER_HISTORY_CACHE_COLLECTION, _user_history_cache_key(OTHER))] = {"x": 1}
    for i in range(interactions):
        db.docs[("interactions", f"i{i}")] = {"user_did": DID}
    db.docs[("interactions", "other")] = {"user_did": OTHER}
    db.docs[("users", "other")] = {"user_did": OTHER}
    return db


def args(execute=False, did=DID):
    return parse_args(["--did", did] + (["--execute"] if execute else []))


class _FakeRevoker:
    def __init__(self, outcome: str) -> None:
        self.outcome = outcome
        self.calls: list[str] = []

    async def __call__(self, did: str) -> str:
        self.calls.append(did)
        return self.outcome


def revoker_returning(outcome: str) -> _FakeRevoker:
    return _FakeRevoker(outcome)


def test_dry_run_is_the_default_and_makes_no_changes(capsys):
    db, auth = seeded_db(), FakeAuth({DID})
    before = dict(db.docs)
    assert asyncio.run(run(args(), db, auth, revoker_returning("no_session"))) == 0
    assert db.docs == before
    assert auth.deleted == [] and db.commits == [] and db.recursive_deletes == []
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "would delete" in out


def test_execute_deletes_only_the_target_users_data():
    db, auth = seeded_db(), FakeAuth({DID})
    assert asyncio.run(run(args(execute=True), db, auth, revoker_returning("no_session"))) == 0
    assert not [p for p in db.docs if p[:2] == ("users", KEY)]
    assert ("followed_users_cache", KEY) not in db.docs
    assert ("feed_cache", "c1") not in db.docs
    assert not [p for p in db.docs if p[0] == "interactions" and db.docs[p]["user_did"] == DID]
    assert (USER_HISTORY_CACHE_COLLECTION, _user_history_cache_key(DID)) not in db.docs
    assert ("users", "other") in db.docs
    assert ("feed_cache", "c2") in db.docs
    assert ("interactions", "other") in db.docs
    assert (USER_HISTORY_CACHE_COLLECTION, _user_history_cache_key(OTHER)) in db.docs
    assert auth.deleted == [DID]


def test_execute_is_idempotent(capsys):
    db, auth = seeded_db(), FakeAuth({DID})
    assert asyncio.run(run(args(execute=True), db, auth, revoker_returning("no_session"))) == 0
    capsys.readouterr()
    assert asyncio.run(run(args(execute=True), db, auth, revoker_returning("no_session"))) == 0
    assert auth.deleted == [DID]
    assert "0" in capsys.readouterr().out


def test_large_interaction_sets_are_deleted_in_batches_of_500():
    db, auth = seeded_db(interactions=1203), FakeAuth()
    asyncio.run(run(args(execute=True), db, auth, revoker_returning("no_session")))
    assert db.commits == [500, 500, 203, 1]
    assert not [p for p in db.docs if p[0] == "interactions" and db.docs[p]["user_did"] == DID]


def test_report_always_lists_what_cannot_be_deleted(capsys):
    asyncio.run(run(args(), seeded_db(), FakeAuth(), revoker_returning("no_session")))
    out = capsys.readouterr().out
    for line in NOT_DELETED_FROM_CODE:
        assert line in out
    assert "OAuth grant: none stored" in out


def test_both_mode_flags_are_rejected():
    with pytest.raises(SystemExit) as exc:
        parse_args(["--did", DID, "--dry-run", "--execute"])
    assert exc.value.code == 2


@pytest.mark.parametrize("bad", ["", "did:plc:", "nope", "did:plc:a/b"])
def test_malformed_did_is_rejected(bad):
    with pytest.raises(SystemExit) as exc:
        parse_args(["--did", bad])
    assert exc.value.code == 2


def test_a_failing_store_returns_nonzero(capsys):
    class ExplodingAuth(FakeAuth):
        def exists(self, uid):
            raise RuntimeError("auth down")

    revoker = revoker_returning("no_session")
    assert asyncio.run(run(args(execute=True), seeded_db(), ExplodingAuth(), revoker)) == 1
    assert "auth down" in capsys.readouterr().err


def test_environment_flag_defaults_to_dev():
    assert args().environment == "dev"


@pytest.mark.parametrize("flag", ["--environment", "--env"])
def test_environment_flag_and_its_alias_both_select_stage(flag):
    assert parse_args(["--did", DID, flag, "stage"]).environment == "stage"


def test_dev_environment_leaves_firestore_env_untouched(monkeypatch):
    monkeypatch.setenv("GE_FIRESTORE_PROJECT", "existing-project")
    monkeypatch.setenv("GE_FIRESTORE_DATABASE", "existing-db")
    monkeypatch.setenv("GE_FIRESTORE_EMULATOR_HOST", "localhost:8080")
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080")

    delete_user._configure_environment("dev")

    assert os.environ["GE_FIRESTORE_PROJECT"] == "existing-project"
    assert os.environ["GE_FIRESTORE_DATABASE"] == "existing-db"
    assert os.environ["GE_FIRESTORE_EMULATOR_HOST"] == "localhost:8080"
    assert os.environ["FIRESTORE_EMULATOR_HOST"] == "localhost:8080"


@pytest.mark.parametrize(
    "env,expected_database",
    [("stage", "greenearth-stage"), ("prod", "greenearth-prod")],
)
def test_stage_and_prod_set_project_and_database_and_clear_emulator_host(
    monkeypatch, env, expected_database
):
    monkeypatch.delenv("GE_FIRESTORE_PROJECT", raising=False)
    monkeypatch.delenv("GE_FIRESTORE_DATABASE", raising=False)
    monkeypatch.setenv("GE_FIRESTORE_EMULATOR_HOST", "localhost:8080")
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080")

    delete_user._configure_environment(env)

    assert os.environ["GE_FIRESTORE_PROJECT"] == "greenearth-471522"
    assert os.environ["GE_FIRESTORE_DATABASE"] == expected_database
    assert "GE_FIRESTORE_EMULATOR_HOST" not in os.environ
    assert "FIRESTORE_EMULATOR_HOST" not in os.environ


def with_grant(db, status="active"):
    doc = {"did": DID, "status": status}
    if status == "active":
        doc["ciphertext"] = "opaque"
    db.docs[("oauth_grants", DID)] = doc
    return db


def run_args(*flags, did=DID):
    return parse_args(["--did", did, *flags])


@pytest.mark.parametrize("outcome", ["revoked", "already_revoked", "no_session"])
def test_execute_revokes_first_then_deletes_data_and_the_grant(outcome, capsys):
    db = with_grant(seeded_db())
    revoker = revoker_returning(outcome)
    assert asyncio.run(run(run_args("--execute"), db, FakeAuth([DID]), revoker)) == 0
    assert revoker.calls == [DID]
    assert ("users", KEY) not in db.docs
    assert ("oauth_grants", DID) not in db.docs
    assert ("users", "other") in db.docs
    assert outcome in capsys.readouterr().out


def test_failed_revocation_exits_nonzero_and_deletes_nothing(capsys):
    db = with_grant(seeded_db())
    before = dict(db.docs)
    auth = FakeAuth([DID])
    assert asyncio.run(run(run_args("--execute"), db, auth, revoker_returning("failed"))) == 1
    assert db.docs == before and auth.deleted == []
    assert "revocation failed" in capsys.readouterr().err.lower()


def test_revocation_happens_before_any_deletion():
    db = with_grant(seeded_db())
    order = []

    async def revoker(did):
        order.append(("revoke", ("users", KEY) in db.docs))
        return "revoked"

    asyncio.run(run(run_args("--execute"), db, FakeAuth([DID]), revoker))
    assert order == [("revoke", True)]


def test_dry_run_never_calls_the_revoker_and_reports_the_grant(capsys):
    db = with_grant(seeded_db())
    before = dict(db.docs)
    revoker = revoker_returning("revoked")
    assert asyncio.run(run(run_args(), db, FakeAuth([DID]), revoker)) == 0
    assert revoker.calls == [] and db.docs == before
    assert "would revoke" in capsys.readouterr().out


@pytest.mark.parametrize("status, expected", [("active", "active"), ("revoked", "revoked")])
def test_grant_state_reads_the_grant_document(status, expected):
    assert asyncio.run(grant_state(with_grant(seeded_db(), status), DID)) == expected


def test_grant_state_absent_when_there_is_no_document():
    assert asyncio.run(grant_state(seeded_db(), DID)) == "absent"


def test_skip_flag_is_refused_when_an_active_grant_exists(capsys):
    db = with_grant(seeded_db())
    before = dict(db.docs)
    revoker = revoker_returning("revoked")
    code = asyncio.run(run(run_args("--execute", "--skip-oauth-revocation"), db, FakeAuth([DID]), revoker))
    assert code == 1 and db.docs == before and revoker.calls == []
    assert "active" in capsys.readouterr().err


@pytest.mark.parametrize("status", [None, "revoked"])
def test_skip_flag_allowed_without_an_active_grant_and_makes_no_revoke_call(status):
    db = with_grant(seeded_db(), status) if status else seeded_db()
    revoker = revoker_returning("revoked")
    code = asyncio.run(run(run_args("--execute", "--skip-oauth-revocation"), db, FakeAuth([DID]), revoker))
    assert code == 0 and revoker.calls == []
    assert ("users", KEY) not in db.docs and ("oauth_grants", DID) not in db.docs


def test_rerun_after_success_is_a_clean_noop():
    db = with_grant(seeded_db())
    asyncio.run(run(run_args("--execute"), db, FakeAuth([DID]), revoker_returning("revoked")))
    assert asyncio.run(run(run_args("--execute"), db, FakeAuth(), revoker_returning("no_session"))) == 0


def test_report_lists_the_audit_log_as_retained(capsys):
    asyncio.run(run(run_args(), seeded_db(), FakeAuth(), revoker_returning("no_session")))
    assert "oauth_revocations" in capsys.readouterr().out


def test_failed_revocation_report_never_prints_the_admin_key(capsys):
    db = with_grant(seeded_db())
    asyncio.run(run(run_args("--execute"), db, FakeAuth([DID]), revoker_returning("failed")))
    captured = capsys.readouterr()
    assert "gea_secret" not in captured.out and "gea_secret" not in captured.err


def _transport(status, body):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler), seen


@pytest.fixture(autouse=True)
def _api_env(monkeypatch):
    monkeypatch.setenv("GE_API_URL", "http://api.test")
    monkeypatch.setenv("GE_ADMIN_API_KEY", "gea_secret")


def test_revoke_via_api_posts_the_did_with_the_admin_key():
    t, seen = _transport(200, {"did": DID, "outcome": "revoked"})
    out = asyncio.run(revoke_via_api(DID, client=httpx.AsyncClient(transport=t)))
    assert out == "revoked"
    assert str(seen[0].url) == "http://api.test/api/oauth/revoke"
    assert seen[0].headers["x-api-key"] == "gea_secret"


@pytest.mark.parametrize(
    "status, body", [(502, {"outcome": "failed"}), (403, {}), (500, {}), (200, {"outcome": "bogus"})]
)
def test_revoke_via_api_maps_everything_else_to_failed(status, body):
    t, _ = _transport(status, body)
    assert asyncio.run(revoke_via_api(DID, client=httpx.AsyncClient(transport=t))) == "failed"


def test_revoke_via_api_without_configuration_is_failed(monkeypatch):
    monkeypatch.delenv("GE_ADMIN_API_KEY")
    assert asyncio.run(revoke_via_api(DID)) == "failed"


def test_revoke_via_api_transport_errors_are_failed():
    def boom(request):
        raise httpx.ConnectError("down")

    assert (
        asyncio.run(revoke_via_api(DID, client=httpx.AsyncClient(transport=httpx.MockTransport(boom))))
        == "failed"
    )

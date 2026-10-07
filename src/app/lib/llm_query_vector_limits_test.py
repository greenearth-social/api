"""Quota decisions and transaction retry behavior without Firestore network access."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from google.api_core.exceptions import Aborted
from starlette.requests import Request

from ..routers import llm_query_vectors as router
from . import llm_query_vector_limits as limits
from .llm_query_vector_fit import FitError, PoolTooSmallError
from .llm_query_vector_limits import FitQuotaDecision, reserve_fit_quota

NOW = datetime(2026, 10, 7, 12, tzinfo=UTC)
USER_DID = "did:plc:quota-user"
USER_PATH = "users/quota-user/rate_limits/llm_query_vector_fit"
DAILY_PATH = "llm_query_vector_fit_daily/2026-10-07"


class QuotaStore:
    """Stage document writes until commit while using the real SDK retry wrapper."""

    def __init__(self):
        self.documents: dict[str, dict] = {}
        self.pending: dict[str, dict] = {}
        self.operations: list[tuple[str, str]] = []
        self.db = MagicMock()
        self.transaction = MagicMock()
        self.transaction._read_only = False
        self.transaction._max_attempts = 5
        self.transaction._id = b"quota-test-transaction"
        self.transaction._begin = AsyncMock()
        self.transaction._commit = AsyncMock(side_effect=self.commit)
        self.transaction._rollback = AsyncMock(side_effect=self.pending.clear)
        self.transaction._clean_up.side_effect = self.pending.clear
        self.transaction.set.side_effect = self.set
        self.db.transaction.return_value = self.transaction
        self.db.collection.side_effect = self.collection

    def collection(self, path):
        collection = MagicMock()
        collection.document.side_effect = lambda doc_id: self.reference(f"{path}/{doc_id}")
        return collection

    def reference(self, path):
        ref = MagicMock()
        ref.path = path
        ref.collection.side_effect = lambda name: self.collection(f"{path}/{name}")

        async def get(*, transaction):
            assert transaction is self.transaction
            assert not self.pending, "Firestore disallows reads after staged writes"
            self.operations.append(("get", path))
            snapshot = MagicMock()
            snapshot.exists = path in self.documents
            snapshot.to_dict.return_value = deepcopy(self.documents.get(path))
            return snapshot

        ref.get = AsyncMock(side_effect=get)
        return ref

    def set(self, ref, data):
        self.operations.append(("set", ref.path))
        self.pending[ref.path] = deepcopy(data)

    async def commit(self):
        self.documents.update(self.pending)
        self.pending.clear()


@pytest.fixture
def store():
    return QuotaStore()


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = NOW
    monkeypatch.setattr(limits, "datetime", clock)
    return clock


@pytest.mark.asyncio
async def test_first_admission_creates_both_documents_after_reading_both(store):
    result = await reserve_fit_quota(store.db, USER_DID)

    assert result == FitQuotaDecision()
    assert store.documents == {
        USER_PATH: {"attempts": [NOW]},
        DAILY_PATH: {"count": 1},
    }
    assert store.operations == [
        ("get", USER_PATH),
        ("get", DAILY_PATH),
        ("set", USER_PATH),
        ("set", DAILY_PATH),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("user_count", "daily_count", "scopes", "retry_at"),
    [
        (11, 10, ("user",), NOW + timedelta(hours=5)),
        (1, 999, ("global",), datetime(2026, 10, 8, tzinfo=UTC)),
        (11, 999, ("user", "global"), datetime(2026, 10, 8, tzinfo=UTC)),
    ],
)
async def test_last_slot_is_admitted_and_next_request_rejected_without_writes(
    store, user_count, daily_count, scopes, retry_at
):
    store.documents[USER_PATH] = {"attempts": [NOW - timedelta(hours=1)] * user_count}
    store.documents[DAILY_PATH] = {"count": daily_count}

    admitted = await reserve_fit_quota(store.db, USER_DID)

    assert admitted == FitQuotaDecision(reached_scopes=scopes)
    assert len(store.documents[USER_PATH]["attempts"]) == user_count + 1
    assert store.documents[DAILY_PATH]["count"] == daily_count + 1
    committed = deepcopy(store.documents)
    store.operations.clear()

    rejected = await reserve_fit_quota(store.db, USER_DID)

    assert rejected == FitQuotaDecision(exhausted_scopes=scopes, retry_at=retry_at)
    assert store.documents == committed
    assert store.operations == [("get", USER_PATH), ("get", DAILY_PATH)]


@pytest.mark.asyncio
async def test_rolling_window_expires_exact_cutoff_and_discards_older_timestamps(store):
    recent = [NOW - timedelta(hours=1)] * 11
    store.documents[USER_PATH] = {
        "attempts": [NOW - timedelta(hours=7), NOW - limits.USER_FIT_WINDOW, *recent]
    }

    result = await reserve_fit_quota(store.db, USER_DID)

    assert result == FitQuotaDecision(reached_scopes=("user",))
    assert store.documents[USER_PATH]["attempts"] == [*recent, NOW]
    assert store.documents[DAILY_PATH] == {"count": 1}


@pytest.mark.asyncio
async def test_rolling_quota_can_fill_again_when_oldest_attempt_expires(store, clock):
    oldest = NOW - timedelta(hours=5)
    # Ordering is not needed for the earliest retry calculation.
    store.documents[USER_PATH] = {"attempts": [NOW] * 11 + [oldest]}

    rejected = await reserve_fit_quota(store.db, USER_DID)

    assert rejected.retry_at == oldest + limits.USER_FIT_WINDOW
    clock.now.return_value = rejected.retry_at

    admitted = await reserve_fit_quota(store.db, USER_DID)

    assert admitted == FitQuotaDecision(reached_scopes=("user",))
    assert store.documents[USER_PATH]["attempts"] == [NOW] * 11 + [rejected.retry_at]


@pytest.mark.asyncio
@pytest.mark.parametrize("age_hours", [4, 5.5])
async def test_both_caps_report_later_retry_time(store, clock, age_hours):
    now = NOW.replace(hour=23)
    clock.now.return_value = now
    oldest = now - timedelta(hours=age_hours)
    store.documents[USER_PATH] = {"attempts": [oldest] * limits.USER_FIT_LIMIT}
    store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT}

    result = await reserve_fit_quota(store.db, USER_DID)

    assert result == FitQuotaDecision(
        exhausted_scopes=("user", "global"),
        retry_at=max(oldest + limits.USER_FIT_WINDOW, datetime(2026, 10, 8, tzinfo=UTC)),
    )
    store.transaction.set.assert_not_called()


@pytest.mark.asyncio
async def test_user_quotas_are_independent_and_share_global_count(store):
    store.documents[USER_PATH] = {"attempts": [NOW] * limits.USER_FIT_LIMIT}
    store.documents[DAILY_PATH] = {"count": 12}

    first_user = await reserve_fit_quota(store.db, USER_DID)
    second_user = await reserve_fit_quota(store.db, "did:plc:other-user")

    assert first_user.exhausted_scopes == ("user",)
    assert second_user == FitQuotaDecision()
    assert store.documents[USER_PATH] == {"attempts": [NOW] * limits.USER_FIT_LIMIT}
    assert store.documents["users/other-user/rate_limits/llm_query_vector_fit"] == {
        "attempts": [NOW]
    }
    assert store.documents[DAILY_PATH] == {"count": 13}


@pytest.mark.asyncio
async def test_global_rejection_does_not_create_user_or_prune_expired_attempts(store):
    store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT}
    store.documents[USER_PATH] = {"attempts": [NOW - timedelta(days=1)]}
    before = deepcopy(store.documents)

    existing_user = await reserve_fit_quota(store.db, USER_DID)
    new_user = await reserve_fit_quota(store.db, "did:plc:new-user")

    assert existing_user.exhausted_scopes == new_user.exhausted_scopes == ("global",)
    assert store.documents == before
    store.transaction.set.assert_not_called()


@pytest.mark.asyncio
async def test_utc_midnight_resets_global_count_but_keeps_rolling_user_attempts(store, clock):
    before_midnight = datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC)
    midnight = datetime(2027, 1, 1, tzinfo=UTC)
    clock.now.side_effect = [before_midnight, midnight]
    previous_day = "llm_query_vector_fit_daily/2026-12-31"
    next_day = "llm_query_vector_fit_daily/2027-01-01"
    store.documents[previous_day] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT - 1}

    before = await reserve_fit_quota(store.db, USER_DID)
    after = await reserve_fit_quota(store.db, USER_DID)

    assert before == FitQuotaDecision(reached_scopes=("global",))
    assert after == FitQuotaDecision()
    assert store.documents[previous_day] == {"count": limits.GLOBAL_DAILY_FIT_LIMIT}
    assert store.documents[next_day] == {"count": 1}
    assert store.documents[USER_PATH] == {"attempts": [before_midnight, midnight]}
    assert all(call.args == (UTC,) for call in clock.now.call_args_list)


@pytest.mark.asyncio
async def test_transaction_retry_resamples_time_and_returns_only_committed_decision(store, clock):
    before_midnight = datetime(2026, 10, 7, 23, 59, 59, tzinfo=UTC)
    midnight = datetime(2026, 10, 8, tzinfo=UTC)
    clock.now.side_effect = [before_midnight, midnight]
    store.documents[USER_PATH] = {"attempts": [midnight - limits.USER_FIT_WINDOW] * 11}
    store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT - 1}

    async def commit_after_conflict():
        if store.transaction._commit.await_count == 1:
            raise Aborted("retry the transaction")
        await store.commit()

    store.transaction._commit.side_effect = commit_after_conflict

    result = await reserve_fit_quota(store.db, USER_DID)

    # The aborted attempt filled both caps; the committed retry fills neither.
    assert result == FitQuotaDecision()
    assert store.documents[USER_PATH] == {"attempts": [midnight]}
    assert store.documents[DAILY_PATH] == {"count": limits.GLOBAL_DAILY_FIT_LIMIT - 1}
    assert store.documents["llm_query_vector_fit_daily/2026-10-08"] == {"count": 1}
    assert clock.now.call_count == store.transaction._commit.await_count == 2


@pytest.mark.asyncio
async def test_transaction_retry_can_reject_when_another_request_takes_final_slot(store):
    store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT - 1}

    async def commit_after_conflict():
        if store.transaction._commit.await_count == 1:
            store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT}
            raise Aborted("another request consumed the final slot")
        await store.commit()

    store.transaction._commit.side_effect = commit_after_conflict

    result = await reserve_fit_quota(store.db, USER_DID)

    assert result == FitQuotaDecision(
        exhausted_scopes=("global",), retry_at=datetime(2026, 10, 8, tzinfo=UTC)
    )
    assert USER_PATH not in store.documents
    assert store.transaction._commit.await_count == 2


@pytest.mark.asyncio
async def test_commit_failure_propagates_without_consuming_either_quota(store):
    store.transaction._commit.side_effect = RuntimeError("Firestore unavailable")

    with pytest.raises(RuntimeError, match="Firestore unavailable"):
        await reserve_fit_quota(store.db, USER_DID)

    assert store.documents == {}
    store.transaction._rollback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["pool", "model", "timeout", "cancellation", "storage"])
async def test_route_failures_keep_committed_quota_and_block_next_attempt(
    store, monkeypatch, failure
):
    store.documents[USER_PATH] = {"attempts": [NOW - timedelta(hours=1)] * 11}
    store.documents[DAILY_PATH] = {"count": 4}
    fit = AsyncMock(return_value=SimpleNamespace(query_vector=[0.1]))
    add = AsyncMock()
    collector = MagicMock()
    monkeypatch.setattr(router, "fit_query_vector", fit)
    monkeypatch.setattr(router, "add_llm_query_vector", add)
    monkeypatch.setattr(router, "get_metric_collector", lambda: collector)
    request = Request(
        {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(firestore=store.db, es=None))}
    )
    body = router.QueryVectorFitRequest(prompt="science")
    expected_error: type[BaseException] = HTTPException
    expected_status = {"pool": 422, "model": 502, "timeout": 504}
    if failure == "pool":
        fit.side_effect = PoolTooSmallError("too few posts")
    elif failure == "model":
        fit.side_effect = FitError("model failed")
    elif failure == "timeout":
        async def slow_fit(*_args):
            await asyncio.sleep(1)

        fit.side_effect = slow_fit
        monkeypatch.setattr(router, "FIT_TIMEOUT_S", 0)
    elif failure == "cancellation":
        fit.side_effect = asyncio.CancelledError()
        expected_error = asyncio.CancelledError
    else:
        add.side_effect = RuntimeError("storage failed")
        expected_error = RuntimeError

    with pytest.raises(expected_error) as error:
        await router.fit_llm_query_vector(body, request, USER_DID)

    if isinstance(error.value, HTTPException):
        assert error.value.status_code == expected_status[failure]
    committed = deepcopy(store.documents)
    assert committed[USER_PATH] == {"attempts": [NOW - timedelta(hours=1)] * 11 + [NOW]}
    assert committed[DAILY_PATH] == {"count": 5}
    fit.assert_awaited_once()
    assert add.await_count == (1 if failure == "storage" else 0)

    with pytest.raises(HTTPException) as blocked:
        await router.fit_llm_query_vector(body, request, USER_DID)

    assert blocked.value.status_code == 429
    detail = router.QueryVectorRateLimitDetail.model_validate(blocked.value.detail)
    assert detail.scopes == ["user"]
    assert store.documents == committed
    fit.assert_awaited_once()
    assert add.await_count == (1 if failure == "storage" else 0)
    collector.record.assert_called_once_with(
        "llm_query_vector.fit.cap_reached_count", 1, scope="user"
    )


@pytest.mark.asyncio
async def test_route_emits_each_cap_metric_once_after_transaction_retry(store, monkeypatch):
    store.documents[USER_PATH] = {"attempts": [NOW] * 11}
    store.documents[DAILY_PATH] = {"count": limits.GLOBAL_DAILY_FIT_LIMIT - 1}
    collector = MagicMock()
    monkeypatch.setattr(router, "get_metric_collector", lambda: collector)

    async def commit_after_conflict():
        if store.transaction._commit.await_count == 1:
            raise Aborted("retry the transaction")
        await store.commit()

    async def failed_fit(*_args):
        assert store.transaction._commit.await_count == 2
        assert collector.record.call_count == 2
        raise FitError("model failed after reservation")

    store.transaction._commit.side_effect = commit_after_conflict
    monkeypatch.setattr(router, "fit_query_vector", failed_fit)
    request = Request(
        {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(firestore=store.db, es=None))}
    )

    with pytest.raises(HTTPException) as error:
        await router.fit_llm_query_vector(
            router.QueryVectorFitRequest(prompt="science"), request, USER_DID
        )

    assert error.value.status_code == 502
    assert store.documents[USER_PATH] == {"attempts": [NOW] * 12}
    assert store.documents[DAILY_PATH] == {"count": limits.GLOBAL_DAILY_FIT_LIMIT}
    assert [call.kwargs for call in collector.record.call_args_list] == [
        {"scope": "user"}, {"scope": "global"}
    ]
    assert all(
        call.args == ("llm_query_vector.fit.cap_reached_count", 1)
        for call in collector.record.call_args_list
    )

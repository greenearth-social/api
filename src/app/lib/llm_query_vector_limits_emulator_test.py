"""Opt-in concurrency checks against a local Firestore emulator only."""

import asyncio
import os
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from google.api_core.exceptions import Aborted
from google.auth.credentials import AnonymousCredentials
from google.cloud.firestore import AsyncClient

from . import llm_query_vector_limits as limits
from .firestore import USERS_COLLECTION, user_doc_id

pytestmark = pytest.mark.asyncio

FIXED_NOW = datetime(2026, 10, 7, 12, tzinfo=UTC)
USER_DIDS = tuple(f"did:plc:quota-test-{index}" for index in range(4))


def _user_quota_ref(db: AsyncClient, user_did: str):
    return (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(limits.RATE_LIMITS_COLLECTION)
        .document(limits.FIT_QUOTA_DOCUMENT)
    )


def _daily_quota_ref(db: AsyncClient):
    return db.collection(limits.GLOBAL_FIT_QUOTA_COLLECTION).document(
        FIXED_NOW.date().isoformat()
    )


def _completed_decisions(
    results: list[limits.FitQuotaDecision | BaseException],
) -> list[limits.FitQuotaDecision]:
    decisions = []
    for result in results:
        if isinstance(result, BaseException):
            # The emulator can exhaust the SDK's contention retries. The API
            # fails closed with 503 in this case; verify persisted quotas below.
            assert isinstance(result, Aborted) or (
                isinstance(result, ValueError) and isinstance(result.__cause__, Aborted)
            ), repr(result)
        else:
            decisions.append(result)
    return decisions


@pytest_asyncio.fixture
async def quota_db(monkeypatch):
    emulator_host = os.environ.get("FIRESTORE_QUOTA_TEST_EMULATOR_HOST")
    if not emulator_host:
        pytest.skip("Set FIRESTORE_QUOTA_TEST_EMULATOR_HOST to run local emulator tests")
    # Never inherit the application's project/database or accidentally use live
    # credentials. A unique demo project also isolates simultaneous test runs.
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", emulator_host)
    db = AsyncClient(
        project=f"demo-query-quota-{uuid4().hex[:12]}",
        database="(default)",
        credentials=AnonymousCredentials(),
    )
    frozen_datetime = Mock(wraps=datetime)
    frozen_datetime.now.return_value = FIXED_NOW
    monkeypatch.setattr(limits, "datetime", frozen_datetime)
    try:
        yield db
    finally:
        refs = [_user_quota_ref(db, user_did) for user_did in USER_DIDS]
        refs.append(_daily_quota_ref(db))
        await asyncio.gather(*(ref.delete() for ref in refs))
        db.close()


@pytest.mark.parametrize("remaining_slots", [1, 2])
async def test_concurrent_user_reservations_do_not_exceed_cap(quota_db, remaining_slots):
    user_ref = _user_quota_ref(quota_db, USER_DIDS[0])
    daily_ref = _daily_quota_ref(quota_db)
    await user_ref.set({"attempts": [FIXED_NOW] * (limits.USER_FIT_LIMIT - remaining_slots)})
    await daily_ref.set({"count": 100})

    results = await asyncio.gather(
        *(
            limits.reserve_fit_quota(quota_db, USER_DIDS[0])
            for _ in range(remaining_slots + 1)
        ),
        return_exceptions=True,
    )
    decisions = _completed_decisions(results)

    assert sum(not decision.exhausted_scopes for decision in decisions) == remaining_slots
    assert sum(decision.reached_scopes == ("user",) for decision in decisions) == 1
    assert all(
        decision.exhausted_scopes == ("user",) and not decision.reached_scopes
        for decision in decisions
        if decision.exhausted_scopes
    )
    assert (await user_ref.get()).to_dict() == {"attempts": [FIXED_NOW] * limits.USER_FIT_LIMIT}
    assert (await daily_ref.get()).to_dict() == {"count": 100 + remaining_slots}


@pytest.mark.parametrize("remaining_slots", [1, 2])
async def test_concurrent_users_do_not_exceed_global_cap(quota_db, remaining_slots):
    daily_ref = _daily_quota_ref(quota_db)
    await daily_ref.set({"count": limits.GLOBAL_DAILY_FIT_LIMIT - remaining_slots})

    user_dids = USER_DIDS[: remaining_slots + 1]
    results = await asyncio.gather(
        *(limits.reserve_fit_quota(quota_db, user_did) for user_did in user_dids),
        return_exceptions=True,
    )
    decisions = _completed_decisions(results)

    assert sum(not decision.exhausted_scopes for decision in decisions) == remaining_slots
    assert sum(decision.reached_scopes == ("global",) for decision in decisions) == 1
    assert (await daily_ref.get()).to_dict() == {"count": limits.GLOBAL_DAILY_FIT_LIMIT}
    for user_did, result in zip(user_dids, results, strict=True):
        user_snapshot = await _user_quota_ref(quota_db, user_did).get()
        if isinstance(result, BaseException):
            assert not user_snapshot.exists
        elif result.exhausted_scopes:
            assert result.exhausted_scopes == ("global",)
            assert not result.reached_scopes
            assert not user_snapshot.exists
        else:
            assert user_snapshot.to_dict() == {"attempts": [FIXED_NOW]}

"""Atomically reserve per-user and global capacity for query-vector fitting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from google.cloud.firestore import AsyncClient, AsyncTransaction, async_transactional

from .firestore import USERS_COLLECTION, user_doc_id

USER_FIT_LIMIT = 12
USER_FIT_WINDOW = timedelta(hours=6)
GLOBAL_DAILY_FIT_LIMIT = 1000
RATE_LIMITS_COLLECTION = "rate_limits"
FIT_QUOTA_DOCUMENT = "llm_query_vector_fit"
GLOBAL_FIT_QUOTA_COLLECTION = "llm_query_vector_fit_daily"

QuotaScope = Literal["user", "global"]


@dataclass(frozen=True)
class FitQuotaDecision:
    exhausted_scopes: tuple[QuotaScope, ...] = ()
    reached_scopes: tuple[QuotaScope, ...] = ()
    retry_at: datetime | None = None


async def reserve_fit_quota(db: AsyncClient, user_did: str) -> FitQuotaDecision:
    """Consume one attempt in both quotas, or reject without changing either.

    A reservation is never refunded, even if fitting or storage later fails.
    Firestore errors propagate so callers can fail closed before starting work.
    """
    user_ref = (
        db.collection(USERS_COLLECTION)
        .document(user_doc_id(user_did))
        .collection(RATE_LIMITS_COLLECTION)
        .document(FIT_QUOTA_DOCUMENT)
    )

    @async_transactional
    async def _reserve(transaction: AsyncTransaction) -> FitQuotaDecision:
        # Retries may cross an expiry or UTC midnight, so calculate both the
        # rolling cutoff and the daily document inside each transaction attempt.
        now = datetime.now(UTC)
        daily_ref = db.collection(GLOBAL_FIT_QUOTA_COLLECTION).document(now.date().isoformat())
        user_snapshot = await user_ref.get(transaction=transaction)
        daily_snapshot = await daily_ref.get(transaction=transaction)
        user_data = user_snapshot.to_dict() or {}
        daily_data = daily_snapshot.to_dict() or {}

        cutoff = now - USER_FIT_WINDOW
        attempts = [attempt for attempt in user_data.get("attempts", []) if attempt > cutoff]
        daily_count = daily_data.get("count", 0)
        exhausted_scopes: list[QuotaScope] = []
        retry_times: list[datetime] = []
        if len(attempts) >= USER_FIT_LIMIT:
            exhausted_scopes.append("user")
            retry_times.append(min(attempts) + USER_FIT_WINDOW)
        if daily_count >= GLOBAL_DAILY_FIT_LIMIT:
            exhausted_scopes.append("global")
            retry_times.append(
                now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            )
        if exhausted_scopes:
            return FitQuotaDecision(
                exhausted_scopes=tuple(exhausted_scopes),
                retry_at=max(retry_times),
            )

        # Firestore requires all reads before writes. These two writes commit
        # together, so concurrent requests cannot consume only one quota.
        attempts.append(now)
        daily_count += 1
        transaction.set(user_ref, {"attempts": attempts})
        transaction.set(daily_ref, {"count": daily_count})

        reached_scopes: list[QuotaScope] = []
        if len(attempts) == USER_FIT_LIMIT:
            reached_scopes.append("user")
        if daily_count == GLOBAL_DAILY_FIT_LIMIT:
            reached_scopes.append("global")
        return FitQuotaDecision(reached_scopes=tuple(reached_scopes))

    return await _reserve(db.transaction())

"""Firestore-backed rolling user quotas and UTC calendar-day global quotas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from google.cloud.firestore import AsyncClient, AsyncTransaction, async_transactional

from .firestore import USERS_COLLECTION, user_doc_id

RATE_LIMITS_COLLECTION = "rate_limits"

QuotaScope = Literal["user", "global"]


@dataclass(frozen=True)
class QuotaDecision:
    """Exhausted scopes and their latest reset time, or newly filled scopes."""

    exhausted_scopes: tuple[QuotaScope, ...] = ()
    reached_scopes: tuple[QuotaScope, ...] = ()
    retry_at: datetime | None = None


@dataclass(frozen=True, kw_only=True)
class FirestoreRateLimiter:
    """Reserve a rolling per-user quota and a UTC calendar-day global quota.

    Use a distinct name for each feature's counters. User attempts live at
    users/{user_doc_id}/rate_limits/{name}; daily counts at {name}_daily/{date}.
    Instances hold configuration only and can be shared across requests.
    """

    name: str
    user_limit: int
    user_window: timedelta
    global_daily_limit: int

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name:
            raise ValueError("name must be a nonempty Firestore path segment")
        if self.user_limit <= 0 or self.global_daily_limit <= 0:
            raise ValueError("quota limits must be positive")
        if self.user_window <= timedelta(0):
            raise ValueError("user_window must be positive")

    async def reserve(self, db: AsyncClient, user_did: str) -> QuotaDecision:
        """Consume one attempt in both quotas, or reject without changing either.

        A committed reservation stays consumed even if the caller's work fails.
        Firestore errors propagate so callers can fail closed before starting work.
        """
        user_ref = (
            db.collection(USERS_COLLECTION)
            .document(user_doc_id(user_did))
            .collection(RATE_LIMITS_COLLECTION)
            .document(self.name)
        )

        @async_transactional
        async def _reserve(transaction: AsyncTransaction) -> QuotaDecision:
            # Retries may cross an expiry or UTC midnight, so calculate both the
            # rolling cutoff and the daily document inside each transaction attempt.
            now = datetime.now(UTC)
            daily_ref = db.collection(f"{self.name}_daily").document(now.date().isoformat())
            user_snapshot = await user_ref.get(transaction=transaction)
            daily_snapshot = await daily_ref.get(transaction=transaction)
            user_data = user_snapshot.to_dict() or {}
            daily_data = daily_snapshot.to_dict() or {}

            cutoff = now - self.user_window
            attempts = [attempt for attempt in user_data.get("attempts", []) if attempt > cutoff]
            daily_count = daily_data.get("count", 0)
            exhausted_scopes: list[QuotaScope] = []
            retry_times: list[datetime] = []
            if len(attempts) >= self.user_limit:
                exhausted_scopes.append("user")
                retry_times.append(min(attempts) + self.user_window)
            if daily_count >= self.global_daily_limit:
                exhausted_scopes.append("global")
                retry_times.append(
                    now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                )
            if exhausted_scopes:
                return QuotaDecision(
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
            if len(attempts) == self.user_limit:
                reached_scopes.append("user")
            if daily_count == self.global_daily_limit:
                reached_scopes.append("global")
            return QuotaDecision(reached_scopes=tuple(reached_scopes))

        return await _reserve(db.transaction())

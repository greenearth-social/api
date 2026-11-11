"""Hourly refresh of the followed-users cache for our real users.

Feed requests from ``precompute_artifacts`` users never refresh their cached
follow list; this job does, off the serving path (api#453).  It walks each
user's follow records on their own PDS, which carry the record keys that let
jetstream unfollows apply on the next read (see ``lib/followed_users_cache``).

Cost scales with the flagged population, not the ``users`` collection: the
equality filter is served by Firestore's automatic single-field index, so a run
reads one ``users`` document and one cache document per flagged user, and
writes only the entries that need refreshing.

Run as a Cloud Run Job built from the api image, triggered hourly by Cloud
Scheduler::

    python -m src.app.jobs.refresh_followed_users [--dry-run] [--concurrency N]

Exits non-zero only when the flagged users cannot be listed; a user whose
follows cannot be fetched is counted and left for the next run.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections import Counter
from collections.abc import AsyncIterator

from google.cloud.firestore import AsyncClient, FieldFilter  # type: ignore[import-untyped]

from ..lib.firestore import USERS_COLLECTION, init_firestore_client, user_doc_id
from ..lib.followed_users_cache import FOLLOWED_USERS_CACHE_COLLECTION, FollowedUsersCache
from ..lib.http_client import close_http_client
from ..lib.metrics import MetricCollector, get_metric_collector, set_metric_collector

logger = logging.getLogger(__name__)

# Users per batched cache read (Firestore get_all).
BATCH_SIZE = 100

DEFAULT_CONCURRENCY = 8

# Pinned so every run reports to one timeseries, as the cache's sweep does.
_METRIC_LABELS = {"endpoint": "followed_users_refresh_job", "traffic": "internal"}


async def flagged_user_dids(db: AsyncClient) -> AsyncIterator[str]:
    """DIDs of users flagged ``precompute_artifacts``, projected to ``user_did``."""
    query = (
        db.collection(USERS_COLLECTION)
        .where(filter=FieldFilter("precompute_artifacts", "==", True))
        .select(["user_did"])
    )
    async for doc in query.stream():
        did = (doc.to_dict() or {}).get("user_did")
        if isinstance(did, str) and did:
            yield did
        else:
            logger.warning("Flagged user %s has no user_did; skipping", doc.id)


async def run(
    db: AsyncClient,
    cache: FollowedUsersCache,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    dry_run: bool = False,
) -> Counter[str]:
    """Refresh every flagged user's cache entry that needs it.

    Returns counts keyed by ``flagged``, ``reason:<why>`` for each entry that
    needed a refresh, and ``outcome:<outcome>`` for each refresh attempted.
    """
    counts: Counter[str] = Counter()
    semaphore = asyncio.Semaphore(concurrency)
    tasks: list[asyncio.Task] = []

    async def refresh(key: str, did: str) -> None:
        async with semaphore:
            outcome = await cache.refresh(key, did, records=True)
        counts[f"outcome:{outcome}"] += 1

    async def check(batch: list[str]) -> None:
        keys = {user_doc_id(did): did for did in batch}
        collection = db.collection(FOLLOWED_USERS_CACHE_COLLECTION)
        seen: set[str] = set()
        async for snapshot in db.get_all([collection.document(key) for key in keys]):
            key = snapshot.id
            seen.add(key)
            reason = cache.refresh_reason(cache.entry_from(snapshot, key))
            if reason is not None:
                counts[f"reason:{reason}"] += 1
                if not dry_run:
                    tasks.append(asyncio.create_task(refresh(key, keys[key])))
        for key in keys.keys() - seen:
            # get_all yields a snapshot for missing documents too; guard anyway.
            counts["reason:missing"] += 1
            if not dry_run:
                tasks.append(asyncio.create_task(refresh(key, keys[key])))

    batch: list[str] = []
    async for did in flagged_user_dids(db):
        counts["flagged"] += 1
        batch.append(did)
        if len(batch) >= BATCH_SIZE:
            await check(batch)
            batch = []
    if batch:
        await check(batch)

    await asyncio.gather(*tasks)
    _report(counts)
    return counts


def _report(counts: Counter[str]) -> None:
    needed = sum(v for k, v in counts.items() if k.startswith("reason:"))
    failed = sum(
        v
        for k, v in counts.items()
        if k.startswith("outcome:") and k not in ("outcome:success", "outcome:skipped")
    )
    if mc := get_metric_collector():
        mc.record("follows_refresh_job.users_rate", counts["flagged"], state="flagged",
                  **_METRIC_LABELS)
        mc.record("follows_refresh_job.users_rate", needed, state="needed_refresh",
                  **_METRIC_LABELS)
        mc.record("follows_refresh_job.users_rate", counts["outcome:success"],
                  state="refreshed", **_METRIC_LABELS)
        mc.record("follows_refresh_job.users_rate", failed, state="failed", **_METRIC_LABELS)
    logger.info(
        "Followed-users refresh job: flagged=%d needed_refresh=%d %s",
        counts["flagged"],
        needed,
        " ".join(f"{k}={v}" for k, v in sorted(counts.items()) if k != "flagged"),
    )


def _require_config() -> None:
    """Fail fast: a job pointed at the wrong (default) database would 'succeed'."""
    if os.environ.get("GE_FIRESTORE_EMULATOR_HOST"):
        return
    missing = [
        name for name in ("GE_FIRESTORE_PROJECT", "GE_FIRESTORE_DATABASE")
        if not os.environ.get(name)
    ]
    if missing:
        raise SystemExit(f"Missing required environment: {', '.join(missing)}")


async def _main(args: argparse.Namespace) -> int:
    _require_config()
    metrics = MetricCollector(
        service_name=os.environ.get("GE_SERVICE_NAME", "greenearth-follows-refresh"),
        env=os.environ.get("ENVIRONMENT", "local"),
        export_interval_sec=int(os.environ.get("GE_METRICS_EXPORT_INTERVAL_SEC", "60")),
    )
    set_metric_collector(metrics)
    db = init_firestore_client()
    try:
        await run(
            db,
            FollowedUsersCache(db),
            concurrency=args.concurrency,
            dry_run=args.dry_run,
        )
    except Exception:
        logger.exception("Followed-users refresh job failed")
        return 1
    finally:
        await close_http_client()
        db.close()
        await metrics.shutdown()
        set_metric_collector(None)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh the followed-users cache for precompute_artifacts users."
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("GE_FOLLOWS_REFRESH_JOB_CONCURRENCY", DEFAULT_CONCURRENCY)),
        help="Follow walks in flight at once",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count the entries that need refreshing without walking or writing",
    )
    args = parser.parse_args()
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")
    logging.basicConfig(
        level=os.environ.get("GE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()

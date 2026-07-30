"""Shared, dependency-light helpers for the load-testing scripts.

Kept separate from the scripts themselves so the pure logic — cohort sizing,
session scheduling, page-depth sampling, interaction-payload assembly — can be
unit-tested without touching Elasticsearch, Firestore, Cloud Monitoring, or the
network. See select_users.py, run.py, analyze.py and cleanup.py in this package
for the runnable entry points.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

# GCP project + Firestore database per environment. Both environments live in
# the same project and are separated by database (mirrors feed_debug.py and
# scripts/gcp_setup.sh).
GCP_PROJECT = "greenearth-471522"
FIRESTORE_DATABASES = {
    "dev": None,  # local emulator, leave env untouched
    "stage": "greenearth-stage",
    "prod": "greenearth-prod",
}
CLOUD_RUN_SERVICES = {
    "stage": "greenearth-api-stage",
    "prod": "greenearth-api-prod",
}
CLOUD_RUN_REGION = "us-east1"

# The three cohorts a run is drawn from (see the api#189 plan). Order is the
# fill order when rounding leaves a remainder.
COHORTS = ("existing", "active", "low")


def split_counts(total: int, pct_existing: int, pct_active: int, pct_low: int) -> dict[str, int]:
    """Split ``total`` users across the three cohorts by percentage.

    Percentages must sum to 100. Rounding remainder is handed to cohorts in
    ``COHORTS`` order so the returned counts always sum exactly to ``total``.
    """
    if pct_existing + pct_active + pct_low != 100:
        raise ValueError("cohort percentages must sum to 100")
    if total < 0:
        raise ValueError("total must be non-negative")

    pcts = {"existing": pct_existing, "active": pct_active, "low": pct_low}
    counts = {c: total * pcts[c] // 100 for c in COHORTS}
    remainder = total - sum(counts.values())
    for c in COHORTS:
        if remainder <= 0:
            break
        counts[c] += 1
        remainder -= 1
    return counts


def sample_page_depth(rng: random.Random, mean_pages: float) -> int:
    """Number of pages a session fetches, including the initial page (>= 1).

    Geometric distribution with the given mean, so most sessions are short and a
    few page deep — the shape real feed sessions tend to have.
    """
    if mean_pages < 1:
        raise ValueError("mean_pages must be >= 1")
    # Geometric with support {1, 2, ...} has mean 1/p, so p = 1/mean_pages.
    p = 1.0 / mean_pages
    # mean_pages == 1 → p == 1 → every session is exactly one page. Short-circuit
    # before the log, which would otherwise be log(1 - p) = log(0).
    if p >= 1.0:
        return 1
    # inverse-CDF sample; clamp the random draw away from 0 and 1.
    u = min(max(rng.random(), 1e-9), 1 - 1e-9)
    depth = 1 + int(math.log(1 - u) / math.log(1 - p))
    return max(1, depth)


def session_start_offsets(
    rate_per_min: float, duration_min: float, rng: random.Random
) -> list[float]:
    """Arrival times (seconds from start) of sessions over the run window.

    Models a Poisson process at ``rate_per_min`` sessions/minute: inter-arrival
    gaps are exponential, so sessions cluster and spread the way organic traffic
    does rather than firing on a fixed metronome. Deterministic given ``rng``.
    """
    if rate_per_min <= 0 or duration_min <= 0:
        return []
    rate_per_sec = rate_per_min / 60.0
    horizon = duration_min * 60.0
    offsets: list[float] = []
    t = 0.0
    while True:
        gap = rng.expovariate(rate_per_sec)
        t += gap
        if t >= horizon:
            break
        offsets.append(t)
    return offsets


def weighted_cohort_choice(rng: random.Random, users: list[dict]) -> dict:
    """Pick a user uniformly at random from a selection list."""
    return users[rng.randrange(len(users))]


@dataclass(frozen=True)
class InteractionSpec:
    """One interaction to send, echoing the exact feedContext we were served."""

    item: str
    event: str
    feed_context: str


# Lexicon-prefixed event names, as the AppView would forward them.
_DEFS = "app.bsky.feed.defs#"


def build_interactions(
    feed_items: list[dict],
    rng: random.Random,
    *,
    seen_share: float = 1.0,
    like_share: float = 0.1,
    click_share: float = 0.1,
) -> list[InteractionSpec]:
    """Build a plausible interaction set from a served feed page.

    ``feed_items`` are the raw skeleton items, each ``{"post": uri,
    "feedContext": token}``. Every chosen item is marked seen; a fraction also
    gets a like or a clickthrough. The feedContext is echoed **verbatim** — it
    is the signed token that carries the user's identity and the load-test
    ``lt`` flag, so the server attributes and tags the interaction correctly
    without the client asserting anything.
    """
    specs: list[InteractionSpec] = []
    for item in feed_items:
        uri = item.get("post")
        ctx = item.get("feedContext") or item.get("feed_context")
        if not uri or not ctx:
            continue
        if rng.random() > seen_share:
            continue
        specs.append(InteractionSpec(uri, f"{_DEFS}interactionSeen", ctx))
        if rng.random() < like_share:
            specs.append(InteractionSpec(uri, f"{_DEFS}interactionLike", ctx))
        if rng.random() < click_share:
            specs.append(InteractionSpec(uri, f"{_DEFS}clickthroughItem", ctx))
    return specs


def interactions_request_body(specs: list[InteractionSpec]) -> dict:
    """Assemble the sendInteractions request body from interaction specs."""
    return {
        "interactions": [
            {"item": s.item, "event": s.event, "feedContext": s.feed_context} for s in specs
        ]
    }


def feed_uri_from_describe(describe_response: dict, feed_rkey: str) -> str | None:
    """Find the AT URI for ``feed_rkey`` in a describeFeedGenerator response.

    Matches on the rkey (last path segment) alone — the server resolves feeds by
    rkey regardless of the publisher DID in the URI's authority.
    """
    for feed in describe_response.get("feeds", []):
        uri = feed.get("uri", "")
        if uri.rsplit("/", 1)[-1] == feed_rkey:
            return uri
    return None


def percentiles(values: list[float], ps: tuple[float, ...] = (50, 95, 99)) -> dict[float, float]:
    """Nearest-rank percentiles of ``values`` (empty input → zeros)."""
    if not values:
        return {p: 0.0 for p in ps}
    ordered = sorted(values)
    out: dict[float, float] = {}
    for p in ps:
        # nearest-rank: rank = ceil(p/100 * n), 1-indexed.
        rank = max(1, math.ceil(p / 100.0 * len(ordered)))
        out[p] = ordered[min(rank, len(ordered)) - 1]
    return out

#!/usr/bin/env python3
"""Simulate concurrent Perspective API scoring load for before/after profiling.

Issue #250 investigation: the ranking pipeline scores every candidate
concurrently via `asyncio.gather`, and PerspectiveClient historically shared
the process-wide `httpx.AsyncClient` (default pool: 100 connections, 20
keepalive, 30s timeout) with every other outbound API caller. Under a burst
of concurrent scoring calls this queues requests behind the pool limit --
indistinguishable, from a flame chart, from the Perspective API itself being
slow.

This script isolates that effect without needing a live GE_PERSPECTIVE_API_KEY
or burning real API quota: it starts a local aiohttp mock server that mimics
Perspective's response shape with a fixed artificial per-request latency,
points PerspectiveClient at it via GE_PERSPECTIVE_HOST, and scores a large
batch of synthetic candidates through the real score_candidates() path under
a pyinstrument profiler -- the same Profiler(interval=0.001,
async_mode="enabled") configuration used by app.lib.profiling's per-request
middleware. Run it once on each side of the change to get comparable
before/after flame charts and wall-clock numbers:

    git stash                          # or: git checkout main -- src/app/lib/perspective.py
    pipenv run python scripts/profile_perspective.py --candidates 500

    git stash pop                      # or: git checkout <branch> -- src/app/lib/perspective.py
    pipenv run python scripts/profile_perspective.py --candidates 500

Output: an HTML flame chart under ./profiles/ and a printed wall-clock
summary (total elapsed time and mean time per candidate). If the pool is
the bottleneck, elapsed time will scale with (candidates / pool_size) *
mock_latency before the fix, and stay ~flat at ~mock_latency after it.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from aiohttp import web

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

_MOCK_PORT = 8099
_MOCK_LATENCY_SECONDS = 0.2

_ATTRIBUTES = [
    "REASONING_EXPERIMENTAL",
    "PERSONAL_STORY_EXPERIMENTAL",
    "AFFINITY_EXPERIMENTAL",
    "COMPASSION_EXPERIMENTAL",
    "RESPECT_EXPERIMENTAL",
    "CURIOSITY_EXPERIMENTAL",
    "FEARMONGERING_EXPERIMENTAL",
    "GENERALIZATION_EXPERIMENTAL",
    "SCAPEGOATING_EXPERIMENTAL",
    "MORAL_OUTRAGE_EXPERIMENTAL",
    "ALIENATION_EXPERIMENTAL",
    "TOXICITY",
    "IDENTITY_ATTACK",
    "INSULT",
    "THREAT",
]


async def _mock_analyze(request: web.Request) -> web.Response:
    await asyncio.sleep(_MOCK_LATENCY_SECONDS)
    return web.json_response(
        {"attributeScores": {name: {"summaryScore": {"value": 0.5}} for name in _ATTRIBUTES}}
    )


async def _start_mock_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_post("/v1alpha1/comments:analyze", _mock_analyze)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", _MOCK_PORT)
    await site.start()
    return runner


async def _main_async(n_candidates: int, profile_dir: Path) -> None:
    os.environ.setdefault("GE_PERSPECTIVE_API_KEY", "local-mock-key")
    os.environ["GE_PERSPECTIVE_HOST"] = f"http://127.0.0.1:{_MOCK_PORT}"

    runner = await _start_mock_server()

    from pyinstrument import Profiler

    from app.lib.perspective import score_candidates
    from app.models import CandidatePost

    candidates = [
        CandidatePost(
            at_uri=f"at://profile/synthetic/{i}",
            content=f"Synthetic profiling candidate #{i} with enough text to score.",
            score=0.0,
            minilm_l12_embedding=None,
            generator_name="profile_perspective",
        )
        for i in range(n_candidates)
    ]

    profiler = Profiler(interval=0.001, async_mode="enabled")
    profiler.start()
    start = time.monotonic()
    scores = await score_candidates(candidates)
    elapsed = time.monotonic() - start
    profiler.stop()

    await runner.cleanup()

    profile_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    output_path = profile_dir / f"{ts}-perspective-{n_candidates}candidates.html"
    output_path.write_text(profiler.output_html())

    scored = sum(1 for s in scores.values() if s is not None)
    print(f"candidates={n_candidates} scored={scored} elapsed_s={elapsed:.2f}")
    print(f"mean_s_per_candidate={elapsed / n_candidates:.4f}")
    print(
        f"mock_latency_s={_MOCK_LATENCY_SECONDS} "
        f"(fully-parallel floor: ~{_MOCK_LATENCY_SECONDS:.2f}s)"
    )
    print(f"profile written to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidates", type=int, default=500,
        help="Number of synthetic candidates to score concurrently (default: 500)",
    )
    parser.add_argument(
        "--profile-dir", type=Path, default=Path("profiles"),
        help="Directory to write the pyinstrument HTML report to (default: ./profiles)",
    )
    args = parser.parse_args()
    asyncio.run(_main_async(args.candidates, args.profile_dir))


if __name__ == "__main__":
    main()

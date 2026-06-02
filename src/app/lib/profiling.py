"""Env-gated pyinstrument middleware.

When ``GE_PROFILE=1`` is set, every HTTP request is sampled by
pyinstrument at 1 ms intervals (``async_mode="enabled"`` so it
follows ``await`` boundaries across ``asyncio.gather``). The flame
chart is written to ``./profiles/`` as HTML, named with the current
request ID so it can be cross-referenced with the corresponding
``timed()`` log lines via ``grep``.

The pyinstrument import is lazy so production deployments without
the dev extras don't fail at startup.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request

from .request_context import get_request_id

logger = logging.getLogger(__name__)

DEFAULT_PROFILE_DIR = "profiles"


def _path_slug(path: str) -> str:
    """Filesystem-safe slug for the request path component of the filename."""
    return re.sub(r"[^a-zA-Z0-9]+", "_", path).strip("_") or "root"


def install_profiling(app: FastAPI) -> None:
    """Install the per-request pyinstrument middleware if ``GE_PROFILE=1``.

    No-op otherwise. Must be called after the request-ID middleware is
    registered so the ID is set by the time we build the output filename.
    """
    if os.environ.get("GE_PROFILE") != "1":
        return

    # Lazy import so this is not a hard runtime dep when GE_PROFILE is unset.
    from pyinstrument import Profiler

    profile_dir = Path(os.environ.get("GE_PROFILE_DIR", DEFAULT_PROFILE_DIR))
    profile_dir.mkdir(parents=True, exist_ok=True)
    logger.info("pyinstrument profiling enabled; writing to %s", profile_dir)

    @app.middleware("http")
    async def profile_mw(request: Request, call_next):
        profiler = Profiler(interval=0.001, async_mode="enabled")
        profiler.start()
        try:
            response = await call_next(request)
        finally:
            profiler.stop()

        rid = get_request_id() or "no-rid"
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        filename = f"{ts}-{rid}-{_path_slug(request.url.path)}.html"
        output_path = profile_dir / filename
        try:
            output_path.write_text(profiler.output_html())
            logger.info("profile_written rid=%s path=%s", rid, output_path)
        except Exception:
            logger.exception("Failed to write pyinstrument profile to %s", output_path)

        return response

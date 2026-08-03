"""Release metadata exposed with feed-transparency snapshots."""

from __future__ import annotations

import os


def api_release_sha() -> str | None:
    """Return the deployed API Git SHA, or ``None`` outside a versioned deploy."""
    value = os.environ.get("GE_GIT_SHA", "").strip()
    return value or None

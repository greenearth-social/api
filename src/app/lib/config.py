"""Runtime configuration flags derived from environment variables."""

import os
from contextvars import ContextVar

_fail_fast_override: ContextVar[bool | None] = ContextVar("fail_fast_override", default=None)


def set_fail_fast_for_request(value: bool) -> None:
    """Set the fail-fast flag for the current request context.

    Called once per request from get_feed_skeleton() after evaluating the
    PostHog feature flag. Overrides GE_FAIL_FAST for the duration of the
    async context (i.e. this request and its child coroutines).
    """
    _fail_fast_override.set(value)


def fail_fast() -> bool:
    """When True, pipeline component failures raise instead of being swallowed.

    Per-request value (set via set_fail_fast_for_request) takes precedence.
    Falls back to GE_FAIL_FAST env var (default: false) when no per-request
    value has been set — preserves existing behaviour for local dev and any
    environment that still sets the env var directly.
    """
    override = _fail_fast_override.get()
    if override is not None:
        return override
    return os.environ.get("GE_FAIL_FAST", "false").lower() in ("true", "1", "yes")

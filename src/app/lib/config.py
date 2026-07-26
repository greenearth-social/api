"""Runtime configuration flags derived from environment variables."""

from contextvars import ContextVar

_fail_fast_override: ContextVar[bool] = ContextVar("fail_fast_override", default=False)


def set_fail_fast_for_request(value: bool) -> None:
    """Set the fail-fast flag for the current request context.

    Called once per request from get_feed_skeleton() after evaluating the
    PostHog feature flag. Controls whether pipeline failures raise or are
    swallowed for the duration of this async context.
    """
    _fail_fast_override.set(value)


def fail_fast() -> bool:
    """When True, pipeline component failures raise instead of being swallowed."""
    return _fail_fast_override.get()

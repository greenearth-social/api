"""Runtime configuration flags derived from environment variables."""

import os


def fail_fast() -> bool:
    """When True, pipeline component failures raise instead of being swallowed.

    Controlled by the GE_FAIL_FAST env var (default: false).
    Set to "true" in Cloud Run deployments to disable degraded-feed serving.
    """
    return os.environ.get("GE_FAIL_FAST", "false").lower() in ("true", "1", "yes")

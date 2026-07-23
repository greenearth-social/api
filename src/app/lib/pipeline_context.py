"""Per-request pipeline degradation tracking.

A ContextVar holds the current PipelineContext so every pipeline stage can
record degradation events without threading a parameter through every layer.
Mirrors the FeedDebugRecorder / feed_debug_scope pattern in feed_debug.py.

When no context is installed (the default), current_pipeline_context() returns
None and all degradation hooks are no-ops.
"""

from __future__ import annotations

import contextlib
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum


class DegradationStage(str, Enum):
    CANDIDATE_GEN = "candidate_gen"
    RANK = "rank"
    EMBED_HYDRATION = "embed_hydration"


@dataclass
class DegradationEvent:
    stage: DegradationStage
    component: str    # e.g. "two_tower", "perspective", "fetch_post_embeddings"
    cause: BaseException


@dataclass
class PipelineContext:
    """Mutable context for one feed-render pipeline execution."""

    feed_name: str
    fail_fast: bool = False
    degradations: list[DegradationEvent] = field(default_factory=list)

    def record(self, event: DegradationEvent) -> None:
        """Append *event* to degradations. Re-raises event.cause when fail_fast is set."""
        self.degradations.append(event)
        if self.fail_fast:
            raise event.cause


_context: ContextVar[PipelineContext | None] = ContextVar(
    "ge_pipeline_context", default=None
)


def current_pipeline_context() -> PipelineContext | None:
    """Return the active PipelineContext, or None if outside a pipeline scope."""
    return _context.get()


@contextlib.contextmanager
def pipeline_context_scope(ctx: PipelineContext):
    """Install *ctx* as the active pipeline context for the duration of the block."""
    token = _context.set(ctx)
    try:
        yield ctx
    finally:
        _context.reset(token)

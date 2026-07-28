"""Tests for the pipeline degradation context."""

from __future__ import annotations

import pytest

from .pipeline_context import (
    DegradationEvent,
    DegradationStage,
    PipelineContext,
    current_pipeline_context,
    pipeline_context_scope,
)


class TestScopeInstallation:
    def test_returns_none_outside_scope(self):
        assert current_pipeline_context() is None

    def test_returns_context_inside_scope(self):
        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            assert current_pipeline_context() is ctx

    def test_restores_none_after_scope_exits(self):
        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            pass
        assert current_pipeline_context() is None

    def test_nested_scopes_restore_outer_on_exit(self):
        outer = PipelineContext(feed_name="outer")
        inner = PipelineContext(feed_name="inner")
        with pipeline_context_scope(outer):
            with pipeline_context_scope(inner):
                assert current_pipeline_context() is inner
            assert current_pipeline_context() is outer


class TestRecord:
    def test_record_appends_event(self):
        ctx = PipelineContext(feed_name="your-feed")
        exc = ValueError("gen failed")
        event = DegradationEvent(
            stage=DegradationStage.CANDIDATE_GEN, component="two_tower", cause=exc
        )
        ctx.record(event)
        assert len(ctx.degradations) == 1
        assert ctx.degradations[0] is event

    def test_record_multiple_events(self):
        ctx = PipelineContext(feed_name="your-feed")
        ctx.record(DegradationEvent(DegradationStage.CANDIDATE_GEN, "gen_a", ValueError()))
        ctx.record(DegradationEvent(DegradationStage.RANK, "perspective", RuntimeError()))
        assert len(ctx.degradations) == 2

    def test_record_does_not_raise_when_fail_fast_false(self):
        ctx = PipelineContext(feed_name="your-feed", fail_fast=False)
        ctx.record(
            DegradationEvent(DegradationStage.EMBED_HYDRATION, "fetch_post_embeddings", OSError())
        )
        assert len(ctx.degradations) == 1  # recorded, not raised

    def test_record_raises_cause_when_fail_fast_true(self):
        ctx = PipelineContext(feed_name="your-feed", fail_fast=True)
        cause = RuntimeError("hard failure")
        with pytest.raises(RuntimeError, match="hard failure"):
            ctx.record(DegradationEvent(DegradationStage.RANK, "two_tower", cause))

    def test_degradations_empty_on_init(self):
        ctx = PipelineContext(feed_name="your-feed")
        assert ctx.degradations == []

    def test_fail_fast_defaults_to_false(self):
        ctx = PipelineContext(feed_name="your-feed")
        assert ctx.fail_fast is False

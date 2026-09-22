"""Exercise real two-tower fallback through the shared candidate pipeline."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ...models import CandidateGenerateRequest, CandidatePost, GeneratorSpec
from ..average_user_embedding import AverageUserEmbedding
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from ..inference import UserEmbeddingResult
from ..pipeline_context import PipelineContext, pipeline_context_scope
from ..user_history_cache import UserHistory, UserHistoryItem
from . import generate, two_tower
from .base import CandidateResult


@pytest.fixture
def dependencies(monkeypatch):
    prior = AverageUserEmbedding((1.0, 0.0), 2, "1" * 32, "2" * 32, "test-run", 2)
    history = UserHistory([UserHistoryItem("at://liked/1", "2026-09-21T00:00:00Z", [1.0])])
    history_mock = AsyncMock(return_value=history)
    prediction = AsyncMock(return_value=UserEmbeddingResult(1, 1, [0.0, 1.0], "1" * 32, "2" * 32))
    search = AsyncMock(
        return_value=[CandidatePost(at_uri="at://two_tower/1", generator_name="two_tower")]
    )
    other = SimpleNamespace(
        generate=AsyncMock(
            return_value=CandidateResult(
                generator_name="popularity",
                candidates=[CandidatePost(at_uri="at://popularity/1", generator_name="popularity")],
            )
        )
    )
    tower = two_tower.TwoTowerCandidateGenerator("two_tower", "actual")
    monkeypatch.setattr(two_tower, "get_average_user_embedding", lambda: prior)
    monkeypatch.setattr(two_tower, "get_average_user_embedding_error", lambda: "not_configured")
    monkeypatch.setattr(two_tower, "fetch_user_history_features", history_mock)
    monkeypatch.setattr(two_tower, "predict_user_embedding", prediction)
    monkeypatch.setattr(two_tower, "get_inference_settings", lambda: ("http://inference", "key"))
    monkeypatch.setattr(two_tower, "knn_search_posts", search)
    monkeypatch.setattr(
        generate, "get_generator", lambda name: tower if name == "two_tower" else other
    )
    monkeypatch.setattr(generate, "hydrate_posts", AsyncMock(side_effect=lambda es, posts: posts))
    return SimpleNamespace(history=history_mock, prediction=prediction, search=search, other=other)


def request(*, tower_weight: float = 1.0, count=2):
    return CandidateGenerateRequest(
        user_did="did:plc:test",
        generators=[
            GeneratorSpec(name="two_tower", weight=tower_weight),
            GeneratorSpec(name="popularity", weight=1.0),
        ],
        num_candidates=count,
        video_only=False,
        max_age_hours=168,
        infill=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "has_prior,has_history", [(True, True), (True, False), (False, True), (False, False)]
)
async def test_prior_fallback_preserves_other_source_and_diagnostics(
    monkeypatch, dependencies, has_prior, has_history
):
    if not has_prior:
        monkeypatch.setattr(two_tower, "get_average_user_embedding", lambda: None)
    if not has_history:
        dependencies.history.return_value = UserHistory([])
    context = PipelineContext(feed_name="your-feed", fail_fast=True)
    recorder = FeedDebugRecorder(feed_name="your-feed", regenerated=True)
    with pipeline_context_scope(context), feed_debug_scope(recorder):
        result = await generate.run_generate(request(), object())
    sources = {post.generator_name for post in result.candidates}
    assert sources == ({"two_tower", "popularity"} if has_prior or has_history else {"popularity"})
    assert context.degradations == []
    assert dependencies.prediction.await_count == int(has_history)
    assert dependencies.search.await_count == int(has_prior or has_history)
    if not has_prior and has_history:
        assert recorder.generator_outputs[0].reason == "average_user_embedding_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["prediction", "search"])
async def test_actual_failures_keep_other_sources_and_record_degradation(
    dependencies, failure_stage
):
    error = RuntimeError(f"{failure_stage} unavailable")
    getattr(dependencies, failure_stage).side_effect = error
    context = PipelineContext(feed_name="your-feed")
    with pipeline_context_scope(context):
        result = await generate.run_generate(request(), object())
    assert [post.generator_name for post in result.candidates] == ["popularity"]
    assert len(context.degradations) == 1
    assert context.degradations[0].component == "two_tower"
    assert context.degradations[0].cause is error


@pytest.mark.asyncio
async def test_zero_allocation_does_not_force_two_tower_with_a_prior(dependencies):
    await generate.run_generate(request(tower_weight=0.01, count=1), object())
    dependencies.history.assert_not_awaited()
    dependencies.prediction.assert_not_awaited()
    dependencies.search.assert_not_awaited()
    dependencies.other.generate.assert_awaited_once()

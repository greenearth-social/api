"""Tests for average-prior and actual-user two-tower retrieval."""

import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ...models import CandidatePost
from ..average_user_embedding import AverageUserEmbedding
from ..candidates import get_generator, list_generators
from ..elasticsearch import POSTS_QUALITY_KNN_INDEX
from ..embeddings import GE_POST_EMBEDDING_FIELD
from ..inference import InferenceResponseFormatError, UserEmbeddingResult
from ..user_history_cache import UserHistory, UserHistoryItem
from . import two_tower
from .two_tower import AVG_USER_EMBEDDING_WEIGHT, TwoTowerCandidateGenerator

USER_MODEL = "1" * 32
POST_MODEL = "2" * 32
INFERENCE_SETTINGS = ("https://inference", "api-key")


def make_history(count: int, missing: int = 0) -> UserHistory:
    # Separate retained likes from usable embeddings so tests can prove that
    # missing post vectors do not increase the actual user's blend weight.
    return UserHistory(
        [
            UserHistoryItem(
                at_uri=f"at://post/{i}",
                liked_at="2026-09-01T12:00:00Z",
                embedding=[0.3, 0.4] if i < count else None,
                author_did=f"did:plc:author{i}",
            )
            for i in range(count + missing)
        ]
    )


@pytest.fixture
def prior():
    return AverageUserEmbedding(
        embedding=(1.0, 0.0),
        dimension=2,
        user_model_uuid=USER_MODEL,
        post_model_uuid=POST_MODEL,
        run_id="20260921T202345.400236Z_a1796088",
        contributing_users=406,
    )


@pytest.fixture
def generator():
    return TwoTowerCandidateGenerator(name="two_tower", history_mode="actual")


@pytest.fixture
def dependencies(monkeypatch, prior):
    # Orthogonal unit vectors make the blend coordinates equal its two weights.
    # Mock I/O boundaries, leaving vector selection and fallback logic real.
    history = make_history(2)
    mocks = SimpleNamespace(
        history=AsyncMock(return_value=history),
        prediction=AsyncMock(
            return_value=UserEmbeddingResult(
                history_like_count=2,
                history_embedding_count=2,
                embedding=[0.0, 1.0],
                user_model_uuid=USER_MODEL,
                post_model_uuid=POST_MODEL,
            )
        ),
        settings=Mock(return_value=INFERENCE_SETTINGS),
        prior=Mock(return_value=prior),
        prior_error=Mock(return_value=None),
        recorder=Mock(),
        knn=AsyncMock(
            return_value=[
                CandidatePost(at_uri="at://post/result", score=0.9, generator_name="two_tower")
            ]
        ),
    )
    monkeypatch.setattr(two_tower, "fetch_user_history_features", mocks.history)
    monkeypatch.setattr(two_tower, "predict_user_embedding", mocks.prediction)
    monkeypatch.setattr(two_tower, "get_inference_settings", mocks.settings)
    monkeypatch.setattr(two_tower, "get_average_user_embedding", mocks.prior)
    monkeypatch.setattr(two_tower, "get_average_user_embedding_error", mocks.prior_error)
    monkeypatch.setattr(two_tower, "current_recorder", lambda: mocks.recorder)
    monkeypatch.setattr(two_tower, "knn_search_posts", mocks.knn)
    monkeypatch.delenv("GE_TWO_TOWER_KNN_INDEX", raising=False)
    return mocks


def test_registered_generators():
    for name, history_mode in (("two_tower", "actual"), ("two_tower_empty_history", "empty")):
        generator = get_generator(name)
        assert isinstance(generator, TwoTowerCandidateGenerator)
        assert generator.name == name
        assert generator.history_mode == history_mode
        assert name in list_generators()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 2, 64])
async def test_exact_blend_uses_usable_history_count(generator, dependencies, count):
    dependencies.history.return_value = make_history(count)
    dependencies.prediction.return_value = replace(
        dependencies.prediction.return_value,
        history_like_count=count,
        history_embedding_count=count,
    )
    es = object()
    result = await generator.generate(es, "did:plc:user1")

    expected = [
        AVG_USER_EMBEDDING_WEIGHT / (AVG_USER_EMBEDDING_WEIGHT + count),
        count / (AVG_USER_EMBEDDING_WEIGHT + count),
    ]
    assert dependencies.knn.await_args.args[1] == pytest.approx(expected)
    dependencies.history.assert_awaited_once_with(es, "did:plc:user1")
    dependencies.recorder.record_user_features.assert_called_once_with(
        "two_tower", dependencies.history.return_value.liked_uris, count
    )
    if count:
        dependencies.prediction.assert_awaited_once_with(
            dependencies.history.return_value, base_url="https://inference", api_key="api-key"
        )
    else:
        dependencies.prediction.assert_not_awaited()
        dependencies.settings.assert_not_called()
    assert result.reason is None
    assert result.mode == "primary"


@pytest.mark.asyncio
async def test_blend_is_not_normalized(generator, dependencies):
    await generator.generate(object(), "did:plc:user1")
    # Two orthogonal unit inputs with equal weights retain magnitude sqrt(0.5).
    assert dependencies.knn.await_args.args[1] == [0.5, 0.5]


@pytest.mark.asyncio
async def test_missing_history_embeddings_do_not_increase_weight(generator, dependencies):
    dependencies.history.return_value = make_history(1, missing=4)
    dependencies.prediction.return_value = replace(
        dependencies.prediction.return_value, history_like_count=5, history_embedding_count=1
    )
    await generator.generate(object(), "did:plc:user1")
    assert dependencies.knn.await_args.args[1] == pytest.approx([2 / 3, 1 / 3])
    dependencies.recorder.record_user_features.assert_called_once_with(
        "two_tower", dependencies.history.return_value.liked_uris, 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [0, 3])
async def test_prior_only_for_no_usable_history(generator, dependencies, prior, missing):
    dependencies.history.return_value = make_history(0, missing=missing)
    # No inference configuration is needed to serve an artifact alone.
    dependencies.settings.side_effect = RuntimeError("inference not configured")
    await generator.generate(object(), "did:plc:new-user")
    assert dependencies.knn.await_args.args[1] == list(prior.embedding)
    assert dependencies.knn.await_args.kwargs["ge_post_embedding_model_uuid"] == POST_MODEL
    dependencies.prediction.assert_not_awaited()
    dependencies.settings.assert_not_called()


@pytest.mark.asyncio
async def test_empty_variant_does_not_fetch_history_or_infer(dependencies, prior):
    generator = TwoTowerCandidateGenerator("two_tower_empty_history", "empty")
    await generator.generate(object(), "did:plc:user1")
    assert dependencies.knn.await_args.args[1] == list(prior.embedding)
    assert dependencies.knn.await_args.kwargs["generator_name"] == "two_tower_empty_history"
    dependencies.history.assert_not_awaited()
    dependencies.prediction.assert_not_awaited()
    dependencies.settings.assert_not_called()
    dependencies.recorder.record_user_features.assert_called_once_with(
        "two_tower_empty_history", [], 0
    )


@pytest.mark.asyncio
async def test_different_users_with_no_history_use_the_same_prior(generator, dependencies):
    dependencies.history.return_value = make_history(0)
    await generator.generate(object(), "did:plc:first")
    await generator.generate(object(), "did:plc:second")
    assert (
        dependencies.knn.await_args_list[0].args[1] == (dependencies.knn.await_args_list[1].args[1])
    )
    dependencies.prediction.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "history_mode,reason",
    [
        ("actual", "no_user_like_history"),
        ("empty", "average_user_embedding_unavailable"),
    ],
)
async def test_no_history_and_no_prior_returns_no_candidates(dependencies, history_mode, reason):
    dependencies.history.return_value = make_history(0)
    dependencies.prior.return_value = None
    dependencies.prior_error.return_value = "Artifact not configured"
    generator = TwoTowerCandidateGenerator("two_tower", history_mode)
    result = await generator.generate(object(), "did:plc:user1")
    assert result.candidates == []
    assert result.status == "not_run"
    assert result.reason == reason
    dependencies.knn.assert_not_awaited()
    dependencies.prediction.assert_not_awaited()
    dependencies.settings.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_error", ["Not configured", "Invalid artifact", "GCS unavailable"])
async def test_actual_only_when_prior_unavailable(generator, dependencies, caplog, prior_error):
    dependencies.prior.return_value = None
    dependencies.prior_error.return_value = prior_error
    with caplog.at_level(logging.WARNING):
        result = await generator.generate(object(), "did:plc:user1")
    assert dependencies.knn.await_args.args[1] == [0.0, 1.0]
    assert dependencies.knn.await_args.kwargs["ge_post_embedding_model_uuid"] == POST_MODEL
    assert result.status == "success"
    assert result.reason == "average_user_embedding_unavailable"
    assert prior_error in caplog.text


@pytest.mark.asyncio
async def test_unconfigured_prior_is_not_a_warning(generator, dependencies, caplog):
    dependencies.prior.return_value = None
    dependencies.prior_error.return_value = "not_configured"
    with caplog.at_level(logging.INFO):
        result = await generator.generate(object(), "did:plc:user1")
    assert result.reason == "average_user_embedding_unavailable"
    assert "using actual embedding only" in caplog.text
    assert all(record.levelno < logging.WARNING for record in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("user_model_uuid", "3" * 32, "average_user_embedding_model_pair_mismatch"),
        ("post_model_uuid", "3" * 32, "average_user_embedding_model_pair_mismatch"),
        ("dimension", 3, "average_user_embedding_dimension_mismatch"),
    ],
)
async def test_incompatible_prior_falls_back_to_actual(
    generator, dependencies, prior, field, value, reason
):
    updates = {field: value}
    if field == "dimension":
        updates["embedding"] = (1.0, 0.0, 0.0)
    dependencies.prior.return_value = replace(prior, **updates)
    result = await generator.generate(object(), "did:plc:user1")
    assert dependencies.knn.await_args.args[1] == [0.0, 1.0]
    assert dependencies.knn.await_args.kwargs["ge_post_embedding_model_uuid"] == POST_MODEL
    assert result.reason == reason


@pytest.mark.asyncio
@pytest.mark.parametrize("actual", [[-1.0, 0.0], [1e308, 1e308]])
async def test_invalid_blend_falls_back_to_valid_actual(generator, dependencies, actual):
    # Opposing vectors cancel at equal weights; huge finite coordinates overflow
    # during weighting. Neither case should discard a valid actual-only vector.
    dependencies.prediction.return_value = replace(
        dependencies.prediction.return_value, embedding=actual
    )
    result = await generator.generate(object(), "did:plc:user1")
    assert dependencies.knn.await_args.args[1] == actual
    assert result.reason == "average_user_embedding_invalid_blend"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vector", [None, [], [0.0, 0.0], [float("nan"), 1], [float("inf"), 1], [True, 1]]
)
async def test_invalid_actual_fails_instead_of_substituting_prior(generator, dependencies, vector):
    dependencies.prediction.return_value = replace(
        dependencies.prediction.return_value, embedding=vector
    )
    with pytest.raises(InferenceResponseFormatError, match="finite nonzero user embedding"):
        await generator.generate(object(), "did:plc:user1")
    dependencies.knn.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_prediction_metadata_fails(generator, dependencies):
    dependencies.prediction.return_value = replace(
        dependencies.prediction.return_value, post_model_uuid=None
    )
    with pytest.raises(InferenceResponseFormatError, match="model pair metadata"):
        await generator.generate(object(), "did:plc:user1")
    dependencies.knn.assert_not_awaited()


@pytest.mark.asyncio
async def test_does_not_call_readiness(generator, dependencies):
    # A prediction's model pair must drive retrieval even if /ready could report
    # a different model during a rollout. The readiness helper still exists for others.
    with patch("app.lib.inference.get_cached_post_tower_uuid", new_callable=AsyncMock) as ready:
        await generator.generate(object(), "did:plc:user1")
    ready.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh_hours", [24, 168, 720, 876000])
async def test_forwards_all_retrieval_options(generator, dependencies, fresh_hours):
    es = object()
    result = await generator.generate(
        es,
        "did:plc:user1",
        num_candidates=12,
        video_only=True,
        exclude_uris=["at://seen/1"],
        max_age_hours=fresh_hours,
    )
    dependencies.knn.assert_awaited_once_with(
        es,
        [0.5, 0.5],
        12,
        search_field=GE_POST_EMBEDDING_FIELD,
        generator_name="two_tower",
        video_only=True,
        exclude_uris=["at://seen/1"],
        ge_post_embedding_model_uuid=POST_MODEL,
        min_like_count=None,
        max_age_hours=fresh_hours,
        index=POSTS_QUALITY_KNN_INDEX,
    )
    assert result.candidates == dependencies.knn.return_value


@pytest.mark.asyncio
async def test_defaults_and_zero_candidate_passthrough(generator, dependencies):
    await generator.generate(object(), "did:plc:user1", num_candidates=0)
    assert dependencies.knn.await_args.args[2] == 0
    options = dependencies.knn.await_args.kwargs
    assert options["video_only"] is False
    assert options["exclude_uris"] is None
    assert options["max_age_hours"] == 168


@pytest.mark.asyncio
async def test_empty_retrieval_reports_reason_without_second_search(generator, dependencies):
    dependencies.knn.return_value = []
    result = await generator.generate(object(), "did:plc:user1", exclude_uris=["at://seen/1"])
    dependencies.knn.assert_awaited_once()
    assert result.reason == "no_recent_authors_topics_posts"
    assert result.candidates == []


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency", ["history", "settings", "prediction", "knn"])
async def test_dependency_errors_propagate(generator, dependencies, dependency):
    getattr(dependencies, dependency).side_effect = RuntimeError(f"{dependency} unavailable")
    with pytest.raises(RuntimeError, match=f"{dependency} unavailable"):
        await generator.generate(object(), "did:plc:user1")
    if dependency != "knn":
        dependencies.knn.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_debug_recorder_is_required(generator, dependencies, monkeypatch):
    monkeypatch.setattr(two_tower, "current_recorder", lambda: None)
    result = await generator.generate(object(), "did:plc:user1")
    assert result.status == "success"

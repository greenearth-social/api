"""Tests for the static average embedding and its candidate retrieval path."""

import hashlib
import json
import logging
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from ...models import CandidateGenerateRequest, CandidatePost, GeneratorSpec
from ..elasticsearch import POSTS_KNN_INDEX, POSTS_QUALITY_KNN_INDEX
from ..embeddings import GE_POST_EMBEDDING_FIELD, encode_float32_b64
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from ..pipeline_context import DegradationStage, PipelineContext, pipeline_context_scope
from . import average_two_tower as average_module
from . import get_generator, list_generators
from .average_two_tower import AverageTwoTowerCandidateGenerator, load_average_embedding
from .base import CandidateResult
from .generate import GeneratorError, run_generate
from .two_tower import MIN_LIKE_COUNT

GENERATOR_NAME = "average_two_tower"
USER_MODEL_UUID = "1affd684bc7f45f895e488f83dd0a2fa"
POST_MODEL_UUID = "9b946f280fd84899a7f82246fbc34d17"
SOURCE_COMPLETED_AT = "2026-09-18T16:00:51.631499Z"


@pytest.fixture(autouse=True)
def _reset_embedding_cache():
    load_average_embedding.cache_clear()
    yield
    load_average_embedding.cache_clear()


@pytest.fixture
def asset_data():
    return {
        "embedding": [2.0, -3.0] + [0.0] * 126,
        "dimension": 128,
        "user_model_uuid": USER_MODEL_UUID,
        "post_model_uuid": POST_MODEL_UUID,
        "source_completed_at": SOURCE_COMPLETED_AT,
        "contributing_users": 68,
    }


@pytest.fixture
def asset_file(tmp_path, monkeypatch, asset_data):
    path = tmp_path / "average_user_embedding.json"
    path.write_text(json.dumps(asset_data))
    monkeypatch.setattr(average_module, "AVERAGE_EMBEDDING_PATH", path)
    return path


def test_bundled_asset_matches_the_completed_average_without_user_data():
    data = json.loads(average_module.AVERAGE_EMBEDDING_PATH.read_text())
    assert set(data) == {
        "embedding", "dimension", "user_model_uuid", "post_model_uuid",
        "source_completed_at", "contributing_users",
    }
    assert data["dimension"] == 128
    assert data["user_model_uuid"] == USER_MODEL_UUID
    assert data["post_model_uuid"] == POST_MODEL_UUID
    assert data["source_completed_at"] == SOURCE_COMPLETED_AT
    assert data["contributing_users"] == 68
    # Digest of the exact unnormalized vector in the completed 68-user output.
    digest = hashlib.sha256(
        json.dumps(data["embedding"], separators=(",", ":")).encode()
    ).hexdigest()
    assert digest == "3678e242fa90bb48fb955744a7771abf38a665f7b01ee3f31c8bb3ea62ade779"
    assert load_average_embedding().embedding == tuple(data["embedding"])


def test_asset_is_validated_once_and_cached_immutably(asset_file, asset_data):
    result = load_average_embedding()
    assert result.embedding == tuple(asset_data["embedding"])
    assert result.dimension == 128
    assert result.user_model_uuid == USER_MODEL_UUID
    assert result.post_model_uuid == POST_MODEL_UUID
    assert result.source_completed_at == SOURCE_COMPLETED_AT
    assert result.contributing_users == 68

    asset_file.write_text("not JSON")
    assert load_average_embedding() is result
    with pytest.raises(FrozenInstanceError):
        result.dimension = 2  # type: ignore[misc]
    with pytest.raises(TypeError):
        result.embedding[0] = 99.0  # type: ignore[index]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("embedding", []),
        ("embedding", [1.0] * 127),
        ("embedding", [0.0] * 128),
        ("embedding", [float("nan")] + [1.0] * 127),
        ("embedding", [float("inf")] + [1.0] * 127),
        ("embedding", [True] + [1.0] * 127),
        ("embedding", ["1"] + [1.0] * 127),
        ("embedding", None),
        ("dimension", 127),
        ("dimension", True),
        ("dimension", 128.0),
        ("user_model_uuid", ""),
        ("post_model_uuid", "   "),
        ("post_model_uuid", None),
        ("contributing_users", 0),
        ("contributing_users", True),
        ("contributing_users", 68.0),
        ("source_completed_at", "not a timestamp"),
        ("source_completed_at", "2026-09-18T16:00:51"),
        ("source_completed_at", "2026-09-18T16:00:51+05:00"),
    ],
)
def test_rejects_invalid_vectors_and_metadata(asset_file, asset_data, field, value):
    asset_data[field] = value
    asset_file.write_text(json.dumps(asset_data))
    with pytest.raises(ValueError):
        load_average_embedding()


@pytest.mark.parametrize("contents", ["not JSON", "[]", "{}", "null"])
def test_rejects_malformed_or_incomplete_assets(asset_file, contents):
    asset_file.write_text(contents)
    with pytest.raises(ValueError):
        load_average_embedding()


def test_missing_asset_is_not_replaced_with_a_default_vector(asset_file):
    asset_file.unlink()
    with pytest.raises(OSError):
        load_average_embedding()


def test_registered_as_builtin_generator():
    assert isinstance(get_generator(GENERATOR_NAME), AverageTwoTowerCandidateGenerator)
    assert GENERATOR_NAME in list_generators()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("index_override", "expected_index", "min_like_count"),
    [(None, POSTS_QUALITY_KNN_INDEX, None), (POSTS_KNN_INDEX, POSTS_KNN_INDEX, MIN_LIKE_COUNT)],
)
async def test_retrieval_uses_exact_average_and_forwards_filters(
    asset_file, asset_data, monkeypatch, index_override, expected_index, min_like_count
):
    if index_override is None:
        monkeypatch.delenv("GE_TWO_TOWER_KNN_INDEX", raising=False)
    else:
        monkeypatch.setenv("GE_TWO_TOWER_KNN_INDEX", index_override)
    es = object()
    posts = [CandidatePost(at_uri="at://post/one", generator_name=GENERATOR_NAME, score=0.8)]
    knn_search = AsyncMock(return_value=posts)
    monkeypatch.setattr(average_module, "knn_search_posts", knn_search)

    result = await AverageTwoTowerCandidateGenerator().generate(
        es, "did:plc:first", num_candidates=7, video_only=True,
        exclude_uris=["at://post/seen"], max_age_hours=72,
    )

    knn_search.assert_awaited_once_with(
        es, asset_data["embedding"], 7,
        search_field=GE_POST_EMBEDDING_FIELD,
        generator_name=GENERATOR_NAME,
        video_only=True,
        exclude_uris=["at://post/seen"],
        ge_post_embedding_model_uuid=POST_MODEL_UUID,
        min_like_count=min_like_count,
        max_age_hours=72,
        index=expected_index,
    )
    assert result.generator_name == GENERATOR_NAME
    assert result.candidates == posts
    assert result.reason is None


@pytest.mark.asyncio
async def test_users_share_the_average_without_history_or_inference(
    asset_file, asset_data, monkeypatch
):
    knn_search = AsyncMock(return_value=[])
    monkeypatch.setattr(average_module, "knn_search_posts", knn_search)
    # No configured inference service is needed, even for a DID with no history.
    monkeypatch.delenv("GE_INFERENCE_BASE_URL", raising=False)
    monkeypatch.delenv("GE_INFERENCE_API_KEY", raising=False)
    with (
        patch("app.lib.inference.compute_user_embedding", new_callable=AsyncMock) as compute,
        patch("app.lib.inference.predict_user_tower_single", new_callable=AsyncMock) as predict,
        patch("app.lib.user_history_cache.fetch_user_history_features", new_callable=AsyncMock)
        as history,
        patch("app.lib.inference.get_cached_post_tower_uuid", new_callable=AsyncMock) as uuid,
    ):
        generator = AverageTwoTowerCandidateGenerator()
        for did in ("did:plc:first", "did:plc:nohistory"):
            result = await generator.generate(object(), did)
            assert result.candidates == []
            assert result.reason == "no_recent_average_embedding_posts"

    compute.assert_not_awaited()
    predict.assert_not_awaited()
    history.assert_not_awaited()
    uuid.assert_not_awaited()
    assert knn_search.await_count == 2
    first, second = knn_search.await_args_list
    assert first.args[1] == second.args[1] == asset_data["embedding"]
    assert first.args[1] is not second.args[1]
    first.args[1][0] = 99
    assert load_average_embedding().embedding == tuple(asset_data["embedding"])
    assert second.args[1] == asset_data["embedding"]
    assert second.args[2] == 100
    assert second.kwargs["video_only"] is False
    assert second.kwargs["exclude_uris"] is None
    assert second.kwargs["max_age_hours"] == 168


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("ES unavailable"), ValueError("bad asset")])
async def test_pipeline_preserves_popularity_and_records_average_diagnostics(
    asset_file, monkeypatch, caplog, failure
):
    knn_search = AsyncMock(return_value=[])
    monkeypatch.setattr(average_module, "knn_search_posts", knn_search)
    if isinstance(failure, ValueError):
        asset_file.write_text("{}")
    elif failure is not None:
        knn_search.side_effect = failure
    popular_post = CandidatePost(
        at_uri="at://post/popular", generator_name="popularity",
        minilm_l12_embedding=encode_float32_b64([1.0, 2.0]), politics_score=0.2,
    )
    popularity = get_generator("popularity")
    assert popularity is not None
    popular_generate = AsyncMock(
        return_value=CandidateResult(generator_name="popularity", candidates=[popular_post])
    )
    monkeypatch.setattr(popularity, "generate", popular_generate)
    request = CandidateGenerateRequest(
        user_did="did:plc:test",
        generators=[
            GeneratorSpec(name="popularity", weight=0.5),
            GeneratorSpec(name=GENERATOR_NAME, weight=0.5),
        ],
        num_candidates=2, video_only=False, max_age_hours=168, infill=None,
    )
    recorder = FeedDebugRecorder(feed_name="cold-start", regenerated=True)
    recorder.set_generate_request(request)
    context = PipelineContext(feed_name="cold-start")
    with pipeline_context_scope(context), feed_debug_scope(recorder), caplog.at_level(logging.INFO):
        result = await run_generate(request, es=object())

    assert result.candidates == [popular_post]
    popular_generate.assert_awaited_once()
    assert popular_generate.await_args is not None
    assert popular_generate.await_args.kwargs["num_candidates"] == 1
    assert popular_post.at_uri is not None
    recorder.record_final_order([popular_post.at_uri])
    now = datetime.now(UTC)
    metadata = recorder.build_pipeline_metadata(
        request_id="test", generated_at=now, expires_at=now + timedelta(minutes=15)
    )
    diagnostics = {entry.name: entry for entry in metadata.generator_diagnostics}
    average_diagnostic = diagnostics[GENERATOR_NAME]
    assert average_diagnostic.requested_count == 1
    assert average_diagnostic.returned_count == 0
    assert average_diagnostic.contributed_count == 0
    assert diagnostics["popularity"].contributed_count == 1
    if failure is None:
        assert average_diagnostic.status == "empty"
        assert average_diagnostic.reason == "no_recent_average_embedding_posts"
        assert context.degradations == []
    else:
        assert average_diagnostic.status == "error"
        assert average_diagnostic.reason == "generator_error"
        assert len(context.degradations) == 1
        assert context.degradations[0].stage == DegradationStage.CANDIDATE_GEN
        assert context.degradations[0].component == GENERATOR_NAME
        assert "Candidate generator 'average_two_tower' failed" in caplog.text


@pytest.mark.asyncio
async def test_standalone_request_propagates_retrieval_failure(asset_file, monkeypatch):
    monkeypatch.setattr(
        average_module, "knn_search_posts", AsyncMock(side_effect=RuntimeError("ES unavailable"))
    )
    request = CandidateGenerateRequest(
        user_did="did:plc:test", generators=[GeneratorSpec(name=GENERATOR_NAME, weight=1.0)],
        num_candidates=100, video_only=False, max_age_hours=168, infill=None,
    )
    with pytest.raises(GeneratorError, match="average_two_tower.*ES unavailable"):
        await run_generate(request, es=object())

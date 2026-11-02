"""Per-generator rank-score normalization in the slate pipeline (issue #572)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ..models import (
    CandidateGenerateRequest,
    CandidateGenerateResult,
    CandidatePost,
    GeneratorSpec,
    RankedCandidate,
    RankModelSpec,
    RankPredictRequest,
    RankPredictResult,
    SlateConfig,
)
from .embeddings import encode_float32_b64
from .feed_debug import FeedDebugRecorder, feed_debug_scope
from .slate_pipeline import normalize_scores_by_generator, run_ranking_pipeline

EMBEDDING = encode_float32_b64([0.0] * 383 + [1.0])


def _post(uri: str, score: float, generator: str | None) -> CandidatePost:
    return CandidatePost(
        at_uri=uri, score=score, generator_name=generator, minilm_l12_embedding=EMBEDDING
    )


def _scores(posts: list[CandidatePost]) -> dict[str, float]:
    return {p.at_uri: p.score for p in posts if p.at_uri and p.score is not None}


class TestNormalizeScoresByGenerator:
    def test_each_generator_spans_zero_to_one(self):
        ordered = [
            _post("at://f/1", 0.9, "followed_users"),
            _post("at://f/2", 0.7, "followed_users"),
            _post("at://f/3", 0.5, "followed_users"),
            _post("at://l/1", 0.4, "llm_query_vector"),
            _post("at://l/2", 0.2, "llm_query_vector"),
        ]

        out = normalize_scores_by_generator(ordered)

        assert _scores(out) == pytest.approx(
            {"at://f/1": 1.0, "at://f/2": 0.5, "at://f/3": 0.0, "at://l/1": 1.0, "at://l/2": 0.0}
        )

    def test_resorts_by_normalized_score_and_interleaves_generators(self):
        # Every LLM post scores below every followed-users post before
        # normalization; afterwards the LLM source's best post ties the top.
        ordered = [
            _post("at://f/1", 0.9, "followed_users"),
            _post("at://f/2", 0.7, "followed_users"),
            _post("at://f/3", 0.5, "followed_users"),
            _post("at://l/1", 0.4, "llm_query_vector"),
            _post("at://l/2", 0.3, "llm_query_vector"),
            _post("at://l/3", 0.2, "llm_query_vector"),
        ]

        out = [p.at_uri for p in normalize_scores_by_generator(ordered)]

        # Ties keep the incoming raw-score order (stable sort).
        assert out == ["at://f/1", "at://l/1", "at://f/2", "at://l/2", "at://f/3", "at://l/3"]

    def test_lone_and_constant_groups_map_to_one(self):
        ordered = [
            _post("at://f/1", 0.9, "followed_users"),
            _post("at://f/2", 0.1, "followed_users"),
            _post("at://p/1", 0.3, "popularity"),
            _post("at://t/1", 0.2, "two_tower"),
            _post("at://t/2", 0.2, "two_tower"),
        ]

        out = _scores(normalize_scores_by_generator(ordered))

        assert out["at://p/1"] == 1.0
        assert out["at://t/1"] == 1.0 and out["at://t/2"] == 1.0

    def test_missing_generator_name_forms_its_own_group(self):
        ordered = [
            _post("at://f/1", 0.9, "followed_users"),
            _post("at://x/1", 0.6, None),
            _post("at://x/2", 0.4, None),
            _post("at://f/2", 0.1, "followed_users"),
        ]

        out = _scores(normalize_scores_by_generator(ordered))

        assert out == pytest.approx(
            {"at://f/1": 1.0, "at://x/1": 1.0, "at://x/2": 0.0, "at://f/2": 0.0}
        )

    def test_returns_copies_and_leaves_input_untouched(self):
        ordered = [_post("at://f/1", 0.9, "f"), _post("at://f/2", 0.1, "f")]

        normalize_scores_by_generator(ordered)

        assert [p.score for p in ordered] == [0.9, 0.1]

    def test_empty_input(self):
        assert normalize_scores_by_generator([]) == []


# --- pipeline integration -------------------------------------------------

CANDIDATES = [
    _post("at://f/1", 0.0, "followed_users"),
    _post("at://f/2", 0.0, "followed_users"),
    _post("at://f/3", 0.0, "followed_users"),
    _post("at://l/1", 0.0, "llm_query_vector"),
    _post("at://l/2", 0.0, "llm_query_vector"),
    _post("at://l/3", 0.0, "llm_query_vector"),
]
# Heavy-ranker-style scores: every LLM post below every followed-users post,
# and the weakest LLM post below the 0.15 quality floor.
RANKINGS = RankPredictResult(
    rankings=[
        RankedCandidate(at_uri="at://f/1", rank=1, rank_score=0.9),
        RankedCandidate(at_uri="at://f/2", rank=2, rank_score=0.7),
        RankedCandidate(at_uri="at://f/3", rank=3, rank_score=0.5),
        RankedCandidate(at_uri="at://l/1", rank=4, rank_score=0.4),
        RankedCandidate(at_uri="at://l/2", rank=5, rank_score=0.3),
        RankedCandidate(at_uri="at://l/3", rank=6, rank_score=0.1),
    ]
)


def _slate(**overrides) -> SlateConfig:
    template = RankPredictRequest.model_construct(
        candidates=[],
        models=[RankModelSpec(name="heavy_ranker", weight=1.0)],
        user_did="",
        politics=1.0,
    )
    fields = {
        "rank_request_template": template,
        "diversify": False,
        "min_rank_score": 0.15,
        "max_render_share": 0.5,
    }
    fields.update(overrides)
    return SlateConfig(**fields)


def _gen_request() -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[
            GeneratorSpec(name="followed_users", weight=0.5),
            GeneratorSpec(name="llm_query_vector", weight=0.5),
        ],
        user_did="did:plc:user",
        num_candidates=6,
    )


async def _run(slate: SlateConfig, recorder: FeedDebugRecorder | None = None):
    with (
        patch(
            "app.lib.slate_pipeline.run_generate",
            new_callable=AsyncMock,
            return_value=CandidateGenerateResult(candidates=CANDIDATES),
        ),
        patch(
            "app.lib.slate_pipeline.hydrate_posts",
            new_callable=AsyncMock,
            return_value=CANDIDATES,
        ),
        patch(
            "app.lib.slate_pipeline.run_predict",
            new_callable=AsyncMock,
            return_value=RANKINGS,
        ),
    ):
        if recorder is None:
            return await run_ranking_pipeline(
                slate, _gen_request(), MagicMock(), feed_name="your-feed"
            )
        with feed_debug_scope(recorder):
            return await run_ranking_pipeline(
                slate, _gen_request(), MagicMock(), feed_name="your-feed"
            )


@pytest.mark.asyncio
async def test_flag_off_serves_raw_rank_order():
    result = await _run(_slate())

    # Top half by raw score: the LLM source is shut out entirely.
    assert result.uris == ["at://f/1", "at://f/2", "at://f/3"]
    assert result.low_score_uris == ["at://l/3"]


@pytest.mark.asyncio
async def test_flag_on_cuts_on_raw_score_then_grades_each_generator_on_its_curve():
    recorder = FeedDebugRecorder(feed_name="your-feed", regenerated=False)

    result = await _run(_slate(normalize_rank_by_generator=True), recorder)

    # The absolute floor still applies to the raw score ...
    assert result.low_score_uris == ["at://l/3"]
    # ... then survivors interleave: both sources reach the served half.
    assert result.uris == ["at://f/1", "at://l/1", "at://f/2"]
    # Ties (f/1 & l/1 at 1.0, f/3 & l/2 at 0.0) keep the raw-score order.
    assert recorder.order_after_rank == [
        "at://f/1",
        "at://l/1",
        "at://f/2",
        "at://f/3",
        "at://l/2",
    ]
    assert recorder.rank_normalization == [
        ("at://f/1", "followed_users", 0.9, 1.0),
        ("at://l/1", "llm_query_vector", 0.4, 1.0),
        ("at://f/2", "followed_users", 0.7, pytest.approx(0.5)),
        ("at://f/3", "followed_users", 0.5, 0.0),
        ("at://l/2", "llm_query_vector", 0.3, 0.0),
    ]


@pytest.mark.asyncio
async def test_flag_on_without_ranker_is_a_no_op():
    recorder = FeedDebugRecorder(feed_name="your-feed", regenerated=False)

    result = await _run(
        _slate(rank_request_template=None, min_rank_score=None, normalize_rank_by_generator=True),
        recorder,
    )

    assert len(result.uris) == 3
    assert recorder.rank_normalization == []


@pytest.mark.asyncio
async def test_flag_on_skips_normalization_when_ranking_fails_open():
    recorder = FeedDebugRecorder(feed_name="your-feed", regenerated=False)
    slate = _slate(normalize_rank_by_generator=True, min_rank_score=None)
    from .pipeline_context import PipelineContext, pipeline_context_scope

    with (
        patch(
            "app.lib.slate_pipeline.run_generate",
            new_callable=AsyncMock,
            return_value=CandidateGenerateResult(candidates=CANDIDATES),
        ),
        patch(
            "app.lib.slate_pipeline.hydrate_posts", new_callable=AsyncMock, return_value=CANDIDATES
        ),
        patch(
            "app.lib.slate_pipeline.run_predict",
            new_callable=AsyncMock,
            side_effect=RuntimeError("ranker down"),
        ),
        feed_debug_scope(recorder),
        pipeline_context_scope(PipelineContext(feed_name="your-feed")),
    ):
        result = await run_ranking_pipeline(
            slate, _gen_request(), MagicMock(), feed_name="your-feed"
        )

    assert len(result.uris) == 3
    assert recorder.rank_normalization == []

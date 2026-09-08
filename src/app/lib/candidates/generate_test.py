"""Tests for run_generate pipeline degradation behavior."""

from __future__ import annotations

import asyncio
import logging
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from ...models import CandidateGenerateRequest, CandidatePost, GeneratorSpec, MaxAgeHours
from ..candidates import generate as generate_module
from ..candidates.base import CandidateGenerator, CandidateResult
from ..candidates.generate import GeneratorError, run_generate
from ..config import set_fail_fast_for_request
from ..embeddings import decode_float32_b64, encode_float32_b64
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from ..metrics import MetricCollector, set_metric_collector
from ..pipeline_context import (
    DegradationStage,
    PipelineContext,
    pipeline_context_scope,
)

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _HangingGenerator(CandidateGenerator):
    def __init__(self, name: str = "hanging"):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        await asyncio.sleep(9999)
        raise AssertionError("unreachable")


class _FailingGenerator(CandidateGenerator):
    def __init__(self, exc: Exception, name: str = "failing"):
        self._exc = exc
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        raise self._exc


class _EmptyGenerator(CandidateGenerator):
    def __init__(self, name: str = "empty"):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        return CandidateResult(generator_name=self.name, candidates=[])


class _StaticGenerator(CandidateGenerator):
    def __init__(self, name: str, candidates: list[CandidatePost]):
        self._name = name
        self._candidates = candidates
        self.calls: list[dict] = []

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        self.calls.append(
            {
                "num_candidates": num_candidates,
                "video_only": video_only,
                "exclude_uris": exclude_uris,
                "max_age_hours": max_age_hours,
            }
        )
        excluded = set(exclude_uris or [])
        candidates = [c for c in self._candidates if c.at_uri not in excluded]
        return CandidateResult(generator_name=self.name, candidates=candidates[:num_candidates])


class _FailThenReturnGenerator(CandidateGenerator):
    def __init__(self, name: str, candidates: list[CandidatePost]):
        self._name = name
        self._candidates = candidates
        self.calls = 0

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("primary failed")
        return CandidateResult(
            generator_name=self.name, candidates=self._candidates[:num_candidates]
        )


class FakeMetricCollector:
    def __init__(self):
        self.calls: list[tuple[str, float, dict]] = []

    def record(self, name: str, value: float, **attributes: str) -> None:
        self.calls.append((name, value, dict(attributes)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    generator_name: str,
    *,
    num_candidates: int = 5,
    infill: str | None = None,
    exclude_uris: list[str] | None = None,
    max_age_hours: MaxAgeHours = 168,
) -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name=generator_name, weight=1.0)],
        user_did="did:plc:test",
        num_candidates=num_candidates,
        video_only=False,
        infill=infill,
        exclude_uris=exclude_uris or [],
        max_age_hours=max_age_hours,
    )


def _candidate(uri: str, generator_name: str = "test") -> CandidatePost:
    return CandidatePost(at_uri=uri, generator_name=generator_name)


def _stub_generators(monkeypatch, mapping: dict) -> None:
    monkeypatch.setattr(generate_module, "get_generator", lambda name: mapping.get(name))


@pytest.fixture(autouse=True)
def _reset_metric_collector():
    yield
    set_metric_collector(None)


# ---------------------------------------------------------------------------
# Post hydration
# ---------------------------------------------------------------------------


class TestPostHydration:
    @pytest.mark.asyncio
    async def test_fills_missing_embeddings_and_politics_scores_without_overwriting_values(
        self, monkeypatch
    ):
        existing_embedding = encode_float32_b64([9.0, 8.0])
        candidates = [
            CandidatePost(at_uri="at://post/both-missing"),
            CandidatePost(
                at_uri="at://post/politics-missing",
                minilm_l12_embedding=existing_embedding,
            ),
            CandidatePost(
                at_uri="at://post/embedding-missing",
                politics_score=0.4,
            ),
            CandidatePost(
                at_uri="at://post/complete",
                minilm_l12_embedding=existing_embedding,
                politics_score=0.6,
            ),
        ]
        fetch = AsyncMock(
            return_value=[
                ("at://post/both-missing", [1.0, 2.0], 0.0),
                ("at://post/politics-missing", [3.0, 4.0], 0.25),
                ("at://post/embedding-missing", [5.0, 6.0], 0.9),
            ]
        )
        monkeypatch.setattr(
            generate_module,
            "fetch_post_embeddings_and_politics_scores",
            fetch,
        )
        es = object()

        hydrated = await generate_module.hydrate_posts(es, candidates)

        fetch.assert_awaited_once_with(
            es,
            [
                "at://post/both-missing",
                "at://post/politics-missing",
                "at://post/embedding-missing",
            ],
            index="posts_recent",
        )
        assert hydrated[0].minilm_l12_embedding == encode_float32_b64([1.0, 2.0])
        assert hydrated[0].politics_score == 0.0
        assert hydrated[1].minilm_l12_embedding == existing_embedding
        assert hydrated[1].politics_score == 0.25
        assert hydrated[2].minilm_l12_embedding == encode_float32_b64([5.0, 6.0])
        assert hydrated[2].politics_score == 0.4
        assert hydrated[3] is candidates[3]

    @pytest.mark.asyncio
    async def test_skips_es_when_every_candidate_is_fully_hydrated(self, monkeypatch):
        candidates = [
            CandidatePost(
                at_uri="at://post/complete",
                minilm_l12_embedding=encode_float32_b64([1.0, 2.0]),
                politics_score=0.0,
            )
        ]
        fetch = AsyncMock()
        monkeypatch.setattr(
            generate_module,
            "fetch_post_embeddings_and_politics_scores",
            fetch,
        )

        hydrated = await generate_module.hydrate_posts(object(), candidates)

        assert hydrated is candidates
        fetch.assert_not_awaited()


# ---------------------------------------------------------------------------
# Main generator timeout tests
# (soft-fail now requires a PipelineContext; hard-fail is the no-context default)
# ---------------------------------------------------------------------------


class TestGeneratorTimeout:
    @pytest.mark.asyncio
    async def test_timeout_swallow_returns_no_candidates_and_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"slow_generator": _HangingGenerator("slow_generator")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(_make_request("slow_generator"), es=None)

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        name, value, attrs = failure_calls[0]
        assert value == 1
        assert attrs == {
            "generator_name": "slow_generator",
            "outcome": "timeout",
            "is_infill": "false",
        }

    @pytest.mark.asyncio
    async def test_timeout_swallow_logs_warning_not_exception(self, monkeypatch, caplog):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"slow_generator": _HangingGenerator("slow_generator")})

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            with caplog.at_level(logging.WARNING):
                await run_generate(_make_request("slow_generator"), es=None)

        timeout_warnings = [
            r for r in caplog.records if "timed out" in r.message and r.levelno == logging.WARNING
        ]
        error_logs = [
            r
            for r in caplog.records
            if r.levelno >= logging.ERROR and "slow_generator" in r.message
        ]
        assert len(timeout_warnings) == 1
        assert len(error_logs) == 0

    @pytest.mark.asyncio
    async def test_timeout_no_swallow_raises_generator_error_promptly(self, monkeypatch):
        set_fail_fast_for_request(True)
        # No PipelineContext installed → hard fail (GeneratorError)
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"slow_generator": _HangingGenerator("slow_generator")})

        with pytest.raises(GeneratorError) as exc_info:
            await asyncio.wait_for(
                run_generate(_make_request("slow_generator"), es=None),
                timeout=1.0,
            )

        assert exc_info.value.name == "slow_generator"

    @pytest.mark.asyncio
    async def test_timeout_no_swallow_records_metric_before_raising(self, monkeypatch):
        set_fail_fast_for_request(True)
        # No PipelineContext installed → hard fail (GeneratorError)
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"slow_generator": _HangingGenerator("slow_generator")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        with pytest.raises(GeneratorError):
            await run_generate(_make_request("slow_generator"), es=None)

        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs["outcome"] == "timeout"
        assert attrs["is_infill"] == "false"

    @pytest.mark.asyncio
    async def test_exception_records_error_outcome_metric(self, monkeypatch):
        gen = _FailingGenerator(ValueError("boom"), name="network_likes")
        _stub_generators(monkeypatch, {"network_likes": gen})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(_make_request("network_likes"), es=None)

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "network_likes",
            "outcome": "error",
            "is_infill": "false",
        }

    @pytest.mark.asyncio
    async def test_swallowed_primary_failure_records_infill_debug_output(self, monkeypatch):
        # With PipelineContext: primary failure is degraded (an empty
        # status="error" CandidateResult is recorded, not skipped, so debug/
        # transparency views can show why the generator contributed nothing),
        # and the infill success is recorded separately.
        gen = _FailThenReturnGenerator("popular", [_candidate("at://infill/1", "popular")])
        _stub_generators(monkeypatch, {"popular": gen})
        rec = FeedDebugRecorder(feed_name="f", regenerated=False)

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            with feed_debug_scope(rec):
                result = await run_generate(
                    _make_request("popular", num_candidates=1, infill="popular"),
                    es=None,
                )

        assert [c.at_uri for c in result.candidates] == ["at://infill/1"]
        assert [
            (output.generator_name, output.status, [c.at_uri for c in output.candidates])
            for output in rec.generator_outputs
        ] == [
            ("popular", "error", []),
            ("popular", "success", ["at://infill/1"]),
        ]

    @pytest.mark.asyncio
    async def test_success_records_success_count_metric(self, monkeypatch):
        _stub_generators(monkeypatch, {"popular": _EmptyGenerator("popular")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        await run_generate(_make_request("popular"), es=None)

        success_calls = [c for c in mc.calls if c[0] == "candidates.generate.success_count"]
        assert len(success_calls) == 1
        _, _, attrs = success_calls[0]
        assert attrs == {"generator_name": "popular", "is_infill": "false"}

    @pytest.mark.asyncio
    async def test_success_records_fill_share_metric(self, monkeypatch):
        """A generator that under-fills its allocation is visible per generator."""
        _stub_generators(monkeypatch, {"popular": _EmptyGenerator("popular")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        await run_generate(_make_request("popular", num_candidates=4), es=None)

        fill_calls = [c for c in mc.calls if c[0] == "candidates.generate.fill_share"]
        assert len(fill_calls) == 1
        _, value, attrs = fill_calls[0]
        assert value == 0.0
        assert attrs == {"generator_name": "popular", "is_infill": "false"}


# ---------------------------------------------------------------------------
# Infill generator timeout/error tests
# ---------------------------------------------------------------------------


class TestInfillGeneratorTimeout:
    @pytest.mark.asyncio
    async def test_empty_selected_source_does_not_run_fallback_without_infill(self, monkeypatch):
        selected = _EmptyGenerator("network_likes")
        fallback = _StaticGenerator("popular", [_candidate("at://fallback", "popular")])
        get_generator = MagicMock(
            side_effect=lambda name: {
                "network_likes": selected,
                "popular": fallback,
            }.get(name)
        )
        monkeypatch.setattr(generate_module, "get_generator", get_generator)

        result = await run_generate(
            _make_request("network_likes", num_candidates=5, infill=None),
            es=None,
        )

        assert result.candidates == []
        get_generator.assert_called_once_with("network_likes")
        assert fallback.calls == []

    @pytest.mark.asyncio
    async def test_infill_timeout_swallow_returns_empty_and_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(
            monkeypatch,
            {
                "random": _EmptyGenerator("random"),
                "popular": _HangingGenerator("popular"),
            },
        )
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
            )

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "popular",
            "outcome": "timeout",
            "is_infill": "true",
        }

    @pytest.mark.asyncio
    async def test_infill_timeout_no_swallow_raises_generator_error_with_is_infill(
        self, monkeypatch
    ):
        set_fail_fast_for_request(True)
        # No PipelineContext → hard fail
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(
            monkeypatch,
            {
                "random": _EmptyGenerator("random"),
                "popular": _HangingGenerator("popular"),
            },
        )

        with pytest.raises(GeneratorError) as exc_info:
            await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
            )

        assert exc_info.value.name == "popular"
        assert exc_info.value.is_infill is True

    @pytest.mark.asyncio
    async def test_infill_timeout_no_swallow_records_metric(self, monkeypatch):
        set_fail_fast_for_request(True)
        # No PipelineContext → hard fail
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(
            monkeypatch,
            {
                "random": _EmptyGenerator("random"),
                "popular": _HangingGenerator("popular"),
            },
        )
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        with pytest.raises(GeneratorError):
            await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
            )

        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs["is_infill"] == "true"
        assert attrs["outcome"] == "timeout"

    @pytest.mark.asyncio
    async def test_infill_exception_records_error_outcome(self, monkeypatch):
        _stub_generators(
            monkeypatch,
            {
                "random": _EmptyGenerator("random"),
                "popular": _FailingGenerator(RuntimeError("db down"), name="popular"),
            },
        )
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
            )

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "popular",
            "outcome": "error",
            "is_infill": "true",
        }

    @pytest.mark.asyncio
    async def test_infill_success_records_success_count_metric(self, monkeypatch):
        _stub_generators(
            monkeypatch,
            {
                "random": _EmptyGenerator("random"),
                "popular": _EmptyGenerator("popular"),
            },
        )
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        await run_generate(
            _make_request("random", num_candidates=5, infill="popular"),
            es=None,
        )

        success_calls = [c for c in mc.calls if c[0] == "candidates.generate.success_count"]
        assert len(success_calls) == 2  # one for "random", one for "popular" infill
        infill_success = [c for c in success_calls if c[2].get("is_infill") == "true"]
        assert len(infill_success) == 1
        assert infill_success[0][2] == {"generator_name": "popular", "is_infill": "true"}

    @pytest.mark.asyncio
    async def test_infill_excludes_request_and_primary_candidate_uris(self, monkeypatch):
        primary = _StaticGenerator(
            "popular",
            [
                _candidate("at://seen/1", "popular"),
                _candidate("at://primary/1", "popular"),
                _candidate("at://primary/2", "popular"),
            ],
        )
        infill = _StaticGenerator(
            "popular_infill",
            [
                _candidate("at://seen/1", "popular_infill"),
                _candidate("at://primary/1", "popular_infill"),
                _candidate("at://primary/2", "popular_infill"),
                _candidate("at://infill/1", "popular_infill"),
                _candidate("at://infill/2", "popular_infill"),
            ],
        )
        _stub_generators(monkeypatch, {"popular": primary, "popular_infill": infill})

        result = await run_generate(
            _make_request(
                "popular",
                num_candidates=4,
                infill="popular_infill",
                exclude_uris=["at://seen/1"],
            ),
            es=None,
        )

        assert infill.calls[0]["exclude_uris"] == [
            "at://seen/1",
            "at://primary/1",
            "at://primary/2",
        ]
        assert primary.calls[0]["max_age_hours"] == 168
        assert infill.calls[0]["max_age_hours"] == 168
        assert [c.at_uri for c in result.candidates] == [
            "at://primary/1",
            "at://primary/2",
            "at://infill/1",
            "at://infill/2",
        ]


# ---------------------------------------------------------------------------
# New: PipelineContext degradation tests
# ---------------------------------------------------------------------------


def _request(*generator_names: str) -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name=n, weight=1.0) for n in generator_names],
        user_did="did:plc:user",
        num_candidates=10,
        video_only=False,
        infill=None,
        max_age_hours=168,
    )


class _FakeGenerator(CandidateGenerator):
    def __init__(self, name_: str, *, fail: bool = False, cause: Exception | None = None):
        self._name = name_
        self._fail = fail
        self._cause = cause or RuntimeError(f"{name_} failed")

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        if self._fail:
            raise self._cause
        return CandidateResult(
            generator_name=self._name,
            candidates=[CandidatePost(at_uri=f"at://{self._name}/1", score=1.0)],
        )


def _make_failing(name: str, *, cause: Exception | None = None) -> _FakeGenerator:
    return _FakeGenerator(name, fail=True, cause=cause)


def _make_success(name: str) -> _FakeGenerator:
    return _FakeGenerator(name, fail=False)


class TestNoPipelineContext:
    @pytest.mark.asyncio
    async def test_generator_failure_raises_without_context(self, monkeypatch):
        monkeypatch.setattr(
            "app.lib.candidates.generate.get_generator",
            lambda name: _make_failing(name),
        )
        with pytest.raises(GeneratorError):
            await run_generate(_request("two_tower"), es=object())

    @pytest.mark.asyncio
    async def test_generator_success_returns_candidates(self, monkeypatch):
        monkeypatch.setattr(
            "app.lib.candidates.generate.get_generator",
            lambda name: _make_success(name),
        )
        result = await run_generate(_request("two_tower"), es=object())
        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://two_tower/1"


class TestWithPipelineContext:
    @pytest.mark.asyncio
    async def test_generator_failure_records_degradation_and_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            "app.lib.candidates.generate.get_generator",
            lambda name: _make_failing(name),
        )
        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(_request("two_tower"), es=object())

        assert result.candidates == []
        assert len(ctx.degradations) == 1
        assert ctx.degradations[0].stage == DegradationStage.CANDIDATE_GEN
        assert ctx.degradations[0].component == "two_tower"

    @pytest.mark.asyncio
    async def test_partial_results_when_one_of_two_generators_fails(self, monkeypatch):
        def _get(name: str):
            return _make_failing(name) if name == "two_tower" else _make_success(name)

        monkeypatch.setattr("app.lib.candidates.generate.get_generator", _get)
        ctx = PipelineContext(feed_name="your-feed")
        with pipeline_context_scope(ctx):
            result = await run_generate(_request("two_tower", "followed_users"), es=object())

        assert len(result.candidates) == 1
        assert result.candidates[0].at_uri == "at://followed_users/1"
        assert len(ctx.degradations) == 1

    @pytest.mark.asyncio
    async def test_generator_failure_reraises_when_fail_fast(self, monkeypatch):
        cause = RuntimeError("es connection refused")
        monkeypatch.setattr(
            "app.lib.candidates.generate.get_generator",
            lambda name: _make_failing(name, cause=cause),
        )
        ctx = PipelineContext(feed_name="your-feed", fail_fast=True)
        with pipeline_context_scope(ctx):
            with pytest.raises(RuntimeError, match="es connection refused"):
                await run_generate(_request("two_tower"), es=object())

    @pytest.mark.asyncio
    async def test_infill_failure_records_degradation(self, monkeypatch):
        """Infill generator failure should also be tracked."""

        def _get(name: str):
            if name == "followed_users":
                return _make_success(name)
            return _make_failing(name)

        monkeypatch.setattr("app.lib.candidates.generate.get_generator", _get)
        ctx = PipelineContext(feed_name="your-feed")
        req = CandidateGenerateRequest(
            generators=[GeneratorSpec(name="followed_users", weight=1.0)],
            user_did="did:plc:user",
            num_candidates=10,
            video_only=False,
            infill="popularity",
            max_age_hours=168,
        )
        with pipeline_context_scope(ctx):
            result = await run_generate(req, es=object())

        assert len(result.candidates) == 1
        assert any(d.component == "popularity:infill" for d in ctx.degradations)


# ---------------------------------------------------------------------------
# Embedding hydration (request.hydrate_embeddings)
# ---------------------------------------------------------------------------


class _FixedGenerator(CandidateGenerator):
    """Returns a fixed candidate list, ignoring the allocated count.

    Lets a single generator drive dedup/truncation in the tests below.
    """

    def __init__(self, name: str, candidates: list[CandidatePost]):
        self._name = name
        self._candidates = candidates

    @property
    def name(self) -> str:
        return self._name

    async def generate(
        self,
        es,
        user_did,
        num_candidates=100,
        video_only=False,
        exclude_uris=None,
        max_age_hours=168,
    ):
        return CandidateResult(generator_name=self._name, candidates=list(self._candidates))


class _RecordingFetch:
    """Stands in for ``fetch_post_embeddings``, recording its call args."""

    def __init__(self, pairs: list[tuple[str, object]] | None = None):
        self._pairs = pairs or []
        self.calls: list[dict] = []

    async def __call__(self, es, at_uris, index="posts"):
        self.calls.append({"es": es, "at_uris": list(at_uris), "index": index})
        return list(self._pairs)


def _hydrate_request(
    generator_name: str,
    *,
    num_candidates: int = 5,
    hydrate: bool = True,
) -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name=generator_name, weight=1.0)],
        user_did="did:plc:test",
        num_candidates=num_candidates,
        video_only=False,
        infill=None,
        exclude_uris=[],
        max_age_hours=168,
        hydrate_embeddings=hydrate,
    )


class TestHydrateEmbeddingsRequestFlag:
    @pytest.mark.asyncio
    async def test_disabled_by_default_skips_es_fetch(self, monkeypatch):
        fetch = _RecordingFetch([("at://a", [1.0, 0.5])])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_make_request("gen"), es=object())

        assert fetch.calls == []
        assert result.candidates[0].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_explicitly_disabled_skips_es_fetch(self, monkeypatch):
        fetch = _RecordingFetch([("at://a", [1.0, 0.5])])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen", hydrate=False), es=object())

        assert fetch.calls == []
        assert result.candidates[0].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_enabled_hydrates_missing_embeddings_as_base64(self, monkeypatch):
        vec = [1.0, 0.5, -0.25]
        fetch = _RecordingFetch([("at://a", vec)])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert len(fetch.calls) == 1
        assert fetch.calls[0]["at_uris"] == ["at://a"]
        assert fetch.calls[0]["index"] == "posts_recent"

        encoded = result.candidates[0].minilm_l12_embedding
        assert isinstance(encoded, str)
        # Base64-encoded little-endian float32, per the CandidatePost contract.
        assert encoded == encode_float32_b64(vec)
        assert decode_float32_b64(encoded) == vec

    @pytest.mark.asyncio
    async def test_candidates_with_existing_embedding_are_not_refetched(self, monkeypatch):
        existing = encode_float32_b64([9.0, 9.0])
        candidates = [
            CandidatePost(at_uri="at://a", minilm_l12_embedding=existing),
            _candidate("at://b"),
        ]
        fetch = _RecordingFetch([("at://b", [1.0, 0.5])])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", candidates)})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert fetch.calls[0]["at_uris"] == ["at://b"]
        assert result.candidates[0].minilm_l12_embedding == existing
        encoded = result.candidates[1].minilm_l12_embedding
        assert isinstance(encoded, str)
        assert decode_float32_b64(encoded) == [1.0, 0.5]

    @pytest.mark.asyncio
    async def test_no_es_call_when_every_candidate_already_hydrated(self, monkeypatch):
        existing = encode_float32_b64([1.0])
        candidates = [CandidatePost(at_uri="at://a", minilm_l12_embedding=existing)]
        fetch = _RecordingFetch()
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", candidates)})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert fetch.calls == []
        assert result.candidates[0].minilm_l12_embedding == existing

    @pytest.mark.asyncio
    async def test_partial_es_result_hydrates_only_matched_candidates(self, monkeypatch):
        candidates = [_candidate("at://a"), _candidate("at://b")]
        # ES silently skips posts without a stored embedding.
        fetch = _RecordingFetch([("at://a", [1.0, 0.5])])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", candidates)})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert fetch.calls[0]["at_uris"] == ["at://a", "at://b"]
        assert [c.at_uri for c in result.candidates] == ["at://a", "at://b"]
        encoded = result.candidates[0].minilm_l12_embedding
        assert isinstance(encoded, str)
        assert decode_float32_b64(encoded) == [1.0, 0.5]
        assert result.candidates[1].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_unencodable_vector_is_skipped_without_dropping_candidates(self, monkeypatch):
        candidates = [_candidate("at://a"), _candidate("at://b")]
        fetch = _RecordingFetch([("at://a", [1.0, 0.5]), ("at://b", "not-a-vector")])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", candidates)})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert [c.at_uri for c in result.candidates] == ["at://a", "at://b"]
        encoded = result.candidates[0].minilm_l12_embedding
        assert isinstance(encoded, str)
        assert decode_float32_b64(encoded) == [1.0, 0.5]
        assert result.candidates[1].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_empty_es_result_leaves_candidates_unchanged(self, monkeypatch):
        fetch = _RecordingFetch([])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen"), es=object())

        assert len(fetch.calls) == 1
        assert result.candidates[0].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_hydration_runs_after_dedup_and_truncation(self, monkeypatch):
        # Duplicate "at://a" plus a third candidate beyond num_candidates: only the
        # deduped, truncated final slate should reach ES.
        candidates = [
            _candidate("at://a"),
            _candidate("at://a"),
            _candidate("at://b"),
            _candidate("at://c"),
        ]
        fetch = _RecordingFetch([("at://a", [1.0]), ("at://b", [0.5])])
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", candidates)})
        monkeypatch.setattr(generate_module, "fetch_post_embeddings", fetch)

        result = await run_generate(_hydrate_request("gen", num_candidates=2), es=object())

        assert len(fetch.calls) == 1
        assert fetch.calls[0]["at_uris"] == ["at://a", "at://b"]
        assert [c.at_uri for c in result.candidates] == ["at://a", "at://b"]
        assert all(c.minilm_l12_embedding is not None for c in result.candidates)


class TestHydrateEmbeddingsFailures:
    @pytest.mark.asyncio
    async def test_timeout_returns_unhydrated_candidates_and_records_degradation(
        self, monkeypatch, caplog
    ):
        monkeypatch.setattr(generate_module, "_EMBED_HYDRATION_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})

        async def _hangs(*args, **kwargs):
            await asyncio.sleep(9999)

        monkeypatch.setattr(generate_module, "fetch_post_embeddings", _hangs)

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            with caplog.at_level(logging.WARNING, logger=generate_module.logger.name):
                result = await run_generate(_hydrate_request("gen"), es=object())

        assert [c.at_uri for c in result.candidates] == ["at://a"]
        assert result.candidates[0].minilm_l12_embedding is None

        hydration_degradations = [
            d for d in ctx.degradations if d.stage == DegradationStage.EMBED_HYDRATION
        ]
        assert len(hydration_degradations) == 1
        assert hydration_degradations[0].component == "fetch_post_embeddings"
        assert isinstance(hydration_degradations[0].cause, TimeoutError)

        hydration_logs = [
            r
            for r in caplog.records
            if r.name == generate_module.logger.name and r.message.startswith("Embedding hydration")
        ]
        assert len(hydration_logs) == 1
        assert hydration_logs[0].levelno == logging.WARNING
        # An expected timeout is a warning without a traceback, and reports the
        # configured budget.
        assert "timed out after 0.0s" in hydration_logs[0].message
        assert hydration_logs[0].exc_info is None

    @pytest.mark.asyncio
    async def test_es_error_returns_unhydrated_candidates_and_records_degradation(
        self, monkeypatch, caplog
    ):
        failure = RuntimeError("Elasticsearch failed")
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})

        async def _raises(*args, **kwargs):
            raise failure

        monkeypatch.setattr(generate_module, "fetch_post_embeddings", _raises)

        ctx = PipelineContext(feed_name="test-feed")
        with pipeline_context_scope(ctx):
            with caplog.at_level(logging.ERROR, logger=generate_module.logger.name):
                result = await run_generate(_hydrate_request("gen"), es=object())

        assert [c.at_uri for c in result.candidates] == ["at://a"]
        assert result.candidates[0].minilm_l12_embedding is None

        hydration_degradations = [
            d for d in ctx.degradations if d.stage == DegradationStage.EMBED_HYDRATION
        ]
        assert len(hydration_degradations) == 1
        assert hydration_degradations[0].cause is failure

        hydration_logs = [
            r
            for r in caplog.records
            if r.name == generate_module.logger.name and r.message.startswith("Embedding hydration")
        ]
        assert len(hydration_logs) == 1
        assert hydration_logs[0].levelno == logging.ERROR
        # An unexpected failure keeps its traceback.
        assert hydration_logs[0].message == "Embedding hydration failed; continuing without"
        assert hydration_logs[0].exc_info is not None

    @pytest.mark.asyncio
    async def test_es_error_without_pipeline_context_still_returns_candidates(self, monkeypatch):
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})

        async def _raises(*args, **kwargs):
            raise RuntimeError("Elasticsearch failed")

        monkeypatch.setattr(generate_module, "fetch_post_embeddings", _raises)

        # No PipelineContext: generator failures would hard-fail here, but a
        # hydration failure is never fatal — the slate is served unhydrated.
        result = await run_generate(_hydrate_request("gen"), es=object())

        assert [c.at_uri for c in result.candidates] == ["at://a"]
        assert result.candidates[0].minilm_l12_embedding is None

    @pytest.mark.asyncio
    async def test_hydration_failure_propagates_under_fail_fast(self, monkeypatch):
        _stub_generators(monkeypatch, {"gen": _FixedGenerator("gen", [_candidate("at://a")])})

        async def _raises(*args, **kwargs):
            raise RuntimeError("Elasticsearch failed")

        monkeypatch.setattr(generate_module, "fetch_post_embeddings", _raises)

        # fail_fast surfaces the cause out of ctx.record, so hydration is not
        # silently degraded in that (diagnostic) mode.
        ctx = PipelineContext(feed_name="test-feed", fail_fast=True)
        with pipeline_context_scope(ctx):
            with pytest.raises(RuntimeError, match="Elasticsearch failed"):
                await run_generate(_hydrate_request("gen"), es=object())

        assert len(ctx.degradations) == 1
        assert ctx.degradations[0].stage == DegradationStage.EMBED_HYDRATION

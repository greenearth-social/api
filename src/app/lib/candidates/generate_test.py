"""Tests for per-generator timeout/cancellation and failure metrics in run_generate."""

import asyncio
import logging
from typing import cast

import pytest

from ...models import CandidateGenerateRequest, CandidatePost, GeneratorSpec
from ..candidates import generate as generate_module
from ..candidates.base import CandidateGenerator, CandidateResult
from ..candidates.generate import GeneratorError, run_generate
from ..feed_debug import FeedDebugRecorder, feed_debug_scope
from ..metrics import MetricCollector, set_metric_collector


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _HangingGenerator(CandidateGenerator):
    def __init__(self, name: str = "hanging"):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(self, es, user_did, num_candidates=100, video_only=False, exclude_uris=None):
        await asyncio.sleep(9999)
        raise AssertionError("unreachable")


class _FailingGenerator(CandidateGenerator):
    def __init__(self, exc: Exception, name: str = "failing"):
        self._exc = exc
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(self, es, user_did, num_candidates=100, video_only=False, exclude_uris=None):
        raise self._exc


class _EmptyGenerator(CandidateGenerator):
    def __init__(self, name: str = "empty"):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def generate(self, es, user_did, num_candidates=100, video_only=False, exclude_uris=None):
        return CandidateResult(generator_name=self.name, candidates=[])


class _StaticGenerator(CandidateGenerator):
    def __init__(self, name: str, candidates: list[CandidatePost]):
        self._name = name
        self._candidates = candidates
        self.calls: list[dict] = []

    @property
    def name(self) -> str:
        return self._name

    async def generate(self, es, user_did, num_candidates=100, video_only=False, exclude_uris=None):
        self.calls.append(
            {
                "num_candidates": num_candidates,
                "video_only": video_only,
                "exclude_uris": exclude_uris,
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

    async def generate(self, es, user_did, num_candidates=100, video_only=False, exclude_uris=None):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("primary failed")
        return CandidateResult(generator_name=self.name, candidates=self._candidates[:num_candidates])


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
) -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name=generator_name, weight=1.0)],
        user_did="did:plc:test",
        num_candidates=num_candidates,
        video_only=False,
        infill=infill,
        exclude_uris=exclude_uris or [],
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
# Main generator timeout tests
# ---------------------------------------------------------------------------


class TestGeneratorTimeout:
    @pytest.mark.asyncio
    async def test_timeout_swallow_returns_no_candidates_and_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        result = await run_generate(_make_request("post_similarity"), es=None)

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generate.failure_count"]
        assert len(failure_calls) == 1
        name, value, attrs = failure_calls[0]
        assert value == 1
        assert attrs == {
            "generator_name": "post_similarity",
            "outcome": "timeout",
            "is_infill": "false",
        }

    @pytest.mark.asyncio
    async def test_timeout_swallow_logs_warning_not_exception(self, monkeypatch, caplog):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})

        with caplog.at_level(logging.WARNING):
            await run_generate(_make_request("post_similarity"), es=None)

        timeout_warnings = [
            r for r in caplog.records
            if "timed out" in r.message and r.levelno == logging.WARNING
        ]
        error_logs = [
            r for r in caplog.records
            if r.levelno >= logging.ERROR and "post_similarity" in r.message
        ]
        assert len(timeout_warnings) == 1
        assert len(error_logs) == 0

    @pytest.mark.asyncio
    async def test_timeout_no_swallow_raises_generator_error_promptly(self, monkeypatch):
        monkeypatch.setenv("GE_FAIL_FAST", "true")
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})

        with pytest.raises(GeneratorError) as exc_info:
            await asyncio.wait_for(
                run_generate(_make_request("post_similarity"), es=None),
                timeout=1.0,
            )

        assert exc_info.value.name == "post_similarity"

    @pytest.mark.asyncio
    async def test_timeout_no_swallow_records_metric_before_raising(self, monkeypatch):
        monkeypatch.setenv("GE_FAIL_FAST", "true")
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

        with pytest.raises(GeneratorError):
            await run_generate(_make_request("post_similarity"), es=None)

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
    async def test_swallowed_primary_failure_records_empty_debug_output(self, monkeypatch):
        gen = _FailThenReturnGenerator("popular", [_candidate("at://infill/1", "popular")])
        _stub_generators(monkeypatch, {"popular": gen})
        rec = FeedDebugRecorder(feed_name="f", regenerated=False)

        with feed_debug_scope(rec):
            result = await run_generate(
                _make_request("popular", num_candidates=1, infill="popular"),
                es=None,
            )

        assert [c.at_uri for c in result.candidates] == ["at://infill/1"]
        assert [
            (output.generator_name, [c.at_uri for c in output.candidates])
            for output in rec.generator_outputs
        ] == [
            ("popular", []),
            ("popular", ["at://infill/1"]),
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


# ---------------------------------------------------------------------------
# Infill generator timeout/error tests
# ---------------------------------------------------------------------------


class TestInfillGeneratorTimeout:
    @pytest.mark.asyncio
    async def test_infill_timeout_swallow_returns_empty_and_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

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
    async def test_infill_timeout_no_swallow_raises_generator_error_with_is_infill(self, monkeypatch):
        monkeypatch.setenv("GE_FAIL_FAST", "true")
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })

        with pytest.raises(GeneratorError) as exc_info:
            await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
            )

        assert exc_info.value.name == "popular"
        assert exc_info.value.is_infill is True

    @pytest.mark.asyncio
    async def test_infill_timeout_no_swallow_records_metric(self, monkeypatch):
        monkeypatch.setenv("GE_FAIL_FAST", "true")
        monkeypatch.setattr(generate_module, "_GENERATOR_TIMEOUT_SEC", 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })
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
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _FailingGenerator(RuntimeError("db down"), name="popular"),
        })
        mc = FakeMetricCollector()
        set_metric_collector(cast(MetricCollector, mc))

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
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _EmptyGenerator("popular"),
        })
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
        assert [c.at_uri for c in result.candidates] == [
            "at://primary/1",
            "at://primary/2",
            "at://infill/1",
            "at://infill/2",
        ]

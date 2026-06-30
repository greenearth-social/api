"""Tests for per-generator timeout/cancellation and failure metrics in run_generate."""

import asyncio
import logging

import pytest

from ...models import CandidateGenerateRequest, GeneratorSpec
from ..candidates import generate as generate_module
from ..candidates.base import CandidateGenerator, CandidateResult
from ..candidates.generate import GeneratorError, run_generate
from ..metrics import set_metric_collector


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
) -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name=generator_name)],
        user_did="did:plc:test",
        num_candidates=num_candidates,
        infill=infill,
    )


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
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})
        mc = FakeMetricCollector()
        set_metric_collector(mc)

        result = await run_generate(_make_request("post_similarity"), es=None, swallow_errors=True)

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
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
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})

        with caplog.at_level(logging.WARNING):
            await run_generate(_make_request("post_similarity"), es=None, swallow_errors=True)

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
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})

        with pytest.raises(GeneratorError) as exc_info:
            await asyncio.wait_for(
                run_generate(_make_request("post_similarity"), es=None, swallow_errors=False),
                timeout=1.0,
            )

        assert exc_info.value.name == "post_similarity"

    @pytest.mark.asyncio
    async def test_timeout_no_swallow_records_metric_before_raising(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {"post_similarity": _HangingGenerator("post_similarity")})
        mc = FakeMetricCollector()
        set_metric_collector(mc)

        with pytest.raises(GeneratorError):
            await run_generate(_make_request("post_similarity"), es=None, swallow_errors=False)

        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs["outcome"] == "timeout"
        assert attrs["is_infill"] == "false"

    @pytest.mark.asyncio
    async def test_exception_records_error_outcome_metric(self, monkeypatch):
        gen = _FailingGenerator(ValueError("boom"), name="network_likes")
        _stub_generators(monkeypatch, {"network_likes": gen})
        mc = FakeMetricCollector()
        set_metric_collector(mc)

        result = await run_generate(_make_request("network_likes"), es=None, swallow_errors=True)

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "network_likes",
            "outcome": "error",
            "is_infill": "false",
        }


# ---------------------------------------------------------------------------
# Infill generator timeout/error tests
# ---------------------------------------------------------------------------


class TestInfillGeneratorTimeout:
    @pytest.mark.asyncio
    async def test_infill_timeout_swallow_returns_empty_and_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })
        mc = FakeMetricCollector()
        set_metric_collector(mc)

        result = await run_generate(
            _make_request("random", num_candidates=5, infill="popular"),
            es=None,
            swallow_errors=True,
        )

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "popular",
            "outcome": "timeout",
            "is_infill": "true",
        }

    @pytest.mark.asyncio
    async def test_infill_timeout_no_swallow_raises_generator_error_with_is_infill(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })

        with pytest.raises(GeneratorError) as exc_info:
            await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
                swallow_errors=False,
            )

        assert exc_info.value.name == "popular"
        assert exc_info.value.is_infill is True

    @pytest.mark.asyncio
    async def test_infill_timeout_no_swallow_records_metric(self, monkeypatch):
        monkeypatch.setattr(generate_module, "_generator_timeout_sec", lambda: 0.01)
        _stub_generators(monkeypatch, {
            "random": _EmptyGenerator("random"),
            "popular": _HangingGenerator("popular"),
        })
        mc = FakeMetricCollector()
        set_metric_collector(mc)

        with pytest.raises(GeneratorError):
            await run_generate(
                _make_request("random", num_candidates=5, infill="popular"),
                es=None,
                swallow_errors=False,
            )

        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
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
        set_metric_collector(mc)

        result = await run_generate(
            _make_request("random", num_candidates=5, infill="popular"),
            es=None,
            swallow_errors=True,
        )

        assert result.candidates == []
        failure_calls = [c for c in mc.calls if c[0] == "candidates.generator_failure_count"]
        assert len(failure_calls) == 1
        _, _, attrs = failure_calls[0]
        assert attrs == {
            "generator_name": "popular",
            "outcome": "error",
            "is_infill": "true",
        }

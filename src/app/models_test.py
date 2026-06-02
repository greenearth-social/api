import pytest
from pydantic import ValidationError

from app.models import CandidateGenerateRequest, FeedConfig, GeneratorSpec


def _minimal_gen_request() -> CandidateGenerateRequest:
    return CandidateGenerateRequest.model_construct(
        generators=[GeneratorSpec(name="test", weight=1.0)],
        num_candidates=30,
        video_only=False,
        exclude_uris=[],
        infill=None,
    )


def _minimal_feed_cfg(**kwargs) -> FeedConfig:
    defaults = dict(
        display_name="Test",
        internal_rkey="ab-t",
        internal_display_name="ab T",
        gen_request_template=_minimal_gen_request(),
    )
    return FeedConfig(**{**defaults, **kwargs})


class TestFeedConfig:
    def test_public_defaults_to_false(self):
        assert _minimal_feed_cfg().public is False

    def test_public_can_be_set_true(self):
        assert _minimal_feed_cfg(public=True).public is True

    def test_rejects_display_name_over_19_chars(self):
        with pytest.raises(ValidationError):
            _minimal_feed_cfg(display_name="A" * 20)

    def test_accepts_display_name_of_exactly_19_chars(self):
        assert len(_minimal_feed_cfg(display_name="A" * 19).display_name) == 19

    def test_internal_rkey_is_required(self):
        with pytest.raises(ValidationError):
            _minimal_feed_cfg(internal_rkey=None)

    def test_internal_display_name_is_required(self):
        with pytest.raises(ValidationError):
            _minimal_feed_cfg(internal_display_name=None)

    def test_internal_fields_can_be_set(self):
        cfg = _minimal_feed_cfg(internal_rkey="e2-s", internal_display_name="e2 S")
        assert cfg.internal_rkey == "e2-s"
        assert cfg.internal_display_name == "e2 S"

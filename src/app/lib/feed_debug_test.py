"""Tests for the feed-debug ContextVar recorder."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .candidates.base import CandidateResult
from .feed_debug import (
    CONTENT_SNIPPET_MAX,
    FeedDebugRecorder,
    current_recorder,
    feed_debug_scope,
)
from ..models import (
    CandidateGenerateRequest,
    CandidatePost,
    GeneratorSpec,
    RankedCandidate,
    RankPredictResult,
)


def _request() -> CandidateGenerateRequest:
    return CandidateGenerateRequest(
        generators=[GeneratorSpec(name="post_similarity", weight=1.0)],
        user_did="did:plc:user",
        num_candidates=10,
        infill="popularity",
    )


def _candidate(
    uri: str, *, embedding: str | None = "ZmFrZQ==", content: str = "hi", author: str = "did:plc:a"
) -> CandidatePost:
    return CandidatePost(
        at_uri=uri,
        content=content,
        minilm_l12_embedding=embedding,
        score=1.0,
        generator_name="post_similarity",
        author_did=author,
    )


class TestRecorderScope:
    def test_no_recorder_by_default(self):
        assert current_recorder() is None

    def test_scope_installs_and_clears(self):
        rec = FeedDebugRecorder(feed_name="your-feed", regenerated=False)
        with feed_debug_scope(rec) as scoped:
            assert scoped is rec
            assert current_recorder() is rec
        assert current_recorder() is None


class TestBuildDocument:
    def _recorder(self) -> FeedDebugRecorder:
        rec = FeedDebugRecorder(feed_name="your-feed", regenerated=True)
        rec.ranker_model = "two_tower"
        rec.diversify = True
        rec.set_generate_request(_request())
        rec.record_generator_output(
            CandidateResult(
                generator_name="post_similarity",
                candidates=[_candidate("at://p/1"), _candidate("at://p/2")],
            )
        )
        rec.record_final_candidates([_candidate("at://p/1"), _candidate("at://p/2")])
        rec.record_user_features("two_tower", ["at://like/1", "at://like/2"], 1)
        rec.record_ranking(
            RankPredictResult(
                rankings=[
                    RankedCandidate(at_uri="at://p/1", rank=1, rank_score=0.9),
                    RankedCandidate(at_uri="at://p/2", rank=2, rank_score=0.5),
                ]
            )
        )
        rec.record_order_after_rank(["at://p/1", "at://p/2"])
        rec.record_final_order(["at://p/2", "at://p/1"])
        return rec

    def _build(self, rec: FeedDebugRecorder, **kwargs):
        now = datetime.now(timezone.utc)
        return rec.build_document(
            request_id="req123",
            username="user.bsky.app",
            generated_at=now,
            expires_at=now + timedelta(days=7),
            **kwargs,
        )

    def test_preserves_structure(self):
        doc = self._build(self._recorder())
        assert doc.request_id == "req123"
        assert doc.user_did == "did:plc:user"
        assert doc.feed_name == "your-feed"
        assert doc.regenerated is True
        assert doc.ranker_model == "two_tower"
        assert doc.diversify is True
        assert len(doc.generator_outputs) == 1
        assert doc.generator_outputs[0].generator_name == "post_similarity"
        assert len(doc.final_candidates) == 2
        assert doc.ranking is not None and len(doc.ranking.rankings) == 2
        assert doc.order_after_rank == ["at://p/1", "at://p/2"]
        assert doc.final_order == ["at://p/2", "at://p/1"]
        assert doc.user_features[0].source == "two_tower"
        assert doc.user_features[0].num_embeddings == 1

    def test_strips_embeddings(self):
        doc = self._build(self._recorder())
        assert all(c.minilm_l12_embedding is None for c in doc.final_candidates)
        for result in doc.generator_outputs:
            assert all(c.minilm_l12_embedding is None for c in result.candidates)

    def test_truncates_content(self):
        rec = FeedDebugRecorder(feed_name="f", regenerated=False)
        rec.set_generate_request(_request())
        long = "x" * (CONTENT_SNIPPET_MAX + 50)
        rec.record_final_candidates([_candidate("at://p/1", content=long)])
        doc = self._build(rec)
        assert len(doc.final_candidates[0].content) == CONTENT_SNIPPET_MAX

    def test_stamps_author_usernames(self):
        doc = self._build(
            self._recorder(),
            author_usernames={"did:plc:a": "alice.bsky.app"},
        )
        assert all(c.author_username == "alice.bsky.app" for c in doc.final_candidates)

    def test_author_dids_union(self):
        rec = FeedDebugRecorder(feed_name="f", regenerated=False)
        rec.set_generate_request(_request())
        rec.record_generator_output(
            CandidateResult(
                generator_name="post_similarity",
                candidates=[_candidate("at://p/1", author="did:plc:a")],
            )
        )
        rec.record_final_candidates([_candidate("at://p/2", author="did:plc:b")])
        assert rec.author_dids() == {"did:plc:a", "did:plc:b"}

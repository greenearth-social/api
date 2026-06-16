"""Tests for the Perspective rank model."""

import asyncio
from unittest.mock import AsyncMock, patch

from ...models import CandidatePost
from .perspective import PerspectiveRanker


def _make_candidate(uri: str | None, content: str | None = "text") -> CandidatePost:
    return CandidatePost(
        at_uri=uri,
        content=content,
        score=1.0,
        minilm_l12_embedding=None,
        generator_name="test",
    )


class TestPerspectiveRanker:
    def test_name(self):
        assert PerspectiveRanker().name == "perspective"

    def test_score_bounds_match_weighted_prc_formula(self):
        # _PRC_WEIGHTS' positive weights sum to 1.0 (6 * 1/6) and negative
        # weights sum to -1.0 (2*(-1/6) + 3*(-1/18) + 4*(-1/8)) — see
        # lib/perspective.py — so the theoretical bounds of the weighted-sum
        # PRC score are exactly (-1.0, 1.0).
        assert PerspectiveRanker().score_bounds == (-1.0, 1.0)

    def test_predict_orders_by_prc_score_descending(self):
        candidates = [
            _make_candidate("at://a/1", content="low quality"),
            _make_candidate("at://a/2", content="medium quality"),
            _make_candidate("at://a/3", content="high quality"),
        ]
        scores = {"at://a/1": 0.1, "at://a/2": 0.5, "at://a/3": 0.9}

        with patch(
            "app.lib.rankers.perspective.score_candidates",
            new_callable=AsyncMock,
            return_value=scores,
        ):
            result = asyncio.run(PerspectiveRanker().predict(es=object(), user_did="did:plc:user1", candidates=candidates))

        rankings = result.result.rankings
        assert [r.at_uri for r in rankings] == ["at://a/3", "at://a/2", "at://a/1"]
        assert [r.rank for r in rankings] == [1, 2, 3]
        assert [r.rank_score for r in rankings] == [0.9, 0.5, 0.1]

    def test_predict_reports_raw_prc_scores_not_normalized(self):
        """`predict` returns raw PRC scores; normalization into [-1, 1] using
        `score_bounds` is `run_predict`'s responsibility, not the ranker's."""
        candidates = [_make_candidate("at://a/1")]
        with patch(
            "app.lib.rankers.perspective.score_candidates",
            new_callable=AsyncMock,
            return_value={"at://a/1": 0.42},
        ):
            result = asyncio.run(PerspectiveRanker().predict(es=object(), user_did="did:plc:user1", candidates=candidates))

        assert result.result.rankings[0].rank_score == 0.42

    def test_predict_skips_candidates_without_at_uri(self):
        candidates = [
            _make_candidate(None, content="no uri"),
            _make_candidate("at://a/1", content="has uri"),
        ]
        with patch(
            "app.lib.rankers.perspective.score_candidates",
            new_callable=AsyncMock,
            return_value={"at://a/1": 0.5},
        ) as mock_score:
            result = asyncio.run(PerspectiveRanker().predict(es=object(), user_did="did:plc:user1", candidates=candidates))

        # score_candidates is only called with candidates that have an at_uri
        (scored_candidates,), _ = mock_score.call_args
        assert [c.at_uri for c in scored_candidates] == ["at://a/1"]
        assert [r.at_uri for r in result.result.rankings] == ["at://a/1"]

    def test_predict_uses_missing_score_for_unscored_candidates(self):
        candidates = [
            _make_candidate("at://a/1", content="scored"),
            _make_candidate("at://a/2", content="unscored"),
        ]
        with patch(
            "app.lib.rankers.perspective.score_candidates",
            new_callable=AsyncMock,
            return_value={"at://a/1": 0.5},  # at://a/2 missing -> None
        ):
            result = asyncio.run(PerspectiveRanker().predict(es=object(), user_did="did:plc:user1", candidates=candidates))

        by_uri = {r.at_uri: r.rank_score for r in result.result.rankings}
        assert by_uri == {"at://a/1": 0.5, "at://a/2": None}

    def test_predict_orders_real_scores_descending_with_missing_last(self):
        candidates = [
            _make_candidate("at://a/missing", content="missing"),
            _make_candidate("at://a/negative", content="negative"),
            _make_candidate("at://a/zero", content="zero"),
            _make_candidate("at://a/positive", content="positive"),
        ]
        with patch(
            "app.lib.rankers.perspective.score_candidates",
            new_callable=AsyncMock,
            return_value={
                "at://a/missing": None,
                "at://a/negative": -0.2,
                "at://a/zero": 0.0,
                "at://a/positive": 0.7,
            },
        ):
            result = asyncio.run(PerspectiveRanker().predict(es=object(), user_did="did:plc:user1", candidates=candidates))

        assert [(r.at_uri, r.rank, r.rank_score) for r in result.result.rankings] == [
            ("at://a/positive", 1, 0.7),
            ("at://a/zero", 2, 0.0),
            ("at://a/negative", 3, -0.2),
            ("at://a/missing", 4, None),
        ]

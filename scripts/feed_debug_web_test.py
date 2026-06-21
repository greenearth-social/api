from __future__ import annotations

from datetime import UTC, datetime, timedelta

import feed_debug as cli
import feed_debug_web as web

from app.documents import (
    FeedDebugDiversificationEntry,
    FeedDebugDocument,
    FeedDebugModelScoreEntry,
    FeedDebugScoreEntry,
)
from app.lib.candidates.base import CandidateResult
from app.models import (
    CandidateGenerateRequest,
    CandidatePost,
    GeneratorSpec,
    RankedCandidate,
    RankPredictResult,
)

USER_DID = "did:plc:testuser"
USERNAME = "testuser.bsky.social"


def _candidate() -> CandidatePost:
    return CandidatePost(
        at_uri="at://did:plc:author/app.bsky.feed.post/abc",
        content="Solar farms are looking especially good today.",
        score=0.71234,
        author_did="did:plc:author",
        author_username="alice.bsky.social",
        image_count=2,
        contains_images=True,
        external_uri="https://example.com",
    )


def _doc(feed_name: str = web.TARGET_FEED_NAME) -> FeedDebugDocument:
    now = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)
    candidate = _candidate()
    return FeedDebugDocument(
        request_id="req-abc123",
        user_did=USER_DID,
        username=USERNAME,
        feed_name=feed_name,
        generate_request=CandidateGenerateRequest(
            generators=[
                GeneratorSpec(name="two_tower", weight=0.5),
                GeneratorSpec(name="followed_users", weight=0.5),
            ],
            user_did=USER_DID,
            num_candidates=25,
            video_only=False,
            infill="popularity",
        ),
        ranker_model="two_tower",
        diversify=True,
        generator_outputs=[
            CandidateResult(generator_name="two_tower", candidates=[candidate]),
        ],
        final_candidates=[candidate],
        ranking=RankPredictResult(
            rankings=[
                RankedCandidate(at_uri=candidate.at_uri or "", rank=3, rank_score=0.66),
            ]
        ),
        model_scores=[
            FeedDebugModelScoreEntry(
                model_name="two_tower",
                weight=1.0,
                scores=[
                    FeedDebugScoreEntry(at_uri=candidate.at_uri or "", score=0.75),
                ],
            ),
            FeedDebugModelScoreEntry(
                model_name="perspective",
                weight=1.0,
                scores=[
                    FeedDebugScoreEntry(at_uri=candidate.at_uri or "", score=0.57),
                ],
            )
        ],
        order_after_rank=[candidate.at_uri or ""],
        final_order=[candidate.at_uri or ""],
        diversification=[
            FeedDebugDiversificationEntry(
                at_uri=candidate.at_uri or "",
                relevance=0.9,
                score=0.7,
                author_penalty=0.1,
                content_penalty=0.2,
            )
        ],
        generated_at=now,
        expires_at=now + timedelta(days=7),
    )


def test_render_debug_doc_includes_summary_and_feed_card():
    html = web._render_page(
        "testuser.bsky.social",
        "stage",
        web._render_debug_doc(_doc(), debug_enabled=True),
    )

    assert "Feed Debug Viewer" in html
    assert "Candidate generators" in html
    assert "your-feed only" in html
    assert "req-abc123" in html
    assert "two_tower(1)" in html
    assert "Final Feed" in html
    assert "#1" in html
    assert "@alice.bsky.social" in html
    assert "Solar farms are looking especially good today." in html
    assert "two_tower" in html
    assert "followed_users" in html
    assert "gen-green" in html
    assert "0.71" in html
    assert "#3 model 0.66" in html
    assert "post-card-layout" in html
    assert "post-card-layout-simple" in html
    assert '<section class="rank-visual">' not in html
    assert "<span>Score breakdown</span>" not in html
    assert "debug-group-rank" in html
    assert "debug-group-diversity" in html
    assert "<h3>Ranking</h3>" in html
    assert "<h3>Diversity</h3>" in html
    assert "div rel</span><strong>1.00" in html
    assert "div score" in html
    assert (
        'href="https://bsky.app/profile/did:plc:author/post/abc" '
        'target="_blank" rel="noopener noreferrer"'
    ) in html
    assert "Open in Bluesky" in html


def test_no_records_message_is_presentable():
    result = web.FeedDebugLookup(
        status="no_records",
        query_user="testuser.bsky.social",
        user_did=USER_DID,
        debug_enabled=False,
    )

    html = web._render_lookup_result(result)

    assert "No your-feed feed-debug information found for this user." in html
    assert "feed debugging has not been enabled" in html
    assert "empty-state" in html


def test_disabled_debug_note_still_renders_existing_record():
    html = web._render_debug_doc(_doc(), debug_enabled=False)

    assert "Feed debugging is currently off for this user." in html
    assert "Showing the newest saved record" in html
    assert "your-feed only" in html


def test_environment_switch_uses_checked_radio_state():
    html = web._render_page("testuser.bsky.social", "prod", "")

    assert ".segmented input:checked + span" in html
    assert 'value="prod" checked' in html
    assert '<span>prod</span>' in html


def test_score_and_media_formatting():
    candidate = CandidatePost(
        at_uri="at://p/1",
        contains_images=True,
        video_count=1,
        external_uri="https://example.com",
    )

    assert web._fmt_score(0.123456) == "0.12"
    assert web._fmt_score(None) == "--"
    assert web._media_labels(candidate) == ["image", "1 video", "link"]


def test_at_uri_to_bsky_url_matches_tool_logic():
    assert (
        web._at_uri_to_bsky_url("at://did:plc:xyz123/app.bsky.feed.post/abc123")
        == "https://bsky.app/profile/did:plc:xyz123/post/abc123"
    )
    assert web._at_uri_to_bsky_url("at://did:plc:xyz123/app.bsky.feed.like/abc123") is None


def test_diversification_relevance_contribution_makes_score_add_up():
    div = _doc().diversification[0]
    view = web.DiversificationView(
        relevance=div.relevance,
        score=div.score,
        author_penalty=div.author_penalty,
        content_penalty=div.content_penalty,
    )

    assert web._diversification_relevance_contribution(view) == 1.0
    assert cli._diversification_relevance_contribution(div) == 1.0


def test_score_breakdown_renders_for_non_first_post_with_half_scale():
    item = web.ItemView(
        at_uri="at://did:plc:author/app.bsky.feed.post/abc",
        post_url="https://bsky.app/profile/did:plc:author/post/abc",
        final_position=2,
        author="@alice.bsky.social",
        content="Solar farms are looking especially good today.",
        media_labels=[],
        generators=[],
        rank_position=3,
        rank_score=0.66,
        after_rank_position=3,
        model_scores=[
            web.ModelScoreView("two_tower", 1.0, 0.75),
            web.ModelScoreView("perspective", 1.0, 0.57),
        ],
        diversification=web.DiversificationView(
            relevance=0.9,
            score=0.34,
            author_penalty=0.1,
            content_penalty=0.05,
        ),
    )

    html = web._render_rank_visual(item)

    assert "rank-visual" in html
    assert "<span>Score breakdown</span>" in html
    assert "<strong>0.34</strong>" in html
    assert 'height: 55.68%; bottom: 0.00%;' in html
    assert 'height: 42.32%; bottom: 55.68%;' in html
    assert 'data-scaled-contribution="0.278409"' in html
    assert 'data-scaled-contribution="0.211591"' in html
    assert 'data-div-score="0.340000"' in html
    assert 'style="height: 68.00%; bottom: 0;"' in html
    assert "score 0.75 -> 0.28" in html
    assert "score 0.57 -> 0.21" in html


def test_rank_contributions_scale_to_diversity_relevance():
    contributions = web._rank_contributions(
        [
            web.ModelScoreView("two_tower", 1.0, 0.75),
            web.ModelScoreView("perspective", 1.0, 0.57),
        ],
        rank_total=0.66,
        target_total=0.49,
    )

    assert round(contributions[0].scaled_contribution, 6) == 0.278409
    assert round(contributions[1].scaled_contribution, 6) == 0.211591
    assert round(contributions[0].height_pct, 2) == 55.68
    assert round(contributions[1].height_pct, 2) == 42.32
    assert round(contributions[1].bottom_pct, 2) == 55.68


def test_latest_target_feed_debug_filters_to_your_feed():
    older_target = _doc(feed_name=web.TARGET_FEED_NAME)
    newer_other = _doc(feed_name="unranked-your-feed")

    assert web._latest_target_feed_debug([newer_other, older_target]) is older_target
    assert web._latest_target_feed_debug([newer_other]) is None

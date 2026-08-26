# pyright: reportCallIssue=false
"""Feed catalog — the canonical registry of all published feeds.

Each entry maps a short feed name (the AT Protocol rkey) to a ``FeedConfig``
that holds display metadata **and** the generator/ranker pipeline templates.
Templates are built with ``model_construct`` so that session-specific required
fields (``user_did``, ``candidates``) can be omitted; the XRPC router fills
them in at request time via ``model_copy``.

This module is intentionally separate from the router so that other parts of
the codebase (e.g.  the ``publish_feed.py`` script) can import it without
pulling in FastAPI.
"""

import os

from .models import (
    CandidateGenerateRequest,
    FeedConfig,
    GeneratorSpec,
    RankModelSpec,
    RankPredictRequest,
)

DEFAULT_SOCIAL_RADIUS: int = 3

# The post served, on its own, to logged-out viewers of feeds that need a
# signed-in user to mean anything (``logged_out="explain"``). A feed with no
# items reads as a broken feed, so show something that says why (issue #384).
# The GreenEarth account's "This feed is personalized for you, so you must be
# logged in to see it."
LOGGED_OUT_POST_URI: str = "at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3msw6wzvh7k2k"


def _pinned_post_uri(feed_name: str, fallback: str) -> str:
    """Resolve a deployment-managed pin while retaining a local/dev fallback."""
    env_name = f"GE_PINNED_POST_{feed_name.upper().replace('-', '_')}_URI"
    configured = os.environ.get(env_name, "").strip()
    return configured or fallback


def _survey_post_uri(feed_name: str, fallback: str) -> str:
    """Resolve the survey post URI for a feed from the environment, or fall back to the hardcoded URI."""
    env_name = f"GE_SURVEY_POST_{feed_name.upper().replace('-', '_')}_URI"
    configured = os.environ.get(env_name, "").strip()
    return configured or fallback


# Social-radius preset generator weights for your-feed.
# Index 3 (balanced) matches the default weights defined in the "your-feed"
# FeedConfig below — keep them in sync when tuning.
SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES: dict[int, list[GeneratorSpec]] = {
    0: [  # Friends — only from people you follow
        GeneratorSpec(name="followed_users", weight=1.00),
    ],
    1: [  # Closer
        GeneratorSpec(name="followed_users", weight=0.70),
        GeneratorSpec(name="two_tower", weight=0.10),
        GeneratorSpec(name="popularity", weight=0.10),
        GeneratorSpec(name="network_likes", weight=0.10),
    ],
    2: [
        GeneratorSpec(name="followed_users", weight=0.50),
        GeneratorSpec(name="two_tower", weight=0.15),
        GeneratorSpec(name="popularity", weight=0.15),
        GeneratorSpec(name="network_likes", weight=0.20),
    ],
    3: [  # Balanced — same as your-feed defaults
        GeneratorSpec(name="followed_users", weight=0.30),
        GeneratorSpec(name="two_tower", weight=0.25),
        GeneratorSpec(name="popularity", weight=0.25),
        GeneratorSpec(name="network_likes", weight=0.20),
    ],
    4: [  # Everyone — mostly discovery
        GeneratorSpec(name="followed_users", weight=0.10),
        GeneratorSpec(name="two_tower", weight=0.40),
        GeneratorSpec(name="popularity", weight=0.40),
        GeneratorSpec(name="network_likes", weight=0.10),
    ],
}

SOCIAL_RADIUS_PRESETS_NO_NETWORK_LIKES: dict[int, list[GeneratorSpec]] = {
    0: [  # Friends — only from people you follow
        GeneratorSpec(name="followed_users", weight=1.00),
    ],
    1: [  # Closer
        GeneratorSpec(name="followed_users", weight=0.80),
        GeneratorSpec(name="two_tower", weight=0.10),
        GeneratorSpec(name="popularity", weight=0.10),
    ],
    2: [
        GeneratorSpec(name="followed_users", weight=0.60),
        GeneratorSpec(name="two_tower", weight=0.20),
        GeneratorSpec(name="popularity", weight=0.20),
    ],
    3: [  # Balanced — same as your-feed defaults
        GeneratorSpec(name="followed_users", weight=0.40),
        GeneratorSpec(name="two_tower", weight=0.30),
        GeneratorSpec(name="popularity", weight=0.30),
    ],
    4: [  # Everyone — mostly discovery
        GeneratorSpec(name="followed_users", weight=0.20),
        GeneratorSpec(name="two_tower", weight=0.40),
        GeneratorSpec(name="popularity", weight=0.40),
    ],
}

# NOTE: published display names are limited to 24 graphemes. Internal ("debug")
# feeds are published as "GE <internal_display_name> <git_sha>" (see issue #228),
# so keep internal_display_name to 13 chars or fewer. feeds_test.py enforces this.
#
# NOTE: every private (development) feed sets logged_out="deny" — a feed nobody
# is meant to see has nothing to say to a logged-out visitor. Public feeds take
# the "explain" default, or "serve" when they work without a user. feeds_test.py
# enforces this too.
FEEDS: dict[str, FeedConfig] = {
    "unranked-your-feed": FeedConfig(
        display_name="Unranked YF",
        description="Development feed — same as mysky but without ranking.",
        internal_rkey="e2-s",
        internal_display_name="e2 S",
        logged_out="deny",
        avatar="assets/icons/unranked-your-feed.png",
        preference_source="your-feed",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES.get(DEFAULT_SOCIAL_RADIUS),
            infill="popularity",
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "random": FeedConfig(
        display_name="Random",
        description="A random selection of recent posts from the community.",
        public=True,
        internal_rkey="67-r",
        internal_display_name="67 R",
        avatar="assets/icons/random.png",
        controls=("freshness",),
        diversify=False,
        exclude_seen_posts=False,
        pinned_post_uri=_pinned_post_uri(
            "random",
            "at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3msetia5l7y2j",
        ),
        pinned_post_content=(
            "Click [SETTINGS](https://app.greenearth.social/#/settings/random) to personalize "
            "your feed.\n\nA random slice of the ATProto universe. Still applies your moderation "
            "settings. Part of the GreenEarth Family."
        ),
        # Random posts don't depend on who's asking, so it works logged out.
        logged_out="serve",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="random_posts", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "your-feed": FeedConfig(
        display_name="MySky by GreenEarth",
        description="A controllable feed designed for constructive conversation.",
        public=True,
        internal_rkey="a0-yf",
        internal_display_name="a0 YF",
        avatar="assets/icons/green-earth.png",
        controls=("source_weights", "freshness", "purpose"),
        pinned_post_uri=_pinned_post_uri(
            "your-feed",
            "at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3msetfgpr3t2s",
        ),
        pinned_post_content=(
            "Click [SETTINGS](https://app.greenearth.social/#/settings/your-feed) to personalize "
            "your MySky feed.\n\nA controllable feed designed for constructive conversation."
        ),
        survey_post_uri=_survey_post_uri(
            "your-feed",
            "at://did:plc:66mudnfk2p4olwpaskmrw2vq/app.bsky.feed.post/3mtxartxzwx2s",
        ),
        survey_post_content=(
            "🦋 Enjoying MySky? Or not? 🦋\n"
            "[Sign up here](https://calendly.com/jonathanstray/bluesky-algorithms-talk) "
            "for a paid user interview, $15 to help us build the open social web."
        ),
        # Slate-cutoff starting points — tune further from the feed.slate.kept_share
        # and feed.slate.cutoff_count metrics once live (see issue #248).
        # min_rank_score=0.425 maps the old -0.15 floor into the current [0, 1]
        # combined rank-score range. That value was calibrated from stage
        # feed_debug records for this feed (242 ranked candidates across 4 real
        # loads), where it sat at ~p12-13 and trimmed only the clear tail,
        # leaving max_render_share as the dominant lever on render volume.
        # See "cutoff-preview" below for the live preview of this feed's pipeline
        # with the same thresholds.
        max_render_share=0.5,
        min_rank_score=0.425,
        min_mmr_score=-0.05,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES.get(DEFAULT_SOCIAL_RADIUS),
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            models=[
                RankModelSpec(name="heavy_ranker", weight=1.0),
                RankModelSpec(name="perspective", weight=1.0),
            ],
        ),
    ),
    "best-of-friends": FeedConfig(
        display_name="Best of Friends",
        description="The best posts from people you follow, curated just for you.",
        public=True,
        internal_rkey="fd-bof",
        internal_display_name="fd BOF",
        avatar="assets/icons/best-of-friends.png",
        controls=("freshness", "purpose"),
        pinned_post_uri=_pinned_post_uri(
            "best-of-friends",
            "at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3msetho32pa2g",
        ),
        pinned_post_content=(
            "Click [SETTINGS](https://app.greenearth.social/#/settings/best-of-friends) to "
            "personalize your feed.\n\nThe best posts from your mutuals and people you follow. "
            "Part of the GreenEarth Family."
        ),
        # Slate-cutoff starting points — tune from the feed.slate.kept_share and
        # feed.slate.cutoff_count metrics once live (see issue #248). min_rank_score
        # matches your-feed's empirically-calibrated value above; this feed's own
        # score distribution (followed_users-only, no two_tower/popularity mix)
        # wasn't separately sampled, so treat it as a starting point to revisit
        # once its own metrics are live.
        max_render_share=0.5,
        min_rank_score=0.425,
        min_mmr_score=-0.05,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="followed_users", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            models=[
                RankModelSpec(name="heavy_ranker", weight=1.0),
                RankModelSpec(name="perspective", weight=1.0),
            ],
        ),
    ),
    "cutoff-preview": FeedConfig(
        display_name="Cutoff Preview",
        description="Development feed — your-feed's ranked pipeline with slate-cutoff "
        "limits enabled, for observing and tuning thresholds (see issue #248).",
        internal_rkey="qr-cp",
        internal_display_name="qr CP",
        logged_out="deny",
        preference_source="your-feed",
        # Same generator mix as your-feed, so cutoff behavior here previews what
        # real users would see.
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=SOCIAL_RADIUS_PRESETS_WITH_NETWORK_LIKES.get(DEFAULT_SOCIAL_RADIUS),
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            models=[
                RankModelSpec(name="heavy_ranker", weight=1.0),
                RankModelSpec(name="perspective", weight=1.0),
            ],
        ),
        # Values below are calibrated from real combined rank_score and MMR
        # pick_score distributions pulled from stage feed_debug records (242
        # ranked candidates across 4 real "your-feed" loads from 2 debug-enabled
        # stage users, 2026-07-14 to 2026-07-21). Those records used the old
        # [-1, 1] combined rank-score range, so the rank-score numbers below
        # are mapped into the current [0, 1] range.
        #
        # Empirically the old combined score skewed below the old midpoint
        # (old p10=-0.21, p25=-0.07, p50=+0.07; current p10=0.395,
        # p25=0.465, p50=0.535). A floor at the current midpoint (0.5) would
        # cut roughly half of all candidates by itself, before MMR or the share
        # cap get a say. min_rank_score=0.425 is the old -0.15 floor mapped via
        # (score + 1) / 2; it sits at ~p12-13, trimming only the clearly-bad
        # tail and leaving max_render_share as the dominant lever on render
        # volume, matching the issue's 10-50% band.
        #
        # MMR pick_score is a post-diversification penalized score, not a rank
        # score, so it can still be negative. The old observed p10=+0.01 and
        # minimums (-0.52, -0.08, -0.08, -0.06) show -0.05 already sits below
        # almost all real picks; keep it as the starting point until fresh
        # post-migration debug records are available.
        max_render_share=0.5,
        min_rank_score=0.425,
        min_mmr_score=-0.05,
    ),
    ### (Private) Pure Candidate Generator Feeds, mostly for testing and debugging ###
    "followed-users": FeedConfig(
        display_name="Followed Users",
        description="Development feed — followed-users candidates only.",
        internal_rkey="ij-fu",
        internal_display_name="ij FU",
        logged_out="deny",
        avatar="assets/icons/followed-users.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="followed_users", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "network-likes": FeedConfig(
        display_name="Network Likes",
        description="Development feed — network-likes candidates only.",
        internal_rkey="kl-nl",
        internal_display_name="kl NL",
        logged_out="deny",
        avatar="assets/icons/network-likes.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="network_likes", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "popularity": FeedConfig(
        display_name="Popularity",
        description="Development feed — popularity candidates only.",
        internal_rkey="mn-p",
        internal_display_name="mn P",
        logged_out="deny",
        avatar="assets/icons/popularity.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="popularity", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "two-tower": FeedConfig(
        display_name="Two Tower",
        description="Development feed — two-tower candidates only.",
        internal_rkey="op-tt",
        internal_display_name="op TT",
        logged_out="deny",
        avatar="assets/icons/two-tower.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="two_tower", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "two-tower-empty-history": FeedConfig(
        display_name="TwoTower Cold Start",
        description="Development feed — two-tower candidates for a user with no like history.",
        internal_rkey="tt-eh",
        internal_display_name="tt EH",
        logged_out="deny",
        avatar="assets/icons/two-tower.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="two_tower_empty_history", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "cold-start": FeedConfig(
        display_name="Cold Start",
        description=(
            "Main MySky feed by GreenEarth for a user with no like history and no followed accounts."
        ),
        public=False,
        internal_rkey="mf-cs",
        internal_display_name="mf CS",
        logged_out="deny",
        avatar="assets/icons/green-earth.png",
        max_render_share=0.5,
        min_rank_score=0.425,
        min_mmr_score=-0.05,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="popularity", weight=1.0),
            ],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            models=[
                RankModelSpec(name="heavy_ranker_empty_history", weight=1.0),
                RankModelSpec(name="perspective", weight=1.0),
            ],
        ),
    ),
}


def canonical_feed_name(feed_identifier: str) -> str | None:
    """Resolve a configured feed name from its canonical or published rkey."""
    if feed_identifier in FEEDS:
        return feed_identifier
    return next(
        (
            feed_name
            for feed_name, config in FEEDS.items()
            if config.internal_rkey == feed_identifier
        ),
        None,
    )

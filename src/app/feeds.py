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

from .models import (
    CandidateGenerateRequest,
    FeedConfig,
    GeneratorSpec,
    RankModelSpec,
    RankPredictRequest,
)

# Social-radius preset generator weights for your-feed.
# Index 3 (balanced) matches the default weights defined in the "your-feed"
# FeedConfig below — keep them in sync when tuning.
SOCIAL_RADIUS_PRESETS: dict[int, list[GeneratorSpec]] = {
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

# NOTE: display_name is limited to 24 chars, including the prefix ("GreenEarth, GE Dev, or GE Stg")
FEEDS: dict[str, FeedConfig] = {
    "unranked-your-feed": FeedConfig(
        display_name="Unranked YF",
        description="Development feed — same as green-earth but without ranking.",
        internal_rkey="e2-s",
        internal_display_name="e2 S",
        avatar="assets/icons/unranked-your-feed.png",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="two_tower", weight=0.35),
                GeneratorSpec(name="followed_users", weight=0.35),
                GeneratorSpec(name="popularity", weight=0.3),
            ],
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
        diversify=False,
        exclude_seen_posts=False,
        pinned_post_uri="at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3mq5uvuzydy2o",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="random_posts", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "your-feed": FeedConfig(
        display_name="GreenEarth",
        description="Posts ranked and personalized just for you.",
        public=True,
        internal_rkey="a0-yf",
        internal_display_name="a0 YF",
        avatar="assets/icons/green-earth.png",
        pinned_post_uri="at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3mrash36z5b2c",
        # Slate-cutoff starting points — tune further from the feed.slate.kept_share
        # and feed.slate.cutoff_count metrics once live (see issue #248).
        # min_rank_score=-0.15 is calibrated from real combined rank_score
        # distributions pulled from stage feed_debug records for this feed (242
        # ranked candidates across 4 real loads): the theoretical [-1, 1] midpoint
        # (0.0) sits around the empirical p40-50, so it would cut roughly half of
        # all candidates by itself; -0.15 sits at ~p12-13, trimming only the clear
        # tail and leaving max_render_share as the dominant lever on render volume.
        # See "cutoff-preview" below for the live preview of this feed's pipeline
        # with the same thresholds.
        max_render_share=0.5,
        min_rank_score=0.4,
        min_mmr_score=-0.05,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="followed_users", weight=0.40),
                GeneratorSpec(name="two_tower", weight=0.30),
                GeneratorSpec(name="popularity", weight=0.30),
            ],
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
        pinned_post_uri="at://did:plc:wrmpulygwvuhjn2c3jbalgqj/app.bsky.feed.post/3mq5uvi4exl2s",
        # Slate-cutoff starting points — tune from the feed.slate.kept_share and
        # feed.slate.cutoff_count metrics once live (see issue #248). min_rank_score
        # matches your-feed's empirically-calibrated value above; this feed's own
        # score distribution (followed_users-only, no two_tower/popularity mix)
        # wasn't separately sampled, so treat it as a starting point to revisit
        # once its own metrics are live.
        max_render_share=0.5,
        min_rank_score=0.4,
        min_rank_score=0.5,
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
        # Same generator mix as your-feed, so cutoff behavior here previews what
        # real users would see.
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="followed_users", weight=0.40),
                GeneratorSpec(name="two_tower", weight=0.30),
                GeneratorSpec(name="popularity", weight=0.30),
            ],
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
        # stage users, 2026-07-14 to 2026-07-21), not the theoretical [-1, 1]
        # midpoint used as the initial guess for your-feed/best-of-friends.
        #
        # Empirically the combined score skews well below 0 (p10=-0.21,
        # p25=-0.07, p50=+0.07) — a floor at the theoretical midpoint (0.0)
        # would cut roughly half of all candidates by itself, before MMR or the
        # share cap get a say. min_rank_score=-0.15 sits at ~p12-13, trimming
        # only the clearly-bad tail and leaving max_render_share as the
        # dominant lever on render volume, matching the issue's 10-50% band.
        #
        # MMR pick_score p10=+0.01, and the observed minimums (-0.52, -0.08,
        # -0.08, -0.06) show -0.05 already sits below almost all real picks —
        # it only fires on genuine outliers, so it's left at the same starting
        # value used for your-feed/best-of-friends.
        max_render_share=0.5,
        min_rank_score=-0.15,
        min_mmr_score=-0.05,
    ),

    ### (Private) Pure Candidate Generator Feeds, mostly for testing and debugging ###
    "post-similarity": FeedConfig(
        display_name="Post Similarity",
        description="Development feed — post-similarity candidates only.",
        internal_rkey="gh-ps",
        internal_display_name="gh PS",
        avatar="assets/icons/post-similarity.png",
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="post_similarity", weight=1.0),
            ],
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "followed-users": FeedConfig(
        display_name="Followed Users",
        description="Development feed — followed-users candidates only.",
        internal_rkey="ij-fu",
        internal_display_name="ij FU",
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
}

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

# NOTE: display_name is limited to 24 chars, including the prefix ("GreenEarth, GE Dev, or GE Stg")
FEEDS: dict[str, FeedConfig] = {
    "unranked-your-feed": FeedConfig(
        display_name="Unranked YF",
        description="Development feed — post-similarity and followed-users candidate with popularity infill. No ranking.",
        internal_rkey="e2-s",
        internal_display_name="e2 S",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="two_tower", weight=0.5),
                GeneratorSpec(name="followed_users", weight=0.5),
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
        diversify=False,
        exclude_seen_posts=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="random_posts", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "your-feed": FeedConfig(
        display_name="Your Feed",
        description="Posts ranked and personalized just for you.",
        public=True,
        internal_rkey="a0-yf",
        internal_display_name="a0 YF",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="two_tower", weight=0.5),
                GeneratorSpec(name="followed_users", weight=0.5),
            ],
            infill="popularity",
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

    ### (Private) Pure Candidate Generator Feeds, mostly for testing and debugging ###
    "post-similarity": FeedConfig(
        display_name="Post Similarity",
        description="Development feed — post-similarity candidates only.",
        internal_rkey="gh-ps",
        internal_display_name="gh PS",
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


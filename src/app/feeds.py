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

from .models import CandidateGenerateRequest, FeedConfig, GeneratorSpec, RankPredictRequest

# NOTE: display_name is limited to 24 chars, including the prefix ("GreenEarth, GE Dev, or GE Stg")
FEEDS: dict[str, FeedConfig] = {
    "basic-similarity": FeedConfig(
        display_name="Similarity",
        description="Development feed — post-similarity candidates with popularity infill.",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="post_similarity", weight=0.5),
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
        description="Development feed — random posts.",
        diversify=False,
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="random_posts", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
    ),
    "ranked": FeedConfig(
        display_name="Ranked",
        description="Current-best ranked feed.",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[
                GeneratorSpec(name="post_similarity", weight=0.5),
                GeneratorSpec(name="followed_users", weight=0.5),
            ],
            infill="popularity",
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            model="two_tower",
        ),
    ),
    "best-of-friends": FeedConfig(
        display_name="Best of Friends",
        description="The best posts from people you follow, curated just for you.",
        gen_request_template=CandidateGenerateRequest.model_construct(
            generators=[GeneratorSpec(name="followed_users", weight=1.0)],
            infill=None,
            num_candidates=30,
            video_only=False,
            exclude_uris=[],
        ),
        rank_request_template=RankPredictRequest.model_construct(
            model="two_tower",
        ),
    ),
}


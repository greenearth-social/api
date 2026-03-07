"""Feed catalog — the canonical registry of all published feeds.

Each entry maps a short feed name (the AT Protocol rkey) to a ``FeedConfig``
that holds display metadata **and** the generator pipeline template.  The
template is a ``CandidateGenerateRequest`` with placeholder values for
session-specific fields (``user_did``, ``num_candidates``), which are filled
in at request time by the XRPC router.

This module is intentionally separate from the router so that other parts of
the codebase (e.g.  the ``publish_feed.py`` script) can import it without
pulling in FastAPI.
"""

from .models import CandidateGenerateRequest, FeedConfig, GeneratorSpec

# NOTE: display_name is limited to 24 chars, including the prefix ("GreenEarth, GE Dev, or GE Stg")
FEEDS: dict[str, FeedConfig] = {
    "basic-similarity": FeedConfig(
        display_name="Similarity",
        description="Development feed — post-similarity candidates with popularity infill.",
        gen_request_template=CandidateGenerateRequest(
            generators=[GeneratorSpec(name="post_similarity", weight=1.0)],
            infill="popularity",
            user_did="",
            num_candidates=30,
            video_only=False,
        ),
    ),
    "random": FeedConfig(
        display_name="Random",
        description="Development feed — random posts.",
        gen_request_template=CandidateGenerateRequest(
            generators=[GeneratorSpec(name="random_posts", weight=1.0)],
            infill=None,
            user_did="",
            num_candidates=30,
            video_only=False,
        ),
    ),
}
